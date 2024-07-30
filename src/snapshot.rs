//! Build, read and write snapshots of the filesystem to create our backups.
//!
//! Snapshot files represent a completed backup.
//! They contain magic bytes and a small CBOR record with:
//!
//! - The ID of the tree that was packed up
//!
//! - The absolute paths of the directories in the tree.
//!   (Backups compare their paths to those of previous snapshots.
//!   If they find a match, they use that snapshot as a "parent",
//!   saving time by only hashing modified files.)
//!
//! - Metadata like time, author, and tags.
//!
//! Like Git commits, this makes them very lightweight - this is so little data
//! we don't bother with compression.
//!
//! Unlike Git commits, they don't record their ancestor(s) - we don't especially care
//! about the order of the snapshots so long as all the blobs in their tree are reachable.

use std::collections::BTreeSet;
use std::fs;
use std::io::prelude::*;

use anyhow::{bail, ensure, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use chrono::prelude::*;
use lazy_static::lazy_static;
use rayon::prelude::*;
use regex::Regex;
use serde_derive::{Deserialize, Serialize};

use crate::{
    backend, counters,
    file_util::check_magic,
    hashing::{HashingReader, HashingWriter, ObjectId},
};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    /// The time (local + TZ offset) whe the snapshot was taken
    pub time: DateTime<FixedOffset>,
    /// Snapshot author, defaulting to the machine's hostname
    pub author: String,
    /// Arbitrary user tags
    pub tags: BTreeSet<String>,
    /// The _absolute_ paths the user backed up in this snapshot,
    /// each of which will be a child in the top-level tree
    pub paths: BTreeSet<Utf8PathBuf>,
    /// A tree where each path is a child node.
    pub tree: ObjectId,
}

const MAGIC_BYTES: &[u8] = b"MKBAKSNP1";

fn to_file(fh: &mut fs::File, snapshot: &Snapshot) -> Result<ObjectId> {
    fh.write_all(MAGIC_BYTES)?;

    let mut hasher = HashingWriter::new(fh);

    ciborium::into_writer(snapshot, &mut hasher)?;

    let (id, fh) = hasher.finalize();
    fh.sync_all()?;

    Ok(id)
}

/// Upload a snapshot, finishing a backup.
pub fn upload(snapshot: &Snapshot, backend: &backend::CachedBackend) -> Result<ObjectId> {
    let mut fh = tempfile::Builder::new()
        .prefix("temp-backpak-")
        .suffix(".snapshot")
        .tempfile_in(".") // TODO: Configurable?
        .context("Couldn't open temporary snapshot for writing")?;

    let id = to_file(fh.as_file_mut(), snapshot).context("Couldn't save snapshot")?;

    // Once the snapshot is done, let's persist it and upload it!
    let snapshot_name = format!("{}.snapshot", id);
    let persisted = fh
        .persist(&snapshot_name)
        .with_context(|| format!("Couldn't persist finished snapshot {}", snapshot_name))?;

    backend.write(&snapshot_name, persisted)?;
    Ok(id)
}

/// Loads the snapshot from the given reader,
/// also returning its calculated ID.
fn from_reader<R: Read>(r: &mut R) -> Result<(Snapshot, ObjectId)> {
    check_magic(r, MAGIC_BYTES).context("Wrong magic bytes for snapshot file")?;
    let mut hasher = HashingReader::new(r);
    let snapshot =
        ciborium::from_reader(&mut hasher).context("CBOR decoding of snapshot file failed")?;
    let (id, _) = hasher.finalize();
    Ok((snapshot, id))
}

pub fn find_and_load(
    id_prefix: &str,
    cached_backend: &backend::CachedBackend,
) -> Result<(Snapshot, ObjectId)> {
    let id = find(id_prefix, cached_backend)?;
    Ok((load(&id, cached_backend)?, id))
}

/// Loads the snapshot with the given ID from the backend,
/// verifying its contents match its ID.
pub fn load(id: &ObjectId, cached_backend: &backend::CachedBackend) -> Result<Snapshot> {
    let (snapshot, calculated_id) = from_reader(&mut cached_backend.read_snapshot(id)?)
        .with_context(|| format!("Couldn't load snapshot {}", id))?;
    ensure!(
        *id == calculated_id,
        "Snapshot {}'s contents changed! Now hashes to {}",
        id,
        calculated_id
    );
    counters::bump(counters::Op::SnapshotLoad);
    Ok(snapshot)
}

/// Load all snapshots from the given backend and sort them by date taken.
pub fn load_chronologically(
    cached_backend: &backend::CachedBackend,
) -> Result<Vec<(Snapshot, ObjectId)>> {
    let mut snapshots = cached_backend
        .list_snapshots()?
        .par_iter()
        .map(|file| {
            let snapshot_id = backend::id_from_path(file)?;
            let snap = load(&snapshot_id, cached_backend)?;
            Ok((snap, snapshot_id))
        })
        .collect::<Result<Vec<(Snapshot, ObjectId)>>>()?;
    snapshots.sort_by_key(|(snap, _)| snap.time);
    Ok(snapshots)
}

pub fn find(prefix: &str, cached_backend: &backend::CachedBackend) -> Result<ObjectId> {
    use std::str::FromStr;

    // See if we can skip all the below with an exact match.
    if let Ok(id) = ObjectId::from_str(prefix) {
        return Ok(id);
    }

    lazy_static! {
        // Git-like syntax:
        // Match LAST (or HEAD; git habits die hard), and either a single tilde
        // (meaning one before the last) or ~<num> (meaning <num> before last).
        static ref LAST_REGEX: Regex =
            Regex::new(r"^(?:LAST|HEAD)(?:(~)|(?:~([0-9]+)))?$").unwrap();
    }

    if let Some(cap) = LAST_REGEX.captures(prefix) {
        let groups = cap.iter().collect::<Vec<_>>();
        let index = match groups[..] {
            [_, None, None] => 0,
            [_, Some(_), None] => 1,
            [_, None, Some(n)] => n.as_str().parse().unwrap(),
            _ => unreachable!(),
        };
        match load_chronologically(cached_backend)?
            .iter()
            .rev()
            .nth(index)
        {
            Some((_snap, id)) => return Ok(*id),
            None => bail!("Don't have {} snapshots yet", index + 1),
        };
    }

    // Like Git, require at least a few digits of an ID.
    if prefix.len() < 4 {
        bail!("Provide a snapshot ID with at least 4 digits!");
    }

    let mut matches = cached_backend
        .list_snapshots()?
        .into_iter()
        .filter(|snap| Utf8Path::new(snap).file_stem().unwrap().starts_with(prefix))
        .collect::<Vec<_>>();

    match matches.len() {
        0 => bail!("No snapshots start with {}", prefix),
        1 => backend::id_from_path(matches.pop().unwrap()),
        multiple => bail!("{} different snapshots start with {}", multiple, prefix,),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use tempfile::tempfile;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    fn build_test_snapshot() -> Snapshot {
        Snapshot {
            time: DateTime::parse_from_rfc3339("1969-07-20T20:17:40Z").unwrap(),
            author: String::from("Neil"),
            tags: ["Apollo", "NASA"]
                .iter()
                .map(|s| String::from(*s))
                .collect::<BTreeSet<_>>(),
            paths: ["moon/orbit", "moon/tranquility-base"]
                .iter()
                .map(Utf8PathBuf::from)
                .collect::<BTreeSet<_>>(),
            tree: ObjectId::hash(b"One small step"),
        }
    }

    #[test]
    /// Pack manifest and ID remains stable from build to build.
    fn stability() -> Result<()> {
        init();

        let snapshot = build_test_snapshot();

        /*
        let fh = fs::File::create("tests/references/snapshot.stability")?;
        let mut hasher = HashingWriter::new(fh);
        ciborium::into_writer(&snapshot, &mut hasher)?;
        let (id, _fh) = hasher.finalize();
        */

        let mut snapshot_cbor = Vec::new();
        ciborium::into_writer(&snapshot, &mut snapshot_cbor)?;
        let id = ObjectId::hash(&snapshot_cbor);

        // ID remains stable
        assert_eq!(
            format!("{}", id),
            "pkfspsfbk6f085uj3t7m6sghec5oa7l3pelt6timapj6e"
        );
        // Contents remain stable
        // (We could just use the ID and length,
        // but having some example CBOR in the repo seems helpful.)
        let from_example = fs::read("tests/references/snapshot.stability")?;
        assert_eq!(snapshot_cbor, from_example);
        Ok(())
    }

    #[test]
    fn round_trip() -> Result<()> {
        init();

        let snapshot = build_test_snapshot();
        let mut fh = tempfile()?;
        let written_id = to_file(&mut fh, &snapshot)?;

        fh.seek(std::io::SeekFrom::Start(0))?;
        let (read_snapshot, read_id) = from_reader(&mut fh)?;

        assert_eq!(snapshot, read_snapshot);
        assert_eq!(written_id, read_id);
        Ok(())
    }
}

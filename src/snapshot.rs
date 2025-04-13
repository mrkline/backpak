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
use std::sync::{
    LazyLock,
    atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, bail, ensure};
use camino::Utf8PathBuf;
use jiff::Zoned;
use rayon::prelude::*;
use regex::Regex;
use serde_derive::{Deserialize, Serialize};

use crate::{
    backend, counters,
    file_util::check_magic,
    hashing::{HashingReader, HashingWriter, ObjectId},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    /// The time (instant + local time + TZ) when the snapshot was taken
    #[serde(deserialize_with = "deserialize_zoned")]
    pub time: Zoned,
    /// Snapshot author, defaulting to the machine's hostname
    pub author: String,
    /// Arbitrary user tags
    pub tags: BTreeSet<String>,
    /// The _absolute_ paths backed up in this snapshot,
    /// each of which will be a child in the top-level tree.
    /// We store them here because the top-level tree does not.
    /// (If I back up `/home/me` and `/etc/`, those paths go here,
    /// and my top-level tree is `{ "me" -> subtree, "etc" -> subtree }`.)
    ///
    /// Alternatively, we could write a bunch of special-case code into our filesystem walks
    /// so that the top-level tree uses absolute paths, but subtrees use relative paths.
    /// (It would be insane to store absolute paths at every level!)
    /// And we'd have to plumb that to the [`walk_fs()`](crate::fs_tree::walk_fs) visitors.
    /// And if we stored the absolute paths in the top-level tree instead of here,
    /// we'd have to actually load trees to find a parent snapshot for a backup.
    ///
    /// That sounds like a bad trade. I hope I'm right and don't regret this choice.
    pub paths: BTreeSet<Utf8PathBuf>,
    /// A tree where each path is a child node.
    pub tree: ObjectId,
}

// Older snapshots saved with chrono will be yyyy-mm-ddTH:M:S.f:z
pub fn deserialize_zoned<'de, D>(d: D) -> Result<Zoned, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;

    let as_str = String::deserialize(d)?;
    as_str
        .parse()
        .or_else(|_e| Zoned::strptime("%FT%H:%M:%S%.f%:Q", &as_str))
        .map_err(serde::de::Error::custom)
}

pub fn strftime(z: &Zoned) -> impl std::fmt::Display {
    z.strftime("%a %b %-e %-Y %H:%M:%S %:Q")
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

    // Snapshots are very small compared to packs/indexes;
    // don't bother including them in the "total bytes uploaded" accounting.
    // (Plus, the progress we show with the atomic are done by the time we upload the snapshot(s)).
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
    load_chronologically_with_total_size(cached_backend).map(|(v, _ts)| v)
}

/// [`load_chronologically`] plus the total snapshot size.
///
/// Nice for usage reporting, since it saves us another backend query.
pub fn load_chronologically_with_total_size(
    cached_backend: &backend::CachedBackend,
) -> Result<(Vec<(Snapshot, ObjectId)>, u64)> {
    let total = AtomicU64::new(0);

    let mut snapshots = cached_backend
        .list_snapshots()?
        .par_iter()
        .map_with(&total, |tot, (file, len)| {
            let snapshot_id = backend::id_from_path(file)?;
            let snap = load(&snapshot_id, cached_backend)?;
            tot.fetch_add(*len, Ordering::Relaxed);
            Ok((snap, snapshot_id))
        })
        .collect::<Result<Vec<_>>>()?;
    snapshots.sort_by_key(|(snap, _)| snap.time.timestamp());
    Ok((snapshots, total.load(Ordering::SeqCst)))
}

/// Find a given snapshot and its ID from the loaded chronological list
pub fn find<'a>(
    chronological_snapshots: &'a [(Snapshot, ObjectId)],
    prefix: &str,
) -> Result<&'a (Snapshot, ObjectId)> {
    use std::str::FromStr;

    // See if we can skip all the below with an exact match.
    if let Ok(id) = ObjectId::from_str(prefix) {
        match chronological_snapshots.iter().find(|(_s, i)| *i == id) {
            Some(found) => return Ok(found),
            None => bail!("No snapshot {id}"),
        }
    }

    // Git-like syntax:
    // Match LAST (or HEAD; git habits die hard), and either a single tilde
    // (meaning one before the last) or ~<num> (meaning <num> before last).
    static LAST_REGEX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^(?:LAST|HEAD)(?:(~)|(?:~([0-9]+)))?$").unwrap());

    if let Some(cap) = LAST_REGEX.captures(prefix) {
        let groups = cap.iter().collect::<Vec<_>>();
        let index = match groups[..] {
            [_, None, None] => 0,
            [_, Some(_), None] => 1,
            [_, None, Some(n)] => n.as_str().parse().unwrap(),
            _ => unreachable!(),
        };
        match chronological_snapshots.iter().rev().nth(index) {
            Some(found) => return Ok(found),
            None => bail!("Don't have {} snapshots yet", index + 1),
        };
    }

    // Like Git, require at least a few digits of an ID.
    if prefix.len() < 4 {
        bail!("Provide a snapshot ID with at least 4 digits!");
    }

    let matches = chronological_snapshots
        .iter()
        .filter(|(_s, id)| id.to_string().starts_with(prefix))
        .collect::<Vec<_>>();

    match matches.len() {
        0 => bail!("No snapshots start with {}", prefix),
        1 => Ok(matches[0]),
        multiple => bail!("{} different snapshots start with {}", multiple, prefix,),
    }
}

/// Find the listed snapshots and their IDs, and return them chronologically and deduplicated.
pub fn from_args_list(
    chrono_snapshots: &[(Snapshot, ObjectId)],
    args: &[String],
) -> Result<Vec<(Snapshot, ObjectId)>> {
    let mut desired_snaps = Vec::with_capacity(args.len());
    for desired_snap in args {
        let (s, i) = find(chrono_snapshots, desired_snap)?;
        desired_snaps.push((s.clone(), *i));
    }
    // Take whatever the user asked for and make it chronological with no duplicates.
    desired_snaps.sort_by_key(|(snap, _)| snap.time.timestamp());
    desired_snaps.dedup_by(|(_, id1), (_, id2)| id1 == id2);
    Ok(desired_snaps)
}

#[cfg(test)]
mod test {
    use super::*;

    use tempfile::tempfile;

    fn build_test_snapshot() -> Snapshot {
        Snapshot {
            time: "1969-07-20T20:17:40Z[UTC]".parse().unwrap(),
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
            id.to_string(),
            "4t84ab7sgsjjss803e30mdrokbnibg7ubpb4leds2e91g"
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

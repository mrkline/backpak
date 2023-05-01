//! Build, read and write snapshots of the filesystem to create our backups
use std::collections::BTreeSet;
use std::fs;
use std::io::prelude::*;

use anyhow::{bail, ensure, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use chrono::prelude::*;
use log::*;
use rayon::prelude::*;
use serde_derive::{Deserialize, Serialize};

use crate::backend;
use crate::file_util::check_magic;
use crate::hashing::{HashingReader, HashingWriter, ObjectId};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub time: DateTime<FixedOffset>,
    pub author: String,
    pub tags: BTreeSet<String>,
    pub paths: BTreeSet<Utf8PathBuf>,
    pub tree: ObjectId,
}

const MAGIC_BYTES: &[u8] = b"MKBAKSNP";

fn to_file(fh: &mut fs::File, snapshot: &Snapshot) -> Result<ObjectId> {
    fh.write_all(MAGIC_BYTES)?;

    let mut hasher = HashingWriter::new(fh);

    serde_cbor::to_writer(&mut hasher, snapshot)?;

    let (id, fh) = hasher.finalize();
    fh.sync_all()?;

    Ok(id)
}

/// Upload a snapshot, finishing a backup.
pub fn upload(snapshot: &Snapshot, backend: &backend::CachedBackend) -> Result<()> {
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

    backend.write(&snapshot_name, persisted)
}

/// Loads the snapshot from the given reader,
/// also returning its calculated ID.
fn from_reader<R: Read>(r: &mut R) -> Result<(Snapshot, ObjectId)> {
    check_magic(r, MAGIC_BYTES).context("Wrong magic bytes for snapshot file")?;
    let mut hasher = HashingReader::new(r);
    let snapshot =
        serde_cbor::from_reader(&mut hasher).context("CBOR decoding of snapshot file failed")?;
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
    debug!("Loading snapshot {}", id);
    let (snapshot, calculated_id) = from_reader(&mut cached_backend.read_snapshot(id)?)
        .with_context(|| format!("Couldn't load snapshot {}", id))?;
    ensure!(
        *id == calculated_id,
        "Snapshot {}'s contents changed! Now hashes to {}",
        id,
        calculated_id
    );
    Ok(snapshot)
}

/// Load all snapshots from the given backend and sort them by date taken.
pub fn load_chronologically(
    cached_backend: &crate::backend::CachedBackend,
) -> Result<Vec<(Snapshot, ObjectId)>> {
    debug!("Reading snapshots");
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

pub fn find(prefix: &str, cached_backend: &crate::backend::CachedBackend) -> Result<ObjectId> {
    if prefix == "last" {
        match load_chronologically(cached_backend)?.iter().rev().next() {
            None => bail!("No snapshots taken yet"),
            Some((_snap, id)) => return Ok(*id),
        }
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
        serde_cbor::to_writer(&mut hasher, &snapshot)?;
        let (id, _fh) = hasher.finalize();
        */

        let snapshot = serde_cbor::to_vec(&snapshot)?;
        let id = ObjectId::hash(&snapshot);

        // ID remains stable
        assert_eq!(
            format!("{}", id),
            "306er7979e25dcjnvdqmkcmtr2qo71lth056qr2h5be0i"
        );
        // Contents remain stable
        // (We could just use the ID and length,
        // but having some example CBOR in the repo seems helpful.)
        let from_example = fs::read("tests/references/snapshot.stability")?;
        assert_eq!(snapshot, from_example);
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

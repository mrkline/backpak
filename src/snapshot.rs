use std::collections::BTreeSet;
use std::fs;
use std::io::prelude::*;
use std::path::PathBuf;
use std::sync::mpsc::SyncSender;

use anyhow::*;
use chrono::prelude::*;
use serde_derive::*;

use crate::backend;
use crate::file_util::check_magic;
use crate::hashing::{HashingWriter, ObjectId};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    pub time: DateTime<FixedOffset>,
    pub author: String,
    pub tags: BTreeSet<String>,
    pub paths: BTreeSet<PathBuf>,
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
pub fn upload(snapshot: &Snapshot, to_upload: SyncSender<(String, fs::File)>) -> Result<()> {
    let mut fh = tempfile::Builder::new()
        .prefix("temp-backpak-")
        .suffix(".snapshot")
        .tempfile_in(&std::env::current_dir()?) // TODO: Configurable?
        .context("Couldn't open temporary snapshot for writing")?;

    let id = to_file(fh.as_file_mut(), snapshot).context("Couldn't save snapshot")?;

    // Once the snapshot is done, let's persist it and upload it!
    let snapshot_name = format!("{}.snapshot", id);
    let persisted = fh
        .persist(&snapshot_name)
        .with_context(|| format!("Couldn't persist finished snapshot {}", snapshot_name))?;

    to_upload
        .send((snapshot_name, persisted))
        .context("backup -> uploader channel exited early")?;
    Ok(())
}

pub fn from_reader<R: Read>(r: &mut R) -> Result<Snapshot> {
    check_magic(r, MAGIC_BYTES).context("Wrong magic bytes for snapshot file")?;
    let snapshot = serde_cbor::from_reader(r).context("CBOR decoding of snapshot file failed")?;
    Ok(snapshot)
}

/// Load all snapshots from the given backend and sort them by date taken.
pub fn load_chronologically(
    cached_backend: &crate::backend::CachedBackend,
) -> Result<Vec<(Snapshot, ObjectId)>> {
    let mut snapshots = cached_backend
        .backend
        .list_snapshots()?
        .iter()
        .map(|file| {
            let mut fh = cached_backend.read(file)?;
            let snap = from_reader(&mut fh)?;
            let id = backend::id_from_path(file).unwrap();
            Ok((snap, id))
        })
        .collect::<Result<Vec<(Snapshot, ObjectId)>>>()?;
    snapshots.sort_by_key(|(snap, _)| snap.time);
    Ok(snapshots)
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
                .map(PathBuf::from)
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
            "180ced9d274b8456b277fb756a32ddd8b58386bd880a6d6c512adc09"
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
        to_file(&mut fh, &snapshot)?;

        fh.seek(std::io::SeekFrom::Start(0))?;
        let read_snapshot = from_reader(&mut fh)?;

        assert_eq!(snapshot, read_snapshot);
        Ok(())
    }
}

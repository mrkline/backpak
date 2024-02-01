//! Build, read, and write [indexes](Index) of packs' contents.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::prelude::*;
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::Mutex;

use anyhow::{anyhow, bail, ensure, Context, Result};
use log::*;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use serde_derive::{Deserialize, Serialize};

use crate::backend;
use crate::counters;
use crate::file_util::{check_magic, nice_size};
use crate::hashing::{HashingReader, HashingWriter, ObjectId};
use crate::pack::{PackManifest, PackMetadata};

const MAGIC_BYTES: &[u8] = b"MKBAKIDX";

// Persist WIP (but valid) indexes to a known name so that an interrupted
// backup can read it in and know what we've already backed up.
pub const WIP_NAME: &str = "backpak-wip.index";

/// Maps a pack's ID to the manifest of blobs it holds.
pub type PackMap = BTreeMap<ObjectId, PackManifest>;

/// Maps packs to the blobs they contain,
/// and lists any previous indexes they supersede.
#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Index {
    pub supersedes: BTreeSet<ObjectId>,
    pub packs: PackMap,
}

impl Index {
    #[inline]
    fn is_empty(&self) -> bool {
        self.supersedes.is_empty() && self.packs.is_empty()
    }
}

/// Gather metadata for completed packs from `rx` into an index file,
/// and upload the index files when they reach a sufficient size.
pub fn index(
    starting_index: Index,
    rx: Receiver<PackMetadata>,
    to_upload: SyncSender<(String, File)>,
) -> Result<bool> {
    let mut index = starting_index;
    let mut persisted = None;

    // If we're given a non-empty index, write that out to start with.
    // (For example, it could be an index from `prune` that omits packs
    // we no longer need. If we don't write it but delete those packs anyways...)
    if !index.is_empty() {
        persisted = Some(to_temp_file(&index)?);
    }

    // For each pack...
    while let Ok(PackMetadata { id, manifest }) = rx.recv() {
        ensure!(
            index.packs.insert(id, manifest).is_none(),
            "Duplicate pack received: {}",
            id
        );

        trace!("Wrote {} packs into index", index.packs.len());

        // Rewrite the index every time we get a pack.
        // That way the temp index should always contain a complete list of packs,
        // allowing us to resume a backup from the last finished pack.
        persisted = Some(to_temp_file(&index)?);
    }

    if let Some((index_id, mut persisted)) = persisted {
        let index_name = format!("{}.index", index_id);

        // On Windows, we can't move an open file. Boo, Windows.
        if cfg!(windows) {
            persisted
                .sync_all()
                .with_context(|| format!("Couldn't close {} to rename it", WIP_NAME))?;
            drop(persisted);
            fs::rename(WIP_NAME, &index_name)
                .with_context(|| format!("Couldn't rename {} to {}", WIP_NAME, index_name))?;
            persisted =
                File::open(&index_name).with_context(|| "Couldn't reopen {} after renaming it.")?;
        } else {
            fs::rename(WIP_NAME, &index_name)
                .with_context(|| format!("Couldn't rename {} to {}", WIP_NAME, index_name))?;
        }
        debug!(
            "Index {} finished ({})",
            index_id,
            nice_size(persisted.metadata()?.len())
        );

        to_upload
            .send((index_name, persisted))
            .context("indexer -> uploader channel exited early")?;
        Ok(true)
    } else {
        debug!("No new indexes created - nothing changed");
        Ok(false)
    }
}

fn to_temp_file(index: &Index) -> Result<(ObjectId, File)> {
    // Could we speed things up by reusing the same file handle instead of
    // opening, writing, and closing each time we update the WIP index file?
    // Probably, but we'd have to seek back to the beginning each time,
    // _and_ we'd be assuming that the file grows larger each time.
    // (This _might_ not be true since its contents are compressed...)
    let mut tf = tempfile::Builder::new()
        .prefix("temp-backpak-")
        .suffix(".index")
        .tempfile_in(".")
        .context("Couldn't open temporary index for writing")?;

    let id = to_file(tf.as_file_mut(), index)?;
    let f = tf
        .persist(WIP_NAME)
        .with_context(|| format!("Couldn't persist WIP index to {}", WIP_NAME))?;
    Ok((id, f))
}

fn to_file(fh: &mut fs::File, index: &Index) -> Result<ObjectId> {
    fh.write_all(MAGIC_BYTES)?;

    let mut zstd = zstd::stream::write::Encoder::new(fh, 0)?;
    zstd.multithread(num_cpus::get_physical() as u32)?;

    let mut hasher = HashingWriter::new(zstd);

    ciborium::into_writer(index, &mut hasher)?;

    let (id, zstd) = hasher.finalize();
    let fh = zstd.finish()?;
    fh.sync_all()?;

    Ok(id)
}

/// Load all indexes from the provided backend and combines them into a master
/// index, removing any superseded ones.
pub fn build_master_index(cached_backend: &backend::CachedBackend) -> Result<Index> {
    info!("Building a master index");

    #[derive(Debug, Default)]
    struct Results {
        bad_indexes: BTreeSet<ObjectId>,
        superseded_indexes: BTreeSet<ObjectId>,
        loaded_indexes: BTreeMap<ObjectId, PackMap>,
    }

    let shared = Mutex::new(Results::default());

    cached_backend
        .list_indexes()?
        .par_iter()
        .try_for_each_with(&shared, |shared, index_file| {
            let index_id = backend::id_from_path(index_file)?;
            let mut loaded_index = match load(&index_id, cached_backend) {
                Ok(l) => l,
                Err(e) => {
                    error!("{:?}", e);
                    shared.lock().unwrap().bad_indexes.insert(index_id);
                    return Ok(());
                }
            };
            let mut guard = shared.lock().unwrap();
            guard
                .superseded_indexes
                .append(&mut loaded_index.supersedes);
            ensure!(
                guard
                    .loaded_indexes
                    .insert(index_id, loaded_index.packs)
                    .is_none(),
                "Duplicate index {} read from backend!",
                index_file
            );
            Ok(())
        })?;

    let mut shared = shared.into_inner().unwrap();

    if !shared.bad_indexes.is_empty() {
        bail!(
            "Errors loading indexes {:?}. Consider running backpak rebuild-index.",
            shared.bad_indexes
        );
    }

    // Strip out superseded indexes.
    for superseded in &shared.superseded_indexes {
        if shared.loaded_indexes.remove(superseded).is_some() {
            debug!("Index {} is superseded and can be deleted.", superseded);
        }
    }

    let mut master_pack_map = BTreeMap::new();
    for index in shared.loaded_indexes.values_mut() {
        master_pack_map.append(index);
    }

    Ok(Index {
        supersedes: shared.superseded_indexes,
        packs: master_pack_map,
    })
}

/// A result of [`blob_to_pack_map()`],
/// mapping [`Blob`](crate::blob::Blob) IDs to the the pack where each is stored
pub type BlobMap = FxHashMap<ObjectId, ObjectId>;

/// Given an index, produce a mapping that traces [`Blob`](crate::blob::Blob)s
/// to the packs where they're stored
pub fn blob_to_pack_map(index: &Index) -> Result<BlobMap> {
    debug!("Building a blob -> pack map");
    let mut mapping = FxHashMap::default();

    for (pack_id, manifest) in &index.packs {
        for blob in manifest {
            if let Some(other_pack) = mapping.insert(blob.id, *pack_id) {
                // TODO: Should this just be a warning?
                // This might happen in weird cases like concurrent backups
                // but isn't a huge issue so long as the blobs are valid...
                bail!(
                    "Duplicate blob {} in pack {}, previously seen in pack {}",
                    blob.id,
                    pack_id,
                    other_pack
                );
            }
        }
    }

    Ok(mapping)
}

/// Gather the set of all blobs in a given index.
pub fn blob_set(index: &Index) -> Result<FxHashSet<ObjectId>> {
    debug!("Building a set of all blobs");
    let mut blobs = FxHashSet::default();

    for (pack_id, manifest) in &index.packs {
        for blob in manifest {
            if !blobs.insert(blob.id) {
                // TODO: Ditto - just warn?
                bail!("Duplicate blob {} in pack {}", blob.id, pack_id);
            }
        }
    }

    Ok(blobs)
}

/// Load the index from the given reader,
/// also returning its calculated ID.
fn from_reader<R: Read>(r: &mut R) -> Result<(Index, ObjectId)> {
    check_magic(r, MAGIC_BYTES).context("Wrong magic bytes for index file")?;

    let decoder =
        zstd::stream::read::Decoder::new(r).context("Decompression of index file failed")?;
    let mut hasher = HashingReader::new(decoder);
    let index = ciborium::from_reader(&mut hasher).context("CBOR decoding of index file failed")?;
    let (id, _) = hasher.finalize();
    Ok((index, id))
}

pub fn read_wip() -> Result<Option<Index>> {
    let mut fd = match File::open(WIP_NAME) {
        Ok(w) => w,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(None);
            }
            let e = anyhow!(e).context(format!("Couldn't open {WIP_NAME}"));
            return Err(e);
        }
    };
    let (index, _) = from_reader(&mut fd)?;
    Ok(Some(index))
}

/// Load the index with the given ID from the backend,
/// verifying its contents match its ID.
pub fn load(id: &ObjectId, cached_backend: &backend::CachedBackend) -> Result<Index> {
    let (index, calculated_id) = from_reader(&mut cached_backend.read_index(id)?)
        .with_context(|| format!("Couldn't load index {}", id))?;
    ensure!(
        *id == calculated_id,
        "Index {}'s contents changed! Now hashes to {}",
        id,
        calculated_id
    );
    counters::bump(counters::Op::IndexLoad);
    Ok(index)
}

#[cfg(test)]
mod test {
    use super::*;

    use tempfile::tempfile;

    use crate::blob;
    use crate::pack::PackManifestEntry;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    fn build_test_index() -> Index {
        let mut supersedes = BTreeSet::new();
        supersedes.insert(ObjectId::hash(b"Some previous index"));
        supersedes.insert(ObjectId::hash(b"Another previous index"));

        let mut packs = BTreeMap::new();
        packs.insert(
            ObjectId::hash(b"pack o' chunks"),
            vec![
                PackManifestEntry {
                    blob_type: blob::Type::Chunk,
                    length: 42,
                    id: ObjectId::hash(b"a chunk"),
                },
                PackManifestEntry {
                    blob_type: blob::Type::Chunk,
                    length: 9001,
                    id: ObjectId::hash(b"another chunk"),
                },
            ],
        );
        packs.insert(
            ObjectId::hash(b"pack o'trees"),
            vec![
                PackManifestEntry {
                    blob_type: blob::Type::Tree,
                    length: 182,
                    id: ObjectId::hash(b"first tree"),
                },
                PackManifestEntry {
                    blob_type: blob::Type::Tree,
                    length: 22,
                    id: ObjectId::hash(b"second tree"),
                },
                PackManifestEntry {
                    blob_type: blob::Type::Tree,
                    length: 11,
                    id: ObjectId::hash(b"third tree"),
                },
            ],
        );
        Index { supersedes, packs }
    }

    #[test]
    /// Pack manifest and ID remains stable from build to build.
    fn stability() -> Result<()> {
        init();

        let index = build_test_index();

        /*
        let mut fh = File::create("tests/references/index.stability")?;
        let mut hasher = HashingWriter::new(fh);
        ciborium::into_writer(&index, &mut hasher)?;
        let (id, _fh) = hasher.finalize();
        */

        let mut index_cbor = Vec::new();
        ciborium::into_writer(&index, &mut index_cbor)?;
        let id = ObjectId::hash(&index_cbor);

        // ID remains stable
        assert_eq!(
            format!("{}", id),
            "e3vr9p4gmumq8i1dafgum50iirupu6ahk9fn6c781b09a"
        );
        // Contents remain stable
        // (We could just use the ID and length,
        // but having some example CBOR in the repo seems helpful.)
        let from_example = fs::read("tests/references/index.stability")?;
        assert_eq!(index_cbor, from_example);
        Ok(())
    }

    #[test]
    fn round_trip() -> Result<()> {
        init();

        let index = build_test_index();
        let mut fh = tempfile()?;
        let written_id = to_file(&mut fh, &index)?;

        fh.seek(std::io::SeekFrom::Start(0))?;
        let (read_index, read_id) = from_reader(&mut fh)?;

        assert_eq!(index, read_index);
        assert_eq!(written_id, read_id);
        Ok(())
    }
}

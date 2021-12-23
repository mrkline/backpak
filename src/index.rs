//! Build, read, and write [indexes](Index) of packs' contents.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::prelude::*;
use std::sync::Mutex;

use anyhow::{bail, Context, ensure, Result};
use log::*;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use serde_derive::*;
use tempfile::NamedTempFile;
use tokio::sync::mpsc::{Sender, UnboundedReceiver};

use crate::backend;
use crate::counters;
use crate::file_util::check_magic;
use crate::hashing::{HashingReader, HashingWriter, ObjectId};
use crate::pack::{PackManifest, PackMetadata};

const MAGIC_BYTES: &[u8] = b"MKBAKIDX";

// Persist WIP (but valid) indexes to a known name so that an interrupted
// backup can read it in and know what we've already backed up.
const WIP_NAME: &str = "backpak-wip.index";

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
pub async fn index(
    starting_index: Index,
    mut rx: UnboundedReceiver<PackMetadata>,
    to_upload: Sender<(String, File)>,
) -> Result<bool> {
    let mut index = starting_index;
    let mut index_id = None;
    let mut persisted = None;

    // If we're given a non-empty index, write that out to start with.
    // (For example, it could be an index from `prune` that omits packs
    // we no longer need. If we don't write it but delete those packs anyways...)
    if !index.is_empty() {
        let (id, temp_file) = to_temp_file(&index)?;
        index_id = Some(id);

        persisted = Some(
            temp_file
                .persist(WIP_NAME)
                .with_context(|| format!("Couldn't persist WIP index to {}", WIP_NAME))?,
        );
    }

    // For each pack...
    while let Some(PackMetadata { id, manifest }) = rx.recv().await {
        ensure!(
            index.packs.insert(id, manifest).is_none(),
            "Duplicate pack received: {}",
            id
        );

        trace!("Wrote {} packs into index", index.packs.len());

        // Rewrite the index every time we get a pack.
        // That way the temp index should always contain a complete list of packs,
        // allowing us to resume a backup from the last finished pack.

        let (id, temp_file) = to_temp_file(&index)?;
        index_id = Some(id);

        persisted = Some(
            temp_file
                .persist(WIP_NAME)
                .with_context(|| format!("Couldn't persist WIP index to {}", WIP_NAME))?,
        );
    }

    if let Some(mut persisted) = persisted {
        let index_id = index_id.unwrap();
        let index_name = format!("{}.index", index_id);

        // On Windows, we can't move an open file. Boo, Windows.
        if cfg!(target_family = "windows") {
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
            "Index {} finished ({} bytes)",
            index_id,
            persisted.metadata()?.len()
        );

        to_upload
            .send((index_name, persisted))
            .await
            .context("indexer -> uploader channel exited early")?;
        Ok(true)
    } else {
        debug!("No new indexes created - nothing changed");
        Ok(false)
    }
}

fn to_temp_file(index: &Index) -> Result<(ObjectId, NamedTempFile)> {
    // Could we speed things up by reusing the same file handle instead of
    // opening, writing, and closing each time we update the WIP index file?
    // Probably, but we'd have to seek back to the beginning each time,
    // _and_ we'd be assuming that the file grows larger each time.
    // (This _might_ not be true since its contents are compressed...)
    let mut fh = tempfile::Builder::new()
        .prefix("temp-backpak-")
        .suffix(".index")
        .tempfile_in(&std::env::current_dir()?)
        .context("Couldn't open temporary index for writing")?;

    Ok((to_file(fh.as_file_mut(), index)?, fh))
}

fn to_file(fh: &mut fs::File, index: &Index) -> Result<ObjectId> {
    fh.write_all(MAGIC_BYTES)?;

    let mut zstd = zstd::stream::write::Encoder::new(fh, 0)?;
    zstd.multithread(num_cpus::get_physical() as u32)?;

    let mut hasher = HashingWriter::new(zstd);

    serde_cbor::to_writer(&mut hasher, index)?;

    let (id, zstd) = hasher.finalize();
    let fh = zstd.finish()?;
    fh.sync_all()?;

    Ok(id)
}

/// Load all indexes from the provided backend and combines them into a master
/// index, removing any superseded ones.
pub async fn build_master_index(cached_backend: &backend::CachedBackend) -> Result<Index> {
    info!("Building a master index");

    #[derive(Debug, Default)]
    struct Results {
        bad_indexes: BTreeSet<ObjectId>,
        superseded_indexes: BTreeSet<ObjectId>,
        loaded_indexes: BTreeMap<ObjectId, PackMap>,
    }

    let shared = Mutex::new(Results::default());

    cached_backend
        .list_indexes()
        .await?
        .par_iter()
        .try_for_each_with(&shared, |shared, index_file| {
            let index_id = backend::id_from_path(&index_file)?;
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
            "Errors loading indexes {:?}. Consider running backpack rebuild-index.",
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

pub type BlobMap = FxHashMap<ObjectId, ObjectId>;

/// Given an index, produce a mapping that relates blobs -> their packs
pub fn blob_to_pack_map(index: &Index) -> Result<BlobMap> {
    debug!("Building a blob -> pack map");
    let mut mapping = FxHashMap::default();

    for (pack_id, manifest) in &index.packs {
        for blob in manifest {
            if let Some(other_pack) = mapping.insert(blob.id, *pack_id) {
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
    let index =
        serde_cbor::from_reader(&mut hasher).context("CBOR decoding of index file failed")?;
    let (id, _) = hasher.finalize();
    Ok((index, id))
}

/// Load the index with the given ID from the backend,
/// verifying its contents match its ID.
pub fn load(id: &ObjectId, cached_backend: &backend::CachedBackend) -> Result<Index> {
    debug!("Loading index {}", id);
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
    use crate::pack::*;

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
        serde_cbor::to_writer(&mut hasher, &index)?;
        let (id, _fh) = hasher.finalize();
        */

        let index = serde_cbor::to_vec(&index)?;
        let id = ObjectId::hash(&index);

        // ID remains stable
        assert_eq!(
            format!("{}", id),
            "70ffb4e490b7ada4482d53e1eb141296fd9f1951a25f7330e80ac095"
        );
        // Contents remain stable
        // (We could just use the ID and length,
        // but having some example CBOR in the repo seems helpful.)
        let from_example = fs::read("tests/references/index.stability")?;
        assert_eq!(index, from_example);
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

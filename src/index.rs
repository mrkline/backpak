use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
use std::io::prelude::*;
use std::sync::mpsc::{Receiver, SyncSender};

use anyhow::*;
use log::*;
use serde_derive::*;
use tempfile::NamedTempFile;

use crate::backend;
use crate::file_util::check_magic;
use crate::hashing::{HashingReader, HashingWriter, ObjectId};
use crate::pack::{PackManifest, PackMetadata};
use crate::DEFAULT_TARGET_SIZE;

const MAGIC_BYTES: &[u8] = b"MKBAKIDX";

/// An index maps packs to the blobs they contain,
/// and lists any previous indexes they supersede.
#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Index {
    pub supersedes: BTreeSet<ObjectId>,
    pub packs: BTreeMap<ObjectId, PackManifest>,
}

/// Gathers metadata for completed packs from `rx` into an index file,
/// and uploads the index files when they reach a sufficient size.
pub fn index(rx: Receiver<PackMetadata>, to_upload: SyncSender<(String, File)>) -> Result<()> {
    let mut index = Index::default();

    // For each pack...
    while let Ok(PackMetadata { id, manifest }) = rx.recv() {
        ensure!(
            index.packs.insert(id, manifest).is_none(),
            "Duplicate pack received: {}",
            id
        );

        trace!(
            "Wrote {} packs into index, checking compressed size...",
            index.packs.len()
        );

        // Rewrite the index every time we get a pack.
        // That way the temp index should always contain a complete list of packs,
        // allowing us to resume a backup from the last finished pack.

        let (index_id, temp_file) = to_temp_file(&index)?;
        let compressed_size = temp_file.as_file().metadata()?.len();

        // If we're close enough to our target size, stop
        if compressed_size >= DEFAULT_TARGET_SIZE {
            info!(
                "Index {} finished ({} bytes). Starting another...",
                index_id, compressed_size
            );
            let index_name = format!("{}.index", index_id);
            let persisted = temp_file
                .persist(&index_name)
                .with_context(|| format!("Couldn't persist finished index to {}", index_name))?;

            // Let's axe any temp copies we had.
            // If one doesn't exist or something, that's cool too.
            let _ = fs::remove_file("backpak-wip.index");

            to_upload
                .send((index_name, persisted))
                .context("indexer -> uploader channel exited early")?;

            index = Index::default();
        } else {
            // Persist WIP (but valid) indexes to disk so that an interrupted
            // backup can read it in and know what we've already backed up.
            temp_file
                .persist("backpak-wip.index")
                .context("Couldn't persist WIP index to backpak-wip.index")?;
        }
    }
    if !index.packs.is_empty() {
        let (index_id, temp_file) = to_temp_file(&index)?;
        let compressed_size = temp_file.as_file().metadata()?.len();
        info!("Index {} finished ({} bytes)", index_id, compressed_size);

        let index_name = format!("{}.index", index_id);
        let persisted = temp_file
            .persist(&index_name)
            .with_context(|| format!("Couldn't persist finished index to {}", index_name))?;

        // Let's axe any temp copies we had.
        // If one doesn't exist or something, that's cool too.
        let _ = fs::remove_file("backpak-wip.index");

        to_upload
            .send((index_name, persisted))
            .context("indexer -> uploader channel exited early")?;
    }
    Ok(())
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
        .tempfile_in(&std::env::current_dir()?) // TODO: Configurable?
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

/// Loads all indexes from the provided backend and combines them into a master
/// index, removing any superseded ones.
pub fn build_master_index(cached_backend: &backend::CachedBackend) -> Result<Index> {
    info!("Building a master index of backed-up blobs");

    let mut bad_indexes = BTreeSet::new();

    let mut superseded_indexes = BTreeSet::new();

    // Don't combine the indexes until we know which ones to exclude.
    let mut loaded_indexes: BTreeMap<ObjectId, BTreeMap<ObjectId, PackManifest>> = BTreeMap::new();

    for index_file in cached_backend.backend.list_indexes()? {
        let index = backend::id_from_path(&index_file)?;
        let mut loaded_index = match load(&index, cached_backend) {
            Ok(l) => l,
            Err(e) => {
                error!("{:?}", e);
                bad_indexes.insert(index);
                continue;
            }
        };
        superseded_indexes.append(&mut loaded_index.supersedes);
        ensure!(
            loaded_indexes.insert(index, loaded_index.packs).is_none(),
            "Duplicate index file {} read from backend!",
            index_file
        );
    }

    if !bad_indexes.is_empty() {
        bail!(
            "Errors loading indexes {:?}. Consider running backpack rebuild-index.",
            bad_indexes
        );
    }

    // Strip out superseded indexes.
    for superseded in &superseded_indexes {
        if loaded_indexes.remove(&superseded).is_some() {
            info!("Index {} is superseded and can be deleted.", superseded);
        }
    }

    let mut master_pack_map = BTreeMap::new();
    for index in loaded_indexes.values_mut() {
        master_pack_map.append(index);
    }

    Ok(Index {
        supersedes: superseded_indexes,
        packs: master_pack_map,
    })
}

/// Given an index, produce a mapping that relates blobs -> their packs
pub fn blob_to_pack_map(index: &Index) -> Result<HashMap<ObjectId, ObjectId>> {
    debug!("Building a blob -> pack map");
    let mut mapping = HashMap::new();

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

/// Given an index, produce a mapping that relates blobs -> their packs
pub fn blob_set(index: &Index) -> Result<HashSet<ObjectId>> {
    debug!("Building a set of all blobs");
    let mut blobs = HashSet::new();

    for (pack_id, manifest) in &index.packs {
        for blob in manifest {
            if !blobs.insert(blob.id) {
                bail!("Duplicate blob {} in pack {}", blob.id, pack_id);
            }
        }
    }

    Ok(blobs)
}

/// Loads the index from the given reader,
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

/// Loads the index with the given ID from the backend,
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
    Ok(index)
}

#[cfg(test)]
mod test {
    use super::*;

    use tempfile::tempfile;

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
                    blob_type: BlobType::Chunk,
                    length: 42,
                    id: ObjectId::hash(b"a chunk"),
                },
                PackManifestEntry {
                    blob_type: BlobType::Chunk,
                    length: 9001,
                    id: ObjectId::hash(b"another chunk"),
                },
            ],
        );
        packs.insert(
            ObjectId::hash(b"pack o'trees"),
            vec![
                PackManifestEntry {
                    blob_type: BlobType::Tree,
                    length: 182,
                    id: ObjectId::hash(b"first tree"),
                },
                PackManifestEntry {
                    blob_type: BlobType::Tree,
                    length: 22,
                    id: ObjectId::hash(b"second tree"),
                },
                PackManifestEntry {
                    blob_type: BlobType::Tree,
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

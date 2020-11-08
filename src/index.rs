use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::prelude::*;
use std::path::Path;
use std::str::FromStr;
use std::sync::mpsc::{Receiver, SyncSender};

use anyhow::*;
use log::*;
use serde_derive::*;

use crate::backend::Backend;
use crate::file_util::check_magic;
use crate::hashing::{HashingWriter, ObjectId};
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

pub fn index(rx: Receiver<PackMetadata>, to_upload: SyncSender<String>) -> Result<()> {
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

        let (index_id, compressed_size) = to_temp_file(&index)?;

        // If we're close enough to our target size, stop
        if compressed_size >= DEFAULT_TARGET_SIZE {
            info!(
                "Index {} finished ({} bytes). Starting another...",
                index_id, compressed_size
            );
            let id_name = format!("{}.index", index_id);
            fs::rename(TEMP_INDEX_LOCATION, &id_name).with_context(|| {
                format!("Couldn't rename {} to {}", TEMP_INDEX_LOCATION, id_name)
            })?;
            to_upload
                .send(id_name.clone())
                .context("indexer -> uploader channel exited early")?;

            index = Index::default();
        }
    }
    if !index.packs.is_empty() {
        let (index_id, compressed_size) = to_temp_file(&index)?;
        info!("Index {} finished ({} bytes)", index_id, compressed_size);

        let id_name = format!("{}.index", index_id);
        fs::rename(TEMP_INDEX_LOCATION, &id_name)
            .with_context(|| format!("Couldn't rename {} to {}", TEMP_INDEX_LOCATION, id_name))?;
        to_upload
            .send(id_name)
            .context("indexer -> uploader channel exited early")?;
    }
    Ok(())
}

// TODO: Obviously this should all take place in a configurable temp directory
//
const TEMP_INDEX_LOCATION: &str = "temp.index";

fn to_temp_file(index: &Index) -> Result<(ObjectId, u64)> {
    // Could we speed things up by reusing the same file handle instead of
    // opening, writing, and closing each time we update the WIP index file?
    // Probably, but we'd have to seek back to the beginning each time,
    // _and_ we'd be assuming that the file grows larger each time.
    // (This might not be true since its contents are compressed...)
    let mut fh = File::create(TEMP_INDEX_LOCATION)
        .with_context(|| format!("Couldn't create {}", TEMP_INDEX_LOCATION))?;
    to_file(&mut fh, index)
}

fn to_file(fh: &mut File, index: &Index) -> Result<(ObjectId, u64)> {
    fh.write_all(MAGIC_BYTES)?;

    let mut zstd = zstd::stream::write::Encoder::new(fh, 0)?;
    zstd.multithread(num_cpus::get_physical() as u32)?;

    let mut hasher = HashingWriter::new(zstd);

    serde_cbor::to_writer(&mut hasher, index)?;

    let (id, zstd) = hasher.finalize();
    let fh = zstd.finish()?;
    fh.sync_all()?;
    let length: u64 = fh.metadata()?.len();

    // Because we rewrite the temp index file over and over, don't rename it here.
    // Rename it in the loop above when it's large enough.
    // (Otherwise we'd be leaving behind a set of index files as large
    // as the pack list.)

    Ok((id, length))
}

pub fn build_master_index(backend: &dyn Backend) -> Result<Index> {
    debug!("Building master index...");

    let mut superseded_indexes = BTreeSet::new();

    // Don't combine the indexes until we know which ones to exclude.
    let mut loaded_indexes: BTreeMap<ObjectId, BTreeMap<ObjectId, PackManifest>> = BTreeMap::new();

    for index in backend.list_indexes()? {
        trace!("Loading index {}...", index);

        let to_load_id = Path::new(&index)
            .file_stem()
            .ok_or_else(|| anyhow!("Couldn't determine index ID from {}", index))
            .and_then(|hex| ObjectId::from_str(hex.to_str().unwrap()))?;

        let mut loaded_index = from_reader(&mut backend.read(&index)?)
            .with_context(|| format!("Couldn't load index {}", index))?;
        superseded_indexes.append(&mut loaded_index.supersedes);
        ensure!(
            loaded_indexes
                .insert(to_load_id, loaded_index.packs)
                .is_none(),
            "Duplicate index file {} read from backend!",
            index
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
pub fn blob_to_pack_map(index: &Index) -> Result<BTreeMap<ObjectId, ObjectId>> {
    let mut mapping = BTreeMap::new();

    for (pack_id, manifest) in &index.packs {
        for blob in manifest {
            ensure!(
                mapping.insert(blob.id, *pack_id).is_none(),
                "Duplicate blob {} in pack {}",
                blob.id,
                pack_id
            );
        }
    }

    Ok(mapping)
}

pub fn from_reader<R: Read>(r: &mut R) -> Result<Index> {
    check_magic(r, MAGIC_BYTES).context("Wrong magic bytes for index file")?;

    let decoder =
        zstd::stream::read::Decoder::new(r).context("Decompression of index file failed")?;
    let index = serde_cbor::from_reader(decoder).context("CBOR decoding of index file failed")?;
    Ok(index)
}

#[cfg(test)]
mod test {
    use super::*;

    use tempfile::tempfile;

    use crate::pack::*;

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
        let index = build_test_index();
        let mut fh = tempfile()?;
        to_file(&mut fh, &index)?;

        fh.seek(std::io::SeekFrom::Start(0))?;
        let read_index = from_reader(&mut fh)?;

        assert_eq!(index, read_index);
        Ok(())
    }
}

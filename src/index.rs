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

#[derive(Debug, Default, Serialize, Deserialize)]
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

        let (index_id, compressed_size) = write_index(&index)?;

        // If we're close enough to our target size, stop
        if compressed_size >= DEFAULT_TARGET_SIZE {
            info!(
                "Index {} finished ({} bytes). Starting another...",
                index_id, compressed_size
            );
            let id_name = format!("{}.index", index_id);
            fs::rename(TEMP_INDEX_LOCATION, &id_name)?;
            to_upload
                .send(id_name.clone())
                .context("indexer -> uploader channel exited early")?;

            index = Index::default();
        }
    }
    if !index.packs.is_empty() {
        let (index_id, compressed_size) = write_index(&index)?;
        info!("Index {} finished ({} bytes)", index_id, compressed_size);

        let id_name = format!("{}.index", index_id);
        fs::rename(TEMP_INDEX_LOCATION, &id_name)?;
        to_upload
            .send(id_name)
            .context("indexer -> uploader channel exited early")?;
    }
    Ok(())
}

// TODO: Obviously this should all take place in a configurable temp directory
//
const TEMP_INDEX_LOCATION: &str = "temp.index";

fn write_index(index: &Index) -> Result<(ObjectId, u64)> {
    let mut fh = File::create(TEMP_INDEX_LOCATION)?;
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
    let mut superseded_indexes = BTreeSet::new();

    // Don't combine the indexes until we know which ones to exclude.
    let mut loaded_indexes: BTreeMap<ObjectId, BTreeMap<ObjectId, PackManifest>> = BTreeMap::new();

    for index in backend.list_indexes()? {
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

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::prelude::*;
use std::sync::mpsc::{Receiver, SyncSender};

use anyhow::*;
use log::*;
use serde_derive::*;

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

    // For each chunked file...
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

pub fn from_reader<R: Read>(r: &mut R) -> Result<Index> {
    check_magic(r, MAGIC_BYTES)?;

    let decoder = zstd::stream::read::Decoder::new(r)?;
    let index = serde_cbor::from_reader(decoder)?;
    Ok(index)
}

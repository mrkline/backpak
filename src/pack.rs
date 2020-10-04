use std::fs::File;
use std::io::{self, SeekFrom};
use std::io::prelude::*;
use std::path::Path;
use std::sync::mpsc::Receiver;

use anyhow::*;
use log::*;
use serde_derive::*;

use crate::chunk::Chunk;
use crate::hashing::ObjectId;
use crate::tree::Tree;

pub const DEFAULT_PACK_TARGET_SIZE: u64 = 1024 * 1024 * 100; // 100 MB

type ZstdEncoder<W> = zstd::stream::write::Encoder<W>;
type ZstdDecoder<R> = zstd::stream::read::Decoder<R>;

/// Packs chunked files received from the given channel.
pub fn pack(rx: Receiver<Chunk>) -> Result<()> {
    let mut packfile = Packfile::new("temp.pack")?;

    let mut bytes_written: u64 = 0;
    let mut bytes_before_next_check = DEFAULT_PACK_TARGET_SIZE;

    // For each chunked file...
    while let Ok(chunk) = rx.recv() {
        // For each chunk in that file...
        // Write the pack into the file, keeping track of how many bytes
        // we've written so far.
        packfile.write_file_chunk(&chunk)?;
        bytes_written += chunk.len() as u64;

        // We've written as many bytes as we want the pack size to to be,
        // but we don't know how much they've compressed to.
        // Flush the compressor to see how much space we've actually taken up.
        if bytes_written >= bytes_before_next_check {
            debug!(
                "Wrote {} bytes into pack <TODO>, checking compressed size...",
                bytes_written
            );

            let compressed_size = packfile.flush_and_check_size()?;

            // If we're close enough to our target size, stop
            if compressed_size >= DEFAULT_PACK_TARGET_SIZE * 9 / 10 {
                debug!(
                    "Compressed size is {} (> 90% of {}). Bailing.",
                    compressed_size, DEFAULT_PACK_TARGET_SIZE
                );
                break;
            }
            // Otherwise, write some more
            else {
                bytes_before_next_check = DEFAULT_PACK_TARGET_SIZE - compressed_size;
                debug!(
                    "Compressed size is {}. Writing another {} bytes",
                    compressed_size, bytes_before_next_check
                );
            }
        }
    }
    packfile.finalize()?;
    Ok(())
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum BlobType {
    Data,
    Tree,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct PackManifestEntry {
    #[serde(rename = "type")]
    blob_type: BlobType,
    length: u32,
    id: ObjectId,
}

pub type PackManifest = Vec<PackManifestEntry>;

struct Packfile {
    writer: ZstdEncoder<File>,
    manifest: PackManifest,
}

impl Packfile {
    fn new<P: AsRef<Path>>(p: P) -> io::Result<Self> {
        let mut fh = File::create(p)?;
        fh.write_all(b"MKBAKPAK")?;

        let mut zstd = ZstdEncoder::new(fh, 0)?;
        zstd.multithread(num_cpus::get_physical() as u32)?;
        Ok(Self {
            writer: zstd,
            manifest: Vec::new(),
        })
    }

    /// Write the given file chunk to the packfile and put its ID in the manifest.
    fn write_file_chunk(&mut self, chunk: &Chunk) -> io::Result<()> {
        let chunk_len: usize = chunk.len();
        assert!(chunk_len <= u32::MAX as usize);

        self.writer.write_all(chunk.bytes())?;
        self.manifest.push(PackManifestEntry {
            blob_type: BlobType::Data,
            length: chunk_len as u32,
            id: chunk.id,
        });
        Ok(())
    }

    fn write_tree(&mut self, tree: &Tree) -> Result<()> {
        let tree_cbor = serde_cbor::to_vec(tree)?;
        assert!(tree_cbor.len() < u32::MAX as usize);

        let id = ObjectId::new(&tree_cbor);

        self.writer.write_all(&tree_cbor)?;
        self.manifest.push(PackManifestEntry {
            blob_type: BlobType::Tree,
            length: tree_cbor.len() as u32,
            id
        });
        Ok(())
    }

    /// Flush the compressor and check the size of the packfile so far.
    /// **Warning:** Doing this too frequently hurts the compression ratio.
    fn flush_and_check_size(&mut self) -> Result<u64> {
        self.writer.flush()?;
        let fh: &File = self.writer.get_ref();
        Ok(fh.metadata()?.len())
    }

    /// Finalize the packfile, returning the manifest and its ID.
    fn finalize(self) -> Result<(ObjectId, PackManifest)> {
        // Serialize the manifest.
        let manifest = serde_cbor::to_vec(&self.manifest)?;
        // A pack file is identified by the hash of its (uncompressed) manifest.
        let id = ObjectId::new(&manifest);

        // Finish the compression stream for blobs and trees.
        // We'll compress the manifest separately so we can decompress it
        // without reading everything before it.
        let mut fh: File = self.writer.finish()?;

        // The manifest CBOR will have lots of redundant data - compress it down.
        // TODO: Is multithreading worth it here?
        // This shouldn't be much data compared to blobs and trees.
        let manifest = zstd::block::compress(&manifest, 0)?;
        let manifest_length = manifest.len() as u32;

        fh.write_all(&manifest)?;

        // Write the length of the (compressed) manifest to the end of the file,
        // making it simple and fast to examine the manifest:
        // read the last four bytes, seek to the start of the manifest,
        // and decompress it.
        fh.write_all(&manifest_length.to_be_bytes())?;

        info!(
            "After finish: {} bytes, ID {:x}",
            fh.metadata()?.len(),
            id
        );
        fh.sync_all()?;
        Ok((id, self.manifest))
    }
}

pub fn read_packfile_manifest(file: &Path) -> Result<PackManifest> {
    let mut fh = File::open(file).with_context(|| format!("Couldn't open {}", file.display()))?;

    fh.seek(SeekFrom::End(-4))?;
    let mut manifest_length: [u8; 4] = [0; 4];
    fh.read_exact(&mut manifest_length)?;

    let manifest_length = u32::from_be_bytes(manifest_length);
    fh.seek(SeekFrom::End(-(manifest_length as i64) - 4))?;
    let decoder = ZstdDecoder::new(fh.take(manifest_length as u64))?;

    let manifest: PackManifest = serde_cbor::from_reader(decoder)?;
    Ok(manifest)
}

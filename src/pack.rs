use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::path::Path;
use std::sync::mpsc::Receiver;

use anyhow::Result;
use log::*;
use serde_derive::*;

use crate::chunk::Chunk;
use crate::hashing::ObjectId;

pub const DEFAULT_PACK_TARGET_SIZE: u64 = 1024 * 1024 * 100; // 100 MB

type ZstdEncoder<W> = zstd::stream::write::Encoder<W>;

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
enum BlobType {
    Data,
    Tree,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
struct PackManifestEntry {
    blob_type: BlobType,
    length: u32,
    hash: ObjectId,
}

type PackManifest = Vec<PackManifestEntry>;

struct Packfile {
    writer: ZstdEncoder<File>,
    manifest: PackManifest,
}

impl Packfile {
    fn new<P: AsRef<Path>>(p: P) -> io::Result<Self> {
        let fh = File::create(p)?;
        let mut zstd = ZstdEncoder::new(fh, 0)?;
        zstd.multithread(num_cpus::get_physical() as u32)?;
        Ok(Self {
            writer: zstd,
            manifest: Vec::new(),
        })
    }

    /// Write the given file chunk to the packfile and put its hash in the manifest.
    fn write_file_chunk(&mut self, chunk: &Chunk) -> io::Result<()> {
        let chunk_len: usize = chunk.len();
        assert!(chunk_len <= u32::MAX as usize);

        self.writer.write_all(chunk.bytes())?;
        self.manifest.push(PackManifestEntry {
            blob_type: BlobType::Data,
            length: chunk_len as u32,
            hash: chunk.hash,
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

    /// Finalize the packfile, returning the manifest and its hash.
    fn finalize(mut self) -> Result<(ObjectId, PackManifest)> {
        let manifest = serde_cbor::to_vec(&self.manifest)?;
        let manifest_len = manifest.len() as u32;

        let hash = ObjectId::new(&manifest);

        self.writer.write_all(&manifest)?;

        let mut fh: File = self.writer.finish()?;
        fh.write_all(&manifest_len.to_be_bytes())?;

        info!(
            "After finish: {} bytes, hash {:x}",
            fh.metadata()?.len(),
            hash
        );
        fh.sync_all()?;
        Ok((hash, self.manifest))
    }
}

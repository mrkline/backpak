use std::fs::{self, File};
use std::io::prelude::*;
use std::io::{self, BufWriter, SeekFrom};
use std::sync::mpsc::*;

use anyhow::*;
use log::*;
use serde_derive::*;

use crate::chunk::Chunk;
use crate::file_util;
use crate::hashing::{HashingReader, ObjectId};
use crate::tree::Tree;
use crate::DEFAULT_TARGET_SIZE;

const MAGIC_BYTES: &[u8] = b"MKBAKPAK";

pub enum Blob {
    Chunk(crate::chunk::Chunk),
    Tree(crate::tree::Tree),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BlobType {
    /// A chunk of a file.
    ///
    /// **TODO:** Restic calls these "data". Should we follow suit?
    Chunk,
    /// File and directory metadata
    Tree,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PackManifestEntry {
    #[serde(rename = "type")]
    pub blob_type: BlobType,
    pub length: u32,
    pub id: ObjectId,
}

pub type PackManifest = Vec<PackManifestEntry>;

#[derive(Debug, Clone)]
pub struct PackMetadata {
    pub id: ObjectId,
    pub manifest: PackManifest,
}

/// Packs chunked files received from the given channel.
pub fn pack(
    rx: Receiver<Blob>,
    to_index: Sender<PackMetadata>,
    to_upload: SyncSender<String>,
) -> Result<()> {
    let mut packfile = PackfileWriter::new()?;

    let mut bytes_written: u64 = 0;
    let mut bytes_before_next_check = DEFAULT_TARGET_SIZE;

    // For each chunked file...
    while let Ok(blob) = rx.recv() {
        // Track how many (uncompressed) bytes we've written to the file so far.
        bytes_written += match blob {
            Blob::Chunk(chunk) => packfile.write_file_chunk(&chunk)?,
            Blob::Tree(tree) => packfile.write_tree(&tree)?,
        };

        // We've written as many bytes as we want the pack size to to be,
        // but we don't know how much they've compressed to.
        // Flush the compressor to see how much space we've actually taken up.
        if bytes_written >= bytes_before_next_check {
            trace!(
                "Wrote {} bytes into pack, checking compressed size...",
                bytes_written
            );

            let compressed_size = packfile.flush_and_check_size()?;

            // If we're close enough to our target size, stop
            if compressed_size >= DEFAULT_TARGET_SIZE * 9 / 10 {
                trace!(
                    "Compressed pack size is {} (> 90% of {}). Starting another pack.",
                    compressed_size,
                    DEFAULT_TARGET_SIZE
                );
                let metadata = packfile.finalize()?;

                to_upload
                    .send(format!("{}.pack", metadata.id))
                    .context("packer -> uploader channel exited early")?;
                to_index
                    .send(metadata)
                    .context("packer -> indexer channel exited early")?;

                packfile = PackfileWriter::new()?;
                bytes_written = 0;
                bytes_before_next_check = DEFAULT_TARGET_SIZE;
            }
            // Otherwise, write some more
            else {
                bytes_before_next_check = DEFAULT_TARGET_SIZE - compressed_size;
                trace!(
                    "Compressed pack size is {}. Writing another {} bytes",
                    compressed_size,
                    bytes_before_next_check
                );
            }
        }
    }
    if bytes_written > 0 {
        let metadata = packfile.finalize()?;
        to_upload
            .send(format!("{}.pack", metadata.id))
            .context("packer -> uploader channel exited early")?;
        to_index
            .send(metadata)
            .context("packer -> indexer channel exited early")?;
    }
    Ok(())
}

type ZstdEncoder<W> = zstd::stream::write::Encoder<W>;
type ZstdDecoder<R> = zstd::stream::read::Decoder<R>;

struct PackfileWriter {
    writer: ZstdEncoder<File>,
    manifest: PackManifest,
}

// TODO: Obviously this should all take place in a configurable temp directory

const TEMP_PACKFILE_LOCATION: &str = "temp.pack";

impl PackfileWriter {
    fn new() -> io::Result<Self> {
        let mut fh = File::create(TEMP_PACKFILE_LOCATION)?;
        fh.write_all(MAGIC_BYTES)?;

        let mut zstd = ZstdEncoder::new(fh, 0)?;
        zstd.multithread(num_cpus::get_physical() as u32)?;
        Ok(Self {
            writer: zstd,
            manifest: Vec::new(),
        })
    }

    /// Write the given file chunk to the packfile and put its ID in the manifest.
    fn write_file_chunk(&mut self, chunk: &Chunk) -> io::Result<u64> {
        let chunk_length = chunk.len();
        assert!(chunk_length <= u32::MAX as usize);

        self.writer.write_all(chunk.bytes())?;
        self.manifest.push(PackManifestEntry {
            blob_type: BlobType::Chunk,
            length: chunk_length as u32,
            id: chunk.id,
        });
        Ok(chunk_length as u64)
    }

    fn write_tree(&mut self, tree: &Tree) -> Result<u64> {
        let tree_cbor = serde_cbor::to_vec(tree)?;
        let tree_length = tree_cbor.len();
        assert!(tree_length < u32::MAX as usize);

        let id = ObjectId::hash(&tree_cbor);

        self.writer.write_all(&tree_cbor)?;
        self.manifest.push(PackManifestEntry {
            blob_type: BlobType::Tree,
            length: tree_length as u32,
            id,
        });
        Ok(tree_length as u64)
    }

    /// Flush the compressor and check the size of the packfile so far.
    /// **Warning:** Doing this too frequently hurts the compression ratio.
    fn flush_and_check_size(&mut self) -> Result<u64> {
        self.writer.flush()?;
        let fh: &File = self.writer.get_ref();
        Ok(fh.metadata()?.len())
    }

    /// Finalize the packfile, returning the manifest and its ID.
    fn finalize(self) -> Result<PackMetadata> {
        // Serialize the manifest.
        let manifest = serde_cbor::to_vec(&self.manifest)?;
        // A pack file is identified by the hash of its (uncompressed) manifest.
        let id = ObjectId::hash(&manifest);

        // Finish the compression stream for blobs and trees.
        // We'll compress the manifest separately so we can decompress it
        // without reading everything before it.
        let mut fh: BufWriter<File> = BufWriter::new(self.writer.finish()?);

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
            "Pack {} finished ({} bytes)",
            id,
            fh.get_ref().metadata()?.len(),
        );
        fh.into_inner()?.sync_all()?;

        fs::rename(TEMP_PACKFILE_LOCATION, format!("{}.pack", id))?;

        Ok(PackMetadata {
            id,
            manifest: self.manifest,
        })
    }
}

/// Verifies everything in the packfile matches the given manifest from the index.
pub fn verify<R: Read>(packfile: &mut R, manifest_from_index: &PackManifest) -> Result<()> {
    check_magic(packfile)?;

    let mut decoder = ZstdDecoder::new(packfile).context("Decompression of blob stream failed")?;

    for entry in manifest_from_index {
        let mut hashing_decoder = HashingReader::new((&mut decoder).take(entry.length as u64));

        let mut buf = Vec::with_capacity(entry.length as usize);
        hashing_decoder.read_to_end(&mut buf)?;

        let (hash, _) = hashing_decoder.finalize();
        ensure!(
            entry.id == hash,
            "Calculated hash of blob ({}) doesn't match ID {}",
            hash,
            entry.id
        );
        debug!("Blob {} matches its ID", entry.id);
    }

    Ok(())
}

pub fn manifest_from_reader<R: Seek + Read>(r: &mut R) -> Result<PackManifest> {
    check_magic(r)?;

    r.seek(SeekFrom::End(-4))?;
    let mut manifest_length: [u8; 4] = [0; 4];
    r.read_exact(&mut manifest_length)?;

    let manifest_length = u32::from_be_bytes(manifest_length);
    let manifest_location = -(manifest_length as i64) - 4;
    r.seek(SeekFrom::End(manifest_location)).with_context(|| {
        format!(
            "Couldn't seek {} bytes from the end of packfile to find manifest",
            manifest_location
        )
    })?;
    let decoder = ZstdDecoder::new(r.take(manifest_length as u64))
        .context("Decompression of packfile manifest failed")?;

    let manifest: PackManifest =
        serde_cbor::from_reader(decoder).context("CBOR decodeing of packfile manifest failed")?;
    Ok(manifest)
}

/// Extracts a single blob from a packfile.
/// Useful for `cat blob`.
pub fn extract_blob<R: Read>(
    packfile: &mut R,
    blob_id: &ObjectId,
    manifest_from_index: &PackManifest,
) -> Result<(PackManifestEntry, Vec<u8>)> {
    assert!(
        manifest_from_index
            .iter()
            .find(|entry| entry.id == *blob_id)
            .is_some(),
        "Given blob ID isn't in the given index"
    );

    check_magic(packfile)?;

    let mut decoder = ZstdDecoder::new(packfile).context("Decompression of blob stream failed")?;

    let mut sink = io::sink();

    for entry in manifest_from_index {
        if entry.id == *blob_id {
            let mut hashing_decoder = HashingReader::new(decoder.take(entry.length as u64));

            let mut buf = Vec::with_capacity(entry.length as usize);
            hashing_decoder.read_to_end(&mut buf)?;

            let (hash, _) = hashing_decoder.finalize();
            ensure!(
                *blob_id == hash,
                "Calculated hash of blob ({}) doesn't match ID {}",
                hash,
                *blob_id
            );

            return Ok((*entry, buf));
        } else {
            io::copy(&mut (&mut decoder).take(entry.length as u64), &mut sink)?;
        }
    }

    unreachable!();
}

fn check_magic<R: Read>(r: &mut R) -> Result<()> {
    file_util::check_magic(r, MAGIC_BYTES).context("Wrong magic bytes for packfile")
}

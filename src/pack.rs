use std::fs::File;
use std::io::prelude::*;
use std::io::{self, SeekFrom};
use std::sync::mpsc::*;

use anyhow::*;
use log::*;
use serde_derive::*;
use tempfile::NamedTempFile;

use crate::file_util;
use crate::hashing::{HashingReader, ObjectId};
use crate::DEFAULT_TARGET_SIZE;

const MAGIC_BYTES: &[u8] = b"MKBAKPAK";

pub enum Blob {
    Chunk(crate::chunk::Chunk),
    /// FIXME: Something more strongly typed than a bag of bytes
    ///        and its ID? Or should we send all chunks over that way too?
    ///        (What's nice is that chunks currently are just arcs to their
    ///        underlying buffers, so sending it here is just a ref bump.)
    Tree {
        bytes: Vec<u8>,
        id: ObjectId,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BlobType {
    /// A chunk of a file.
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

/// Serializes a pack's manifest and get its ID.
///
/// A pack file is identified by the hash of its (uncompressed) manifest.
fn serialize_and_hash(manifest: &[PackManifestEntry]) -> Result<(Vec<u8>, ObjectId)> {
    let manifest = serde_cbor::to_vec(&manifest)?;
    let id = ObjectId::hash(&manifest);

    Ok((manifest, id))
}

/// Packs chunked files received from the given channel.
pub fn pack(
    rx: Receiver<Blob>,
    to_index: Sender<PackMetadata>,
    to_upload: SyncSender<(String, File)>,
) -> Result<()> {
    let mut packfile = PackfileWriter::new()?;

    let mut bytes_written: u64 = 0;
    let mut bytes_before_next_check = DEFAULT_TARGET_SIZE;

    // For each chunked file...
    while let Ok(blob) = rx.recv() {
        // Write a blob and check how many (uncompressed) bytes we've written to the file so far.
        bytes_written += packfile.write_blob(blob)?;

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
                let (metadata, persisted) = packfile.finalize()?;
                let finalized_path = format!("{}.pack", metadata.id);

                to_upload
                    .send((finalized_path, persisted))
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
        let (metadata, persisted) = packfile.finalize()?;
        let finalized_path = format!("{}.pack", metadata.id);
        to_upload
            .send((finalized_path, persisted))
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
    writer: ZstdEncoder<NamedTempFile>,
    manifest: PackManifest,
}

// TODO: Obviously this should all take place in a configurable temp directory

impl PackfileWriter {
    fn new() -> Result<Self> {
        let mut fh = tempfile::Builder::new()
            .prefix("temp-backpak-")
            .suffix(".pack")
            .tempfile_in(&std::env::current_dir()?) // TODO: Configurable?
            .context("Couldn't open temporary packfile for writing")?;

        fh.write_all(MAGIC_BYTES)?;

        let mut zstd = ZstdEncoder::new(fh, 0)?;
        zstd.multithread(num_cpus::get_physical() as u32)?;
        Ok(Self {
            writer: zstd,
            manifest: Vec::new(),
        })
    }

    /// Write the given file chunk or tree to the packfile and add it to the manifest.
    fn write_blob(&mut self, blob: Blob) -> io::Result<u64> {
        let (bytes, blob_type, id) = match &blob {
            Blob::Chunk(chunk) => (chunk.bytes(), BlobType::Chunk, chunk.id),
            Blob::Tree { bytes, id } => (bytes.as_slice(), BlobType::Tree, *id),
        };

        let blob_length = bytes.len();
        assert!(blob_length <= u32::MAX as usize);

        self.writer.write_all(bytes)?;
        self.manifest.push(PackManifestEntry {
            blob_type,
            length: blob_length as u32,
            id,
        });
        Ok(blob_length as u64)
    }

    /// Flush the compressor and check the size of the packfile so far.
    ///
    /// **Warning:** Doing this too frequently hurts the compression ratio.
    fn flush_and_check_size(&mut self) -> Result<u64> {
        self.writer.flush()?;
        let fh = self.writer.get_ref().as_file();
        Ok(fh.metadata()?.len())
    }

    /// Finalize the packfile, returning the manifest & ID with a handle to
    /// the persisted file (so that the uploader doesn't have to reopen it).
    fn finalize(self) -> Result<(PackMetadata, File)> {
        let (manifest, id) = serialize_and_hash(&self.manifest)?;

        // Finish the compression stream for blobs and trees.
        // We'll compress the manifest separately so we can decompress it
        // without reading everything before it.
        let mut fh: NamedTempFile = self.writer.finish()?;

        // The manifest CBOR will have lots of redundant data - compress it down.
        // TODO: Is multithreading worth it here?
        // This shouldn't be much data compared to blobs and trees.
        let mut manifest = zstd::block::compress(&manifest, 0)?;

        // Write the length of the (compressed) manifest to the end of the file,
        // making it simple and fast to examine the manifest:
        // read the last four bytes, seek to the start of the manifest,
        // and decompress it.
        let manifest_length = manifest.len() as u32;
        manifest.extend_from_slice(&manifest_length.to_be_bytes());
        fh.write_all(&manifest)?;

        // All done! Sync, persist, and go home.
        fh.as_file().sync_all()?;
        let pack_name = format!("{}.pack", id);
        let persisted = fh
            .persist(&pack_name)
            .with_context(|| format!("Couldn't persist finished pack to {}", pack_name))?;

        info!(
            "Pack {}.pack finished ({} bytes)",
            id,
            persisted.metadata()?.len(),
        );

        Ok((
            PackMetadata {
                id,
                manifest: self.manifest,
            },
            persisted,
        ))
    }
}

/// Verifies everything in the packfile matches the given manifest from the index.
pub fn verify<R: Read + Seek>(
    packfile: &mut R,
    manifest_from_index: &[PackManifestEntry],
) -> Result<()> {
    check_magic(packfile)?;

    let mut decoder = ZstdDecoder::new(packfile).context("Decompression of blob stream failed")?;

    for entry in manifest_from_index {
        let mut hashing_decoder = HashingReader::new((&mut decoder).take(entry.length as u64));

        let mut buf = Vec::with_capacity(entry.length as usize);
        hashing_decoder.read_to_end(&mut buf)?;

        let (hash, _) = hashing_decoder.finalize();
        ensure!(
            entry.id == hash,
            "Calculated hash of blob ({}) doesn't match its ID ({})",
            hash,
            entry.id
        );
        trace!("Blob {} matches its ID", entry.id);
    }

    // Attempting to read the manifest using `serde_cbor::from_reader()`
    // without the correct `take()` length produces errors.
    // Should we rearrange the file so that isn't a problem?
    // Or is that fine, since verification isn't as performance critical
    // as other interactions?
    let mut packfile = decoder.finish();
    let manifest_from_file = manifest_from_reader(&mut packfile)?;

    ensure!(
        manifest_from_index == manifest_from_file,
        "Pack manifest doesn't match its index entry and file contents"
    );

    Ok(())
}

/// Reads the pack manifest from the back of the given reader.
///
/// _Does not_ check the pack's magic bytes or anything besides the manifest.
pub fn manifest_from_reader<R: Seek + Read>(r: &mut R) -> Result<PackManifest> {
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
        serde_cbor::from_reader(decoder).context("CBOR decoding of packfile manifest failed")?;
    Ok(manifest)
}

/// Extracts a single blob from a packfile.
/// Useful for `cat blob`.
pub fn extract_blob<R: Read>(
    packfile: &mut R,
    blob_id: &ObjectId,
    manifest_from_index: &[PackManifestEntry],
) -> Result<(PackManifestEntry, Vec<u8>)> {
    assert!(
        manifest_from_index.iter().any(|entry| entry.id == *blob_id),
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

pub fn check_magic<R: Read>(r: &mut R) -> Result<()> {
    file_util::check_magic(r, MAGIC_BYTES).context("Wrong magic bytes for packfile")
}

#[cfg(test)]
mod test {
    use super::*;

    use std::fs;

    use crate::chunk;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    /// Pack manifest and ID remains stable from build to build.
    fn stability() -> Result<()> {
        init();

        let manifest = vec![
            PackManifestEntry {
                blob_type: BlobType::Chunk,
                length: 42,
                id: ObjectId::hash(b"first"),
            },
            PackManifestEntry {
                blob_type: BlobType::Tree,
                length: 22,
                id: ObjectId::hash(b"second"),
            },
            PackManifestEntry {
                blob_type: BlobType::Chunk,
                length: 42,
                id: ObjectId::hash(b"third"),
            },
        ];

        let (manifest, id) = serialize_and_hash(&manifest)?;

        // ID remains stable
        assert_eq!(
            format!("{}", id),
            "3b070d0356a4b19eb65f68a5268263e1fc7661923fed5f4994ed840d"
        );
        // Contents remain stable
        // (We could just use the ID and length,
        // but having some example CBOR in the repo seems helpful.)
        let from_example = fs::read("tests/references/pack.stability")?;
        assert_eq!(manifest, from_example);
        Ok(())
    }

    #[test]
    fn smoke() -> Result<()> {
        init();

        let chunks = chunk::chunk_file("tests/references/sr71.txt")
            .context("Couldn't chunk reference file")?;
        let (chunk_tx, chunk_rx) = channel();
        let (pack_tx, pack_rx) = channel();
        let (upload_tx, upload_rx) = sync_channel(1);

        let chunk_packer = std::thread::spawn(move || pack(chunk_rx, pack_tx, upload_tx));

        let upload_chucker = std::thread::spawn(move || -> Result<()> {
            // This test doesn't actually care about the files themselves,
            // at least for now. Axe em!
            while let Ok((to_upload, _)) = upload_rx.recv() {
                fs::remove_file(&to_upload)
                    .with_context(|| format!("Couldn't remove completed packfile {}", to_upload))?;
            }
            Ok(())
        });

        for chunk in &chunks {
            chunk_tx.send(Blob::Chunk(chunk.clone()))?
        }
        drop(chunk_tx);

        let mut merged_manifest: PackManifest = Vec::new();
        while let Ok(mut metadata) = pack_rx.recv() {
            merged_manifest.append(&mut metadata.manifest);
        }

        chunk_packer.join().unwrap()?;
        upload_chucker.join().unwrap()?;

        assert_eq!(chunks.len(), merged_manifest.len());
        for (chunk, manifest_entry) in chunks.iter().zip(merged_manifest.iter()) {
            assert_eq!(manifest_entry.blob_type, BlobType::Chunk);
            assert_eq!(manifest_entry.id, chunk.id);
            assert_eq!(manifest_entry.length as usize, chunk.len());
        }
        Ok(())
    }
}

//! Build, read, and write compressed packs of [blobs](blob::Blob),
//! suitable for storing in a [backend]
//!
//! A pack file contains:
//! 1. Magic bytes
//! 2. A zstd-compressed stream of all blobs in the file
//! 3. A *separate* zstd stream of the CBOR-encoded manifest.
//!    Each manifest entry contains its blob's type, length, and ID.
//! 4. A 32-bit, big-endian manifest length.
//!
//! Compressing the manifest separately and ending with its length makes it trivial to read
//! without having to decompress or read any blobs first.
//!
//! The hash of the manifest is the pack's ID, since it uniquely describes the file.
//! At the end of a backup, each pack's manifest is stored in an index.
//! This means future readers don't need to reference the manifest unless rebuilding an index
//! or verifying the pack.

use std::fs::File;
use std::io::prelude::*;
use std::io::{self, SeekFrom};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    mpsc::{Receiver, SyncSender},
};

use anyhow::{Context, Result, ensure};
use byte_unit::Byte;
use serde_derive::{Deserialize, Serialize};
use tempfile::NamedTempFile;
use tracing::*;

use crate::backend;
use crate::blob::{self, Blob};
use crate::file_util::{self, nice_size};
use crate::hashing::{HashingReader, ObjectId};
use crate::progress::AtomicCountWrite;
use crate::tree;

pub const MAGIC_BYTES: &[u8] = b"MKBAKPAK1";

/// The desired size of [crate::pack] files
pub const DEFAULT_PACK_SIZE: Byte = Byte::from_u64(100_000_000); // 100 MB

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PackManifestEntry {
    #[serde(rename = "type")]
    pub blob_type: blob::Type,
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
    let mut manifest_cbor = Vec::new();
    ciborium::into_writer(&manifest, &mut manifest_cbor)?;
    let id = ObjectId::hash(&manifest_cbor);

    Ok((manifest_cbor, id))
}

/// Packs blobs received from the given channel.
/// Returns the number of bytes packed
pub fn pack(
    target_size: Byte,
    rx: Receiver<Blob>,
    to_index: SyncSender<PackMetadata>,
    to_upload: SyncSender<(String, File)>,
    total_bytes_packed: &AtomicU64,
    total_bytes_compressed: &AtomicU64,
) -> Result<()> {
    let target_size = target_size.as_u64();
    let mut writer = PackfileWriter::new(total_bytes_compressed)?;

    let mut pass_bytes_written: u64 = 0; // Bytes written since the last size check
    let mut bytes_in_pack: u64 = 0;
    let mut bytes_before_next_check = target_size;

    // For each blob...
    while let Ok(blob) = rx.recv() {
        // Each blob arriving here is assumed to be unique per backup run,
        // i.e., the backup/prune/copy main thread handles deduplication.
        // Handling it here seems better at first (DRY) but would only complicate things:
        //
        // 1. `backup` prints out the path of each chunk and whether it was deduped.
        //    To do that here we'd have to pass each blob's path (or a closure?)
        //    through the rx channel.
        //
        // 2. This set of "already backed up blobs" is prepopulated,
        //    and in different ways for different commands.
        //    `backup` and `copy` just use all blobs in the master index.
        //    `prune` uses all blobs we *don't need to repack*.
        //    We'd also have to pass that here, and then...
        //
        // 3. We're running two instances of this function in two threads,
        //    one for trees and one for chunks.
        //    They'd either have to share the set via a mutex,
        //    or we'd have to write code here to figure out which packer we are and
        //    split the set accordingly.
        //    Please no! All the code so far is agnostic to which packer it is.
        //
        // We don't check this invariant here to avoid extra O(N) RAM usage,
        // where N is every single blob in the backup run.

        // Write a blob and check how many (uncompressed) bytes we've written to the file so far.
        let blob_size = writer.write_blob(blob)?;
        pass_bytes_written += blob_size;
        bytes_in_pack += blob_size;
        total_bytes_packed.fetch_add(blob_size, Ordering::Relaxed);

        // If we exceed our target size, stop.
        // We'll probably overshoot since this isn't flushing the Zstd buffer,
        // but it's quick and keeps us from running away.
        let mut end_pack = false;
        if writer.check_size()? >= target_size {
            let compressed_size = writer.flush_and_check_size()?;
            trace!(
                "Compressed pack is {} (>= target of {}). Starting next pack.",
                nice_size(compressed_size),
                nice_size(target_size)
            );
            end_pack = true;
        }
        // We've written as many bytes as we want the pack size to to be,
        // but we don't know how much they've compressed to.
        // Flush the compressor to see how much space we've actually taken up.
        else if pass_bytes_written >= bytes_before_next_check {
            trace!(
                "Wrote {} bytes into pack, checking compressed size...",
                nice_size(bytes_in_pack)
            );

            let compressed_size = writer.flush_and_check_size()?;

            // If we pass our target size, stop
            if compressed_size >= target_size {
                end_pack = true;
                trace!(
                    "Compressed pack is {} (>= target of {}). Starting next pack.",
                    nice_size(compressed_size),
                    nice_size(target_size)
                );
            }
            // Otherwise, write some more
            else {
                // Take our current compression ratio to estimate how much more
                // we need to write to hit the target pack size.
                let current_ratio = bytes_in_pack as f64 / compressed_size as f64;
                bytes_before_next_check =
                    (current_ratio * (target_size - compressed_size) as f64) as u64;
                pass_bytes_written = 0;
                trace!(
                    "Compressed pack is {} (ratio {:.02}). Writing {} before next flush",
                    nice_size(compressed_size),
                    current_ratio,
                    nice_size(bytes_before_next_check)
                );
            }
        }

        if end_pack {
            let (metadata, persisted) = writer.finalize()?;
            let finalized_path = format!("{}.pack", metadata.id);

            to_upload
                .send((finalized_path, persisted))
                .context("packer -> uploader channel exited early")?;
            to_index
                .send(metadata)
                .context("packer -> indexer channel exited early")?;

            writer = PackfileWriter::new(total_bytes_compressed)?;
            pass_bytes_written = 0;
            bytes_in_pack = 0;
            bytes_before_next_check = target_size;
        }
    }
    if bytes_in_pack > 0 {
        let (metadata, persisted) = writer.finalize()?;
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

type ZstdEncoder<W> = zstd::stream::write::Encoder<'static, W>;
type ZstdDecoder<R> = zstd::stream::read::Decoder<'static, R>;

struct PackfileWriter<'a> {
    writer: ZstdEncoder<AtomicCountWrite<'a, NamedTempFile>>,
    manifest: PackManifest,
}

// TODO: Obviously this should all take place in a configurable temp directory

impl<'a> PackfileWriter<'a> {
    fn new(byte_count: &'a AtomicU64) -> Result<Self> {
        let mut fh = tempfile::Builder::new()
            .prefix("temp-backpak-")
            .suffix(".pack")
            .tempfile_in(".")
            .context("Couldn't open temporary packfile for writing")?;

        fh.write_all(MAGIC_BYTES)?;
        let acw = AtomicCountWrite::new(fh, byte_count);
        let mut zstd = ZstdEncoder::new(acw, 0)?;
        zstd.multithread(num_cpus::get_physical() as u32)?;
        Ok(Self {
            writer: zstd,
            manifest: Vec::new(),
        })
    }

    /// Write the given file chunk or tree to the packfile and add it to the manifest.
    fn write_blob(&mut self, blob: Blob) -> io::Result<u64> {
        let blob_bytes: &[u8] = blob.bytes();

        let blob_length = blob_bytes.len();
        assert!(blob_length <= u32::MAX as usize);

        self.writer.write_all(blob_bytes)?;
        self.manifest.push(PackManifestEntry {
            blob_type: blob.kind,
            length: blob_length as u32,
            id: blob.id,
        });
        Ok(blob_length as u64)
    }

    /// Flush the compressor and check the size of the packfile so far.
    ///
    /// **Warning:** Doing this too frequently hurts the compression ratio, at least a little.
    fn flush_and_check_size(&mut self) -> Result<u64> {
        self.writer.flush()?;
        let pos = self.writer.get_ref().get_ref().stream_position()?;
        Ok(pos)
    }

    /// Check the size of the underlying compressed file *without* flushing.
    ///
    /// Doesn't account for whatever data is in the Zstd buffer,
    /// but doesn't change compression ratios either.
    fn check_size(&self) -> Result<u64> {
        let pos = self.writer.get_ref().get_ref().stream_position()?;
        Ok(pos)
    }

    /// Finalize the packfile, returning the manifest & ID with a handle to
    /// the persisted file (so that the uploader doesn't have to reopen it).
    fn finalize(self) -> Result<(PackMetadata, File)> {
        let (manifest, id) = serialize_and_hash(&self.manifest)?;

        // Finish the compression stream for blobs and trees.
        // We'll compress the manifest separately so we can decompress it
        // without reading everything before it.
        let mut fh: NamedTempFile = self.writer.finish()?.into_inner();

        // The manifest CBOR will have lots of redundant data - compress it down.
        // TODO: Is multithreading worth it here?
        // This shouldn't be much data compared to blobs and trees.
        let mut manifest = zstd::bulk::compress(&manifest, 0)?;

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
    blobs_read: &AtomicU64,
) -> Result<()> {
    check_magic(packfile)?;

    let mut decoder = ZstdDecoder::new(packfile).context("Decompression of blob stream failed")?;

    for entry in manifest_from_index {
        let mut hashing_decoder = HashingReader::new((&mut decoder).take(entry.length as u64));

        io::copy(&mut hashing_decoder, &mut io::sink())?;

        let (hash, _) = hashing_decoder.finalize();
        ensure!(
            entry.id == hash,
            "Calculated hash of blob ({}) doesn't match its ID ({})",
            hash,
            entry.id
        );
        blobs_read.fetch_add(1, Ordering::Relaxed);
    }

    // Attempting to read the manifest from CBOR
    // without the correct `take()` length produces errors.
    // Should we rearrange the file so that isn't a problem?
    // Or is that fine, since verification isn't as performance critical
    // as other interactions?
    let mut packfile = decoder.finish();
    let (manifest_from_file, _id) = manifest_from_reader(&mut packfile)?;

    ensure!(
        manifest_from_index == manifest_from_file,
        "Pack manifest doesn't match its index entry and file contents"
    );

    Ok(())
}

/// Reads the pack manifest from the back of the given reader,
/// also returning its calculated ID.
///
/// _Does not_ check the pack's magic bytes or anything besides the manifest.
fn manifest_from_reader<R: Seek + Read>(r: &mut R) -> Result<(PackManifest, ObjectId)> {
    r.seek(SeekFrom::End(-4))?;
    let mut manifest_length: [u8; 4] = [0; 4];
    r.read_exact(&mut manifest_length)?;

    let manifest_length = u32::from_be_bytes(manifest_length);
    let manifest_location = -(manifest_length as i64) - 4;
    r.seek(SeekFrom::End(manifest_location)).with_context(|| {
        format!(
            "Couldn't seek {} bytes from the end of the pack to find the manifest",
            manifest_location
        )
    })?;
    let decoder = ZstdDecoder::new(r.take(manifest_length as u64))
        .context("Decompression of pack manifest failed")?;
    let mut hasher = HashingReader::new(decoder);

    let manifest: PackManifest =
        ciborium::from_reader(&mut hasher).context("CBOR decoding of the pack manifest failed")?;
    let (id, _) = hasher.finalize();
    Ok((manifest, id))
}

/// Loads the manifest of the pack with the given ID from the backend,
/// verifying its contents match its ID.
pub fn load_manifest(
    id: &ObjectId,
    cached_backend: &backend::CachedBackend,
) -> Result<PackManifest> {
    debug!("Loading pack manifest {}", id);
    let mut fh = cached_backend.read_pack(id)?;
    check_magic(&mut fh)?;

    let (manifest, calculated_id) =
        manifest_from_reader(&mut fh).with_context(|| format!("Couldn't load pack {}", id))?;
    ensure!(
        *id == calculated_id,
        "Pack {}'s manifest changed! Now hashes to {}",
        id,
        calculated_id
    );
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

/// Reads in a packfile (presumably of all trees) and appends them to the given forest
pub fn append_to_forest<R: Read + Seek>(
    packfile: &mut R,
    manifest_from_index: &[PackManifestEntry],
    forest: &mut tree::Forest,
) -> Result<()> {
    check_magic(packfile)?;

    let mut decoder = ZstdDecoder::new(packfile).context("Decompression of blob stream failed")?;

    for entry in manifest_from_index {
        // If it's not a tree, or if we have it already, skip it!
        let skip = if entry.blob_type != blob::Type::Tree {
            warn!(
                "Chunk {} found in pack where we expected only trees",
                entry.id
            );
            true
        } else {
            forest.contains_key(&entry.id)
        };
        let entry_length = entry.length as u64;
        if skip {
            assert_eq!(
                entry_length,
                io::copy(&mut (&mut decoder).take(entry_length), &mut io::sink())?
            );
            continue;
        }

        let mut hashing_decoder = HashingReader::new((&mut decoder).take(entry_length));

        let to_add: tree::Tree = ciborium::from_reader(&mut hashing_decoder)
            .with_context(|| format!("CBOR decoding of tree {} failed", entry.id))?;

        let (hash, _) = hashing_decoder.finalize();
        ensure!(
            entry.id == hash,
            "Calculated hash of tree ({}) doesn't match its ID ({})",
            hash,
            entry.id
        );

        assert!(
            forest
                .insert(entry.id, std::sync::Arc::new(to_add))
                .is_none()
        );
    }
    Ok(())
}

pub fn check_magic<R: Read>(r: &mut R) -> Result<()> {
    file_util::check_magic(r, MAGIC_BYTES).context("Wrong magic bytes for packfile")
}

#[cfg(test)]
mod test {
    use super::*;

    use std::fs;
    use std::sync::mpsc::sync_channel;

    use crate::chunk;

    #[test]
    /// Pack manifest and ID remains stable from build to build.
    fn stability() -> Result<()> {
        let manifest = vec![
            PackManifestEntry {
                blob_type: blob::Type::Chunk,
                length: 42,
                id: ObjectId::hash(b"first"),
            },
            PackManifestEntry {
                blob_type: blob::Type::Tree,
                length: 22,
                id: ObjectId::hash(b"second"),
            },
            PackManifestEntry {
                blob_type: blob::Type::Chunk,
                length: 42,
                id: ObjectId::hash(b"third"),
            },
        ];

        let (manifest, id) = serialize_and_hash(&manifest)?;

        // ID remains stable
        assert_eq!(
            format!("{}", id),
            "7c3gq0qmkioptdivd2iid0j3s7u7coci7vmluicktm20q"
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
        let chunks: Vec<_> = chunk::chunk_file("tests/references/sr71.txt")
            .context("Couldn't chunk reference file")?
            .collect();
        let (chunk_tx, chunk_rx) = sync_channel(0);
        let (pack_tx, pack_rx) = sync_channel(0);
        let (upload_tx, upload_rx) = sync_channel(0);

        let unused_byte_count = std::sync::atomic::AtomicU64::new(0);
        let chunk_packer = std::thread::spawn(move || {
            pack(
                DEFAULT_PACK_SIZE,
                chunk_rx,
                pack_tx,
                upload_tx,
                &unused_byte_count,
                &unused_byte_count,
            )
        });

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
            chunk_tx.send(chunk.clone())?
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
            assert_eq!(manifest_entry.blob_type, blob::Type::Chunk);
            assert_eq!(manifest_entry.id, chunk.id);
            assert_eq!(manifest_entry.length as usize, chunk.bytes().len());
        }
        Ok(())
    }
}

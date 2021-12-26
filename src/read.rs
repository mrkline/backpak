//! Tools to traverse a repository, reading blobs
//!
//! This is ultimately how we read backups back out for restore, repack, etc.
use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufReader, SeekFrom};

use anyhow::{anyhow, ensure, Context, Result};
use log::*;

use crate::backend;
use crate::counters;
use crate::hashing::{HashingReader, ObjectId};
use crate::index;
use crate::pack;

type ZstdDecoder<R> = zstd::stream::read::Decoder<R>;

/// Info about the currently-loaded packfile.
///
/// [`BlobReader`](BlobReader) works by lazily opening pack files containing
/// the requested blobs, seeking forward through and (hopefully rarely!)
/// restarting the compressed zstd stream to get it.
struct CurrentPackfile<'a> {
    id: ObjectId,
    blob_stream: ZstdDecoder<BufReader<File>>,
    manifest: &'a pack::PackManifest,
    current_blob_index: usize,
}

pub struct BlobReader<'a> {
    cached_backend: &'a backend::CachedBackend,
    index: &'a index::Index,
    blob_map: &'a index::BlobMap,
    current_pack: Option<CurrentPackfile<'a>>,
}

impl<'a> BlobReader<'a> {
    pub fn new(
        cached_backend: &'a backend::CachedBackend,
        index: &'a index::Index,
        blob_map: &'a index::BlobMap,
    ) -> Self {
        Self {
            cached_backend,
            index,
            blob_map,
            current_pack: None,
        }
    }

    pub fn read_blob(&mut self, blob_id: &ObjectId) -> Result<Vec<u8>> {
        let pack_id: ObjectId = *self
            .blob_map
            .get(blob_id)
            .ok_or_else(|| anyhow!("Blob {} not found in any pack", blob_id))?;

        let should_load_new_pack = match &self.current_pack {
            None => true,
            Some(CurrentPackfile { id, .. }) => *id != pack_id,
        };

        if should_load_new_pack {
            self.load_pack(pack_id)
                .with_context(|| format!("Couldn't load pack {}", pack_id))?;
        }
        let mut current_pack = self.current_pack.as_mut().unwrap();

        // Cool, we're in the right pack. Let's see where the blob is.
        let blob_index = index_of(blob_id, current_pack.manifest, &current_pack.id)?;
        if blob_index < current_pack.current_blob_index {
            warn!(
                "Restarting pack since we're at blob {} and want {} (can't read packs backwards)",
                current_pack.current_blob_index, blob_index
            );
            self.restart_stream()?;

            // self.current_pack was moved; update the reference.
            current_pack = self.current_pack.as_mut().unwrap();
            assert_eq!(current_pack.current_blob_index, 0);
        }
        assert!(blob_index >= current_pack.current_blob_index);

        let mut sink = io::sink();

        if blob_index != current_pack.current_blob_index {
            trace!(
                "Streaming past {} blobs to get to blob {}",
                blob_index - current_pack.current_blob_index,
                blob_id
            );
        }

        let mut blob_bytes = None;

        while blob_index >= current_pack.current_blob_index {
            let entry: &pack::PackManifestEntry =
                &current_pack.manifest[current_pack.current_blob_index];

            if blob_index != current_pack.current_blob_index {
                // Skip blobs until we get to the one we want.
                io::copy(
                    &mut (&mut current_pack.blob_stream).take(entry.length as u64),
                    &mut sink,
                )
                .with_context(|| format!("Couldn't read past blob {}", entry.id))?;
                counters::bump(counters::Op::PackSkippedBlob);
            } else {
                // This is it!
                let mut hashing_decoder =
                    HashingReader::new((&mut current_pack.blob_stream).take(entry.length as u64));
                let mut buf = Vec::with_capacity(entry.length as usize);
                hashing_decoder.read_to_end(&mut buf)?;
                let (hash, _) = hashing_decoder.finalize();
                ensure!(
                    *blob_id == hash,
                    "Calculated hash of blob ({}) doesn't match ID {}",
                    hash,
                    *blob_id
                );
                blob_bytes = Some(buf);
            }

            current_pack.current_blob_index += 1;
        }

        blob_bytes.ok_or_else(|| {
            anyhow!(
                "Couldn't find blob {} in pack {}, but the manifest says it's there",
                blob_id,
                current_pack.id
            )
        })
    }

    // This pokes around in the guts of a packfile, so it should arguably be in
    // pack.rs, but is it worth breaking up?

    fn load_pack(&mut self, id: ObjectId) -> Result<()> {
        debug!("Loading pack {}", id);
        let mut file = self.cached_backend.read_pack(&id)?;
        pack::check_magic(&mut file)?;

        let manifest = self
            .index
            .packs
            .get(&id)
            .ok_or_else(|| anyhow!("Couldn't find pack {} manifest in the index", id))?;

        let blob_stream = ZstdDecoder::new(file).context("Decompression of blob stream failed")?;

        let current_blob_index = 0;

        self.current_pack = Some(CurrentPackfile {
            id,
            blob_stream,
            manifest,
            current_blob_index,
        });

        Ok(())
    }

    fn restart_stream(&mut self) -> Result<()> {
        let mut current_pack: CurrentPackfile = self
            .current_pack
            .take()
            .expect("restart_stream called before pack was loaded");

        let mut file: BufReader<File> = current_pack.blob_stream.finish();
        // Seek back to the start of the zstd stream, past the magic bytes.
        file.seek(SeekFrom::Start(8))?;
        let blob_stream =
            ZstdDecoder::with_buffer(file).context("Decompression of blob stream failed")?;

        // Put the new stream back into current_pack and stuff that back into Self.
        current_pack.blob_stream = blob_stream;
        current_pack.current_blob_index = 0;
        self.current_pack = Some(current_pack);
        counters::bump(counters::Op::PackStreamRestart);

        Ok(())
    }
}

fn index_of(
    id: &ObjectId,
    manifest: &[pack::PackManifestEntry],
    pack_id: &ObjectId,
) -> Result<usize> {
    let (index, _) = manifest
        .iter()
        .enumerate()
        .find(|(_idx, entry)| entry.id == *id)
        .ok_or_else(|| {
            anyhow!(
                "Pack {} doesn't contain blob {}, but blob map says it does",
                pack_id,
                id
            )
        })?;
    Ok(index)
}

#[cfg(test)]
mod test {
    use super::*;

    use std::collections::*;
    use std::sync::mpsc::*;

    use crate::blob;
    use crate::chunk;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn smoke() -> Result<()> {
        init();

        // Create a backend with a single pack from our reference files
        let backup_dir = tempfile::tempdir().expect("Failed to create temp test directory");
        let backup_path = backup_dir.path();
        backend::initialize(backup_path)?;
        let backend = backend::open(backup_path)?;

        let mut chunks = Vec::new();

        chunks.extend(chunk::chunk_file("tests/references/sr71.txt")?);
        chunks.extend(chunk::chunk_file("tests/references/index.stability")?);
        chunks.extend(chunk::chunk_file("tests/references/pack.stability")?);
        chunks.extend(chunk::chunk_file("tests/references/README.md")?);
        assert_eq!(chunks.len(), 4);

        let (chunk_tx, chunk_rx) = channel();
        let (pack_tx, pack_rx) = channel();
        let (upload_tx, upload_rx) = sync_channel(1);

        let chunk_packer = std::thread::spawn(move || pack::pack(chunk_rx, pack_tx, upload_tx));

        let uploader = std::thread::spawn(move || -> Result<backend::CachedBackend> {
            let mut num_packs = 0;
            while let Ok((path, fh)) = upload_rx.recv() {
                backend.write(&path, fh)?;
                num_packs += 1;
            }

            assert_eq!(num_packs, 1);
            Ok(backend)
        });

        for chunk in &chunks {
            chunk_tx.send(chunk.clone())?
        }
        drop(chunk_tx);

        // Instead of writing out an index file with index::index()
        // and reading it back in, let's just synthesize the needed info.
        let index = {
            let metadata = pack_rx.recv()?;
            let supersedes = BTreeSet::new();
            let mut packs = index::PackMap::new();
            packs.insert(metadata.id, metadata.manifest);

            index::Index { packs, supersedes }
        };
        let blob_map = index::blob_to_pack_map(&index)?;

        chunk_packer.join().unwrap()?;
        let backend = uploader.join().unwrap()?;

        // With all that fun over with,
        // (Should we wrap that in some utility function(s) for testing?
        // Or is each test bespoke enough that it wouldn't be helpful?)
        // let's test our reader.
        let mut reader = BlobReader::new(&backend, &index, &blob_map);

        // Read the first chunk:
        readback(&chunks[0], &mut reader)?;
        // Read it again, forcing a restart.
        readback(&chunks[0], &mut reader)?;

        // Seek to the third chunk
        readback(&chunks[2], &mut reader)?;

        // And restart to get the second
        readback(&chunks[1], &mut reader)?;
        // Get the last!
        readback(&chunks[3], &mut reader)?;

        Ok(())
    }

    fn readback(blob: &blob::Blob, reader: &mut BlobReader) -> Result<()> {
        let read_blob = reader
            .read_blob(&blob.id)
            .with_context(|| format!("Couldn't read {}", blob.id))?;
        assert_eq!(read_blob, blob.bytes());
        Ok(())
    }
}

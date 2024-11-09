//! Tools to traverse a repository, reading blobs
//!
//! This is ultimately how we read backups back out for restore, repack, etc.
use std::{cmp::Ordering, io::prelude::*, rc::Rc, time::Instant};

use anyhow::{anyhow, ensure, Context, Result};
use mut_binary_heap::{BinaryHeap, FnComparator};
use rustc_hash::FxHashSet;
use tracing::*;

use crate::backend;
use crate::blob;
use crate::counters;
use crate::file_util;
use crate::hashing::{HashingReader, ObjectId};
use crate::index;
use crate::pack;

type ZstdDecoder<R> = zstd::stream::read::Decoder<'static, R>;

struct TimestampedChunk {
    stamp: Instant,
    chunk: Rc<Vec<u8>>,
}

impl TimestampedChunk {
    fn new(chunk: Vec<u8>) -> Self {
        let stamp = Instant::now();
        let chunk = Rc::new(chunk);
        Self { stamp, chunk }
    }
}

fn min_time(a: &TimestampedChunk, b: &TimestampedChunk) -> Ordering {
    b.stamp.cmp(&a.stamp)
}

type ChunkOrder = fn(&TimestampedChunk, &TimestampedChunk) -> Ordering;
type ChunkComparator = FnComparator<ChunkOrder>;

struct ChunkCache {
    cache: BinaryHeap<ObjectId, TimestampedChunk, ChunkComparator>,
    space_used: usize,
}

impl ChunkCache {
    fn new() -> Self {
        // min heap by timestamp
        let cache: BinaryHeap<ObjectId, TimestampedChunk, ChunkComparator> =
            BinaryHeap::new_by(min_time);
        let space_used = 0;
        Self { cache, space_used }
    }

    fn get(&mut self, id: &ObjectId) -> Option<Rc<Vec<u8>>> {
        if let Some(mut tb) = self.cache.get_mut(id) {
            tb.stamp = Instant::now();
            Some(tb.chunk.clone())
        } else {
            None
        }
    }

    fn insert(&mut self, id: &ObjectId, chunk: &[u8]) {
        // If this chunk is already in-cache, just bump its timestamp.
        if let Some(mut tb) = self.cache.get_mut(id) {
            tb.stamp = Instant::now();
            return;
        }

        let new_entry = TimestampedChunk::new(Vec::from(chunk));
        self.space_used += chunk.len();
        self.cache.push(*id, new_entry);
    }

    fn shrink_to(&mut self, new_size: usize) {
        let mut num_evicted: usize = 0;

        // Free up needed space
        while !self.cache.is_empty() && self.space_used > new_size {
            let popped = self.cache.pop().unwrap();
            assert!(self.space_used >= popped.chunk.len());
            self.space_used -= popped.chunk.len();
            num_evicted += 1;
        }

        trace!(
            "Evicted {num_evicted} chunks from cache, {} ({}) left",
            self.cache.len(),
            file_util::nice_size(self.space_used as u64)
        )
    }
}

pub struct ChunkReader<'a> {
    cached_backend: &'a backend::CachedBackend,
    index: &'a index::Index,
    blob_map: &'a index::BlobMap<'a>,
    cache: ChunkCache,
    read_packs: FxHashSet<ObjectId>,
    biggest_pack_size: usize,
}

impl<'a> ChunkReader<'a> {
    pub fn new(
        cached_backend: &'a backend::CachedBackend,
        index: &'a index::Index,
        blob_map: &'a index::BlobMap,
    ) -> Self {
        let cache = ChunkCache::new();
        Self {
            cached_backend,
            index,
            blob_map,
            cache,
            read_packs: FxHashSet::default(),
            biggest_pack_size: 0,
        }
    }

    /// Just get a blob's size from the index. Much cheaper than actually reading the blob.
    pub fn blob_size(&mut self, id: &ObjectId) -> Result<u32> {
        let pack_id: ObjectId = **self
            .blob_map
            .get(id)
            .ok_or_else(|| anyhow!("Chunk {id} not found in any pack"))?;

        let manifest = self
            .index
            .packs
            .get(&pack_id)
            .ok_or_else(|| anyhow!("Couldn't find pack {pack_id} manifest in the index"))?;

        let entry = manifest
            .iter()
            .find(|e| e.id == *id)
            .ok_or_else(|| anyhow!("Chunk {id} isn't in pack {pack_id} like the index said"))?;

        Ok(entry.length)
    }

    pub fn read_blob(&mut self, id: &ObjectId) -> Result<Rc<Vec<u8>>> {
        // If we get a cache hit, EZ!
        if let Some(b) = self.cache.get(id) {
            counters::bump(counters::Op::ChunkCacheHit);
            return Ok(b);
        }

        counters::bump(counters::Op::ChunkCacheMiss);

        // Otherwise we're gonna have to fish it out of a pack.
        let pack_id: ObjectId = **self
            .blob_map
            .get(id)
            .ok_or_else(|| anyhow!("Chunk {id} not found in any pack"))?;

        trace!("Chunk cache miss; reading pack {pack_id}");
        let loaded_size = self
            .load_pack(pack_id)
            .with_context(|| format!("Couldn't load pack {pack_id}"))?;

        // Hold onto 2x the most we loaded from one pack.
        // Yes this is hokey as hell, but alternatives seem dumber:
        //
        // 1. Making it a multiple of the target pack size means we have to plumb that in here,
        //    and we might evict some of the last few if they happen to be bigger.
        //
        // 2. Keeping track of a specific history of pack sizes - where do we stop?
        //    The last 3? Last 5?
        self.biggest_pack_size = self.biggest_pack_size.max(loaded_size);
        self.cache.shrink_to(self.biggest_pack_size * 2);

        if !self.read_packs.insert(pack_id) {
            counters::bump(counters::Op::PackRereads);
        }
        Ok(self.cache.get(id).unwrap())
    }

    fn load_pack(&mut self, id: ObjectId) -> Result<usize> {
        let mut file = self.cached_backend.read_pack(&id)?;
        pack::check_magic(&mut file)?;

        let manifest = self
            .index
            .packs
            .get(&id)
            .ok_or_else(|| anyhow!("Couldn't find pack {} manifest in the index", id))?;

        let mut blob_stream =
            ZstdDecoder::new(file).context("Decompression of blob stream failed")?;

        let mut bytes_read = 0;
        let mut blob_buf = vec![];
        for entry in manifest {
            if entry.blob_type != blob::Type::Chunk {
                warn!(
                    "Tree {} found in pack where we expected only chunks",
                    entry.id
                );
                continue;
            }

            blob_buf.clear();
            blob_buf.reserve(entry.length as usize);

            let mut hashing_decoder =
                HashingReader::new((&mut blob_stream).take(entry.length as u64));
            hashing_decoder.read_to_end(&mut blob_buf)?;
            let (hash, _) = hashing_decoder.finalize();
            ensure!(
                entry.id == hash,
                "Calculated hash of blob ({}) doesn't match ID {}",
                hash,
                entry.id
            );
            self.cache.insert(&entry.id, &blob_buf);

            bytes_read += entry.length as usize;
        }

        Ok(bytes_read)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::collections::BTreeSet;
    use std::sync::mpsc::sync_channel;

    use crate::blob;
    use crate::chunk;

    #[test]
    fn smoke() -> Result<()> {
        // Create a backend with a single pack from our reference files
        let backend = backend::in_memory();

        let mut chunks = Vec::new();

        chunks.extend(chunk::chunk_file("tests/references/sr71.txt")?);
        chunks.extend(chunk::chunk_file("tests/references/index.stability")?);
        chunks.extend(chunk::chunk_file("tests/references/pack.stability")?);
        chunks.extend(chunk::chunk_file("tests/references/README.md")?);
        assert_eq!(chunks.len(), 4);

        let (chunk_tx, chunk_rx) = sync_channel(0);
        let (pack_tx, pack_rx) = sync_channel(0);
        let (upload_tx, upload_rx) = sync_channel(0);

        let unused_byte_count = std::sync::atomic::AtomicU64::new(0);
        let chunk_packer = std::thread::spawn(move || {
            pack::pack(
                pack::DEFAULT_PACK_SIZE,
                chunk_rx,
                pack_tx,
                upload_tx,
                &unused_byte_count,
                &unused_byte_count,
            )
        });

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
        let mut reader = ChunkReader::new(&backend, &index, &blob_map);

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

    fn readback(blob: &blob::Blob, reader: &mut ChunkReader) -> Result<()> {
        let read_blob = reader
            .read_blob(&blob.id)
            .with_context(|| format!("Couldn't read {}", blob.id))?;
        assert_eq!(*read_blob, blob.bytes());
        Ok(())
    }
}

use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::*;
use fastcdc::FastCDC;
use log::*;
use rayon::prelude::*;

use crate::hashing::ObjectId;

const MEGA: u64 = 1024 * 1024;

/// A chunk of a file.
///
/// All chunks from a file share the same underlying buffer via a refcount to
/// avoid reallocating the whole file, bit by bit, as we pass its chunks to the packer.
#[derive(Clone)]
pub struct Chunk {
    file: Arc<dyn AsRef<[u8]> + Send + Sync>,
    start: usize,
    end: usize,
    pub id: ObjectId,
}

impl Chunk {
    pub fn bytes(&self) -> &[u8] {
        let bytes: &[u8] = (*self.file).as_ref();
        &bytes[self.start..self.end]
    }

    pub fn len(&self) -> usize {
        self.end - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub type ChunkedFile = Vec<Chunk>;

/// Chunks a file into content-based chunks between 512kB and 2MB, aiming for 1MB.
///
/// Duplicati makes a convincing argument that heavyweight attempts to
/// deduplicate data at the chunk level (as opposed to the file level) isn't
/// worth your time
/// (https://duplicati.readthedocs.io/en/latest/appendix-a-how-the-backup-process-works/#processing-similar-data)
/// whereas Restic uses content-based chunking
/// (https://restic.readthedocs.io/en/latest/100_references.html#backups-and-deduplication).
///
/// Duplicati makes some convincing arguments
/// (that the compression algorithm itself will help deduplicate things,
/// and that content shifts happen in small files that compress well anyways),
/// but
///
/// - We need to split large files up into chunks anyways to fit them into packs.
/// - FastCDC is 10x faster than Restic's Rabin-based chunking
///
/// Let's start with chunk sizes similar to Restic's, which with SHA224, produces
/// 28kB of hashes for each GB of data. A smaller chunk size could produce better
/// deduplication, so consider playing with this value  when playing
/// with different inputs, but changing the chunk size means changing the ID of
/// (almost) every chunk in the backup. So let's find one that works pretty well
/// ASAP.
///
/// See https://crates.io/crates/fastcdc
pub fn chunk_file(path: &Path) -> Result<ChunkedFile> {
    const MIN_SIZE: usize = 1024 * 512;
    const TARGET_SIZE: usize = 1024 * 1024;
    const MAX_SIZE: usize = 1024 * 1024 * 2;

    debug!("Chunking {}...", path.display());

    let file = read_file(path).with_context(|| format!("Couldn't read {}", path.display()))?;
    let file_bytes: &[u8] = (*file).as_ref();

    let chunk_count = AtomicUsize::new(0);

    let chunks: Vec<Chunk> = FastCDC::new(file_bytes, MIN_SIZE, TARGET_SIZE, MAX_SIZE)
        .par_bridge()
        .map(|chunk| {
            let file = file.clone();
            let start = chunk.offset;
            let end = chunk.offset + chunk.length;
            let id = ObjectId::hash(&file_bytes[start..end]);
            chunk_count.fetch_add(1, Ordering::Relaxed);
            Chunk {
                file,
                start,
                end,
                id,
            }
        })
        .collect();

    for chunk in &chunks {
        trace!(
            "{}: [{}..{}] {}",
            path.display(),
            chunk.start,
            chunk.end,
            chunk.id
        );
    }

    debug!(
        "Chunked {} into {} chunks",
        path.display(),
        chunk_count.load(Ordering::SeqCst)
    );
    Ok(chunks)
}

fn read_file(path: &Path) -> Result<Arc<dyn AsRef<[u8]> + Send + Sync>> {
    let mut fh = File::open(path)?;
    let file_length = fh.metadata()?.len();
    if file_length < 10 * MEGA {
        debug!("{} is < 10MB, reading to buffer", path.display());
        let mut buffer = Vec::new();
        fh.read_to_end(&mut buffer)?;
        Ok(Arc::new(buffer))
    } else {
        debug!("{} is > 10MB, memory mapping", path.display());
        let mapping = unsafe { memmap::Mmap::map(&fh)? };
        Ok(Arc::new(mapping))
    }
}

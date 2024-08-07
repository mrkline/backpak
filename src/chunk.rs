//! Cut files into content-based chunks.

use std::sync::Arc;

use anyhow::{Context, Result};
use camino::Utf8Path;
use fastcdc::v2020::FastCDC;
use rayon::prelude::*;
use tracing::*;

use crate::blob::{self, Blob};
use crate::file_util;
use crate::hashing::ObjectId;

/// A span of a shared byte buffer,
/// similar to [`Bytes`](https://docs.rs/bytes/latest/bytes/struct.Bytes.html),
/// but referencing a file it came from.
///
/// All chunks from a file share the same underlying buffer via a refcount to
/// avoid reallocating the whole file, bit by bit, as we pass its chunks to the packer.
///
/// It would probably be nicer to just have the Arc and a slice into it,
/// but self-referential structures in Rust are a bit of a PITA...
#[derive(Debug, Clone)]
pub struct FileSpan {
    file: Arc<file_util::LoadedFile>,
    start: usize,
    end: usize,
}

impl AsRef<[u8]> for FileSpan {
    fn as_ref(&self) -> &[u8] {
        let bytes: &[u8] = self.file.bytes();
        &bytes[self.start..self.end]
    }
}

pub type ChunkedFile = Vec<Blob>;

/// Cuts a file into content-based chunks between 512kB and 8MB, aiming for 1MB.
///
/// Duplicati makes a convincing argument that heavyweight attempts to
/// deduplicate data at the chunk level (as opposed to the file level) isn't
/// worth your time
/// (<https://duplicati.readthedocs.io/en/latest/appendix-a-how-the-backup-process-works/#processing-similar-data>)
/// whereas Restic uses content-based chunking
/// (<https://restic.readthedocs.io/en/latest/100_references.html#backups-and-deduplication>).
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
/// See <https://crates.io/crates/fastcdc>
pub fn chunk_file<P: AsRef<Utf8Path>>(path: P) -> Result<ChunkedFile> {
    const MIN_SIZE: u32 = 1024 * 512;
    const TARGET_SIZE: u32 = 1024 * 1024;
    const MAX_SIZE: u32 = 1024 * 1024 * 8;

    let path: &Utf8Path = path.as_ref();

    let file = file_util::read_file(path).with_context(|| format!("Couldn't read {path}"))?;
    let file_bytes: &[u8] = file.bytes();

    trace!("Finding cut points for {path}");
    let chunks: Vec<_> = FastCDC::new(file_bytes, MIN_SIZE, TARGET_SIZE, MAX_SIZE).collect();
    debug!("Chunking {} into {} chunks", path, chunks.len());

    let chunks: Vec<Blob> = chunks
        .par_iter()
        .map(|chunk| {
            let file = file.clone();
            let start = chunk.offset;
            let end = chunk.offset + chunk.length;
            let span = FileSpan { file, start, end };

            let id = ObjectId::hash(span.as_ref());

            trace!("{}: [{}..{}] {}", path, start, end, id);

            Blob {
                contents: blob::Contents::Span(span),
                id,
                kind: blob::Type::Chunk,
            }
        })
        .collect();

    Ok(chunks)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn smoke() -> Result<()> {
        let chunked = chunk_file("tests/references/sr71.txt")?;
        assert_eq!(chunked.len(), 1);

        let chunked = &chunked[0];
        assert_eq!(chunked.bytes().len(), 6934);
        assert_eq!(
            format!("{}", chunked.id),
            "3klf09rvhih97ev102hos4g0hq6cr2b0o74mvhthli7oq"
        );
        Ok(())
    }
}

//! Cut files into content-based chunks.

use std::sync::{mpsc, Arc};
use std::thread;

use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use fastcdc::v2020::{Chunk, FastCDC};
use ouroboros::self_referencing;
use tracing::*;

use crate::blob::{self, Blob};
use crate::file_util::{self, LoadedFile};
use crate::hashing::ObjectId;

/// Cuts a file into content-based chunks between 512kiB and 8MiB, aiming for 1MiB.
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
pub fn chunk_file<P: AsRef<Utf8Path>>(path: P) -> Result<impl Iterator<Item = Blob>> {
    let path: &Utf8Path = path.as_ref();
    let file = file_util::read_file(path).with_context(|| format!("Couldn't read {path}"))?;
    Ok(ChunkIterator::new(path.to_owned(), file))
}

fn new_cdc(src: &[u8]) -> FastCDC {
    const MIN_SIZE: u32 = 1024 * 512;
    const TARGET_SIZE: u32 = 1024 * 1024;
    const MAX_SIZE: u32 = 1024 * 1024 * 8;
    FastCDC::new(src, MIN_SIZE, TARGET_SIZE, MAX_SIZE)
}

/// For small files, use a simple iterator that just wraps the file and FastCDC iterator.
/// For larger files, cut in one thread, hash in another, and send results back through a channel.
enum ChunkIterator {
    Simple(SmallFileChunker),
    Threaded(ThreadedChunker),
}

impl ChunkIterator {
    fn new(path: Utf8PathBuf, file: Arc<LoadedFile>) -> Self {
        // "small" is decided by whether we read or memory-mapped the file in `read_file()`
        match *file {
            LoadedFile::Buffered(_) => ChunkIterator::Simple(SmallFileChunker::from(path, file)),
            LoadedFile::Mapped(_) => ChunkIterator::Threaded(ThreadedChunker::from(path, file)),
        }
    }
}

impl Iterator for ChunkIterator {
    type Item = Blob;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ChunkIterator::Simple(s) => s.next(),
            ChunkIterator::Threaded(ThreadedChunker(t)) => t.next(),
        }
    }
}

/// It's very convenient for the iterator to own the loaded file so we don't have to bump
/// that into [`chunk_file()`]'s caller.
#[self_referencing]
struct SmallFileChunker {
    /// Just for tracing the file's blob spans
    path: Utf8PathBuf,
    file: Arc<LoadedFile>,
    #[borrows(file)]
    #[not_covariant]
    chunker: FastCDC<'this>,
}

impl SmallFileChunker {
    fn from(path: Utf8PathBuf, file: Arc<LoadedFile>) -> Self {
        assert!(matches!(*file, LoadedFile::Buffered(_)));
        SmallFileChunkerBuilder {
            path,
            file,
            chunker_builder: |f: &Arc<LoadedFile>| new_cdc(f.bytes()),
        }
        .build()
    }
}

impl Iterator for SmallFileChunker {
    type Item = Blob;

    fn next(&mut self) -> Option<Self::Item> {
        self.with_mut(|s| {
            s.chunker
                .next()
                .map(|c| chunk_to_blob(s.path, s.file.clone(), c))
        })
    }
}

struct ThreadedChunker(mpsc::IntoIter<Blob>);

impl ThreadedChunker {
    fn from(path: Utf8PathBuf, file: Arc<LoadedFile>) -> Self {
        assert!(matches!(*file, LoadedFile::Mapped(_)));
        // Arbitrary-sized channels, but bust our usual "no buffering" rule -
        // the code that calls `chunk_file()` is only doing this once at a time,
        // and consuming these is the majority of our time.
        let (cuts_tx, cuts_rx) = mpsc::sync_channel(128);
        let (blobs_tx, blobs_rx) = mpsc::sync_channel(128);
        let file2 = file.clone();
        thread::spawn(move || {
            for cut in new_cdc(file.bytes()) {
                if cuts_tx.send(cut).is_err() {
                    break;
                }
            }
        });
        thread::spawn(move || {
            while let Ok(cut) = cuts_rx.recv() {
                if blobs_tx
                    .send(chunk_to_blob(&path, file2.clone(), cut))
                    .is_err()
                {
                    break;
                }
            }
        });
        Self(blobs_rx.into_iter())
    }
}

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
    file: Arc<LoadedFile>,
    start: usize,
    end: usize,
}

impl AsRef<[u8]> for FileSpan {
    fn as_ref(&self) -> &[u8] {
        let bytes: &[u8] = self.file.bytes();
        &bytes[self.start..self.end]
    }
}

fn chunk_to_blob(path: &Utf8Path, file: Arc<LoadedFile>, chunk: Chunk) -> Blob {
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
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn smoke() -> Result<()> {
        let chunked: Vec<_> = chunk_file("tests/references/sr71.txt")?.collect();
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

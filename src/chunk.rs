use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

use anyhow::Result;
use fastcdc::FastCDC;
use log::*;
use rayon::prelude::*;
use sha2::{Digest, Sha224};

use crate::hashing::Sha224Sum;

const MEGA: u64 = 1024 * 1024;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Chunk {
    pub start: usize,
    pub end: usize,
    pub hash: Sha224Sum,
}

pub struct ChunkedFile {
    pub name: PathBuf,
    pub file: Box<dyn AsRef<[u8]> + Send>,
    pub chunks: Vec<Chunk>,
}

impl ChunkedFile {
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], Sha224Sum)> {
        self.chunks.iter().map(move |c| {
            let file_bytes: &[u8] = (*self.file).as_ref();
            (&file_bytes[c.start..c.end], c.hash)
        })
    }
}

pub fn chunk_file(path: PathBuf) -> Result<ChunkedFile> {
    let file = open_file(&path)?;
    let file_bytes: &[u8] = (*file).as_ref();
    let chunks = FastCDC::new(file_bytes, 1024 * 512, 1024 * 1024, 1024 * 1024 * 2);
    let chunks: Vec<fastcdc::Chunk> = chunks.collect();

    trace!("Chunked {} into {} chunks", path.display(), chunks.len());

    let chunks: Vec<Chunk> = chunks
        .into_par_iter()
        .map(|chunk| {
            let start = chunk.offset;
            let end = chunk.offset + chunk.length;
            let hash = Sha224::digest(&file_bytes[start..end]);
            Chunk { start, end, hash }
        })
        .collect();

    for chunk in &chunks {
        trace!(
            "{}: [{}..{}] {:x}",
            path.display(),
            chunk.start,
            chunk.end,
            chunk.hash
        );
    }
    Ok(ChunkedFile {
        name: path,
        file,
        chunks,
    })
}

fn open_file(path: &Path) -> Result<Box<dyn AsRef<[u8]> + Send>> {
    let mut fh = File::open(path)?;
    let file_length = fh.metadata()?.len();
    if file_length < 10 * MEGA {
        debug!("{} is < 10MB, reading to buffer", path.display());
        let mut buffer = Vec::new();
        fh.read_to_end(&mut buffer)?;
        Ok(Box::new(buffer))
    } else {
        debug!("{} is > 10MB, memory mapping", path.display());
        let mapping = unsafe { memmap::Mmap::map(&fh)? };
        Ok(Box::new(mapping))
    }
}

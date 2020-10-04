use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use fastcdc::FastCDC;
use log::*;
use rayon::prelude::*;

use crate::hashing::ObjectId;

const MEGA: u64 = 1024 * 1024;

#[derive(Clone)]
pub struct Chunk {
    file: Arc<dyn AsRef<[u8]> + Send + Sync>,
    start: usize,
    end: usize,
    pub hash: ObjectId,
}

impl Chunk {
    pub fn bytes(&self) -> &[u8] {
        let bytes: &[u8] = (*self.file).as_ref();
        &bytes[self.start..self.end]
    }

    pub fn len(&self) -> usize {
        self.end - self.start
    }
}

pub type ChunkedFile = Vec<Chunk>;

pub fn chunk_file(path: &Path) -> Result<ChunkedFile> {
    let file = open_file(path)?;
    let file_bytes: &[u8] = (*file).as_ref();
    let chunks = FastCDC::new(file_bytes, 1024 * 512, 1024 * 1024, 1024 * 1024 * 2);
    let chunks: Vec<fastcdc::Chunk> = chunks.collect();

    trace!("Chunked {} into {} chunks", path.display(), chunks.len());

    let chunks: Vec<Chunk> = chunks
        .into_par_iter()
        .map(|chunk| {
            let file = file.clone();
            let start = chunk.offset;
            let end = chunk.offset + chunk.length;
            let hash = ObjectId::new(&file_bytes[start..end]);
            Chunk {
                file,
                start,
                end,
                hash,
            }
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
    Ok(chunks)
}

fn open_file(path: &Path) -> Result<Arc<dyn AsRef<[u8]> + Send + Sync>> {
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

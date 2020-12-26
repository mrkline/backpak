use std::io::prelude::*;
use std::path::Path;
use std::sync::Arc;

use anyhow::*;
use log::*;

/// Checks for the given magic bytes at the start of the file
pub fn check_magic<R: Read>(r: &mut R, expected: &[u8]) -> Result<()> {
    let mut magic: [u8; 8] = [0; 8];
    r.read_exact(&mut magic)?;
    ensure!(
        magic == expected,
        "Expected magic bytes {}, found {}",
        unsafe { std::str::from_utf8_unchecked(expected) },
        String::from_utf8_lossy(&magic)
    );
    Ok(())
}

/// A loaded file, either as a buffer (if it's small) or as a memory map.
#[derive(Debug)]
pub enum LoadedFile {
    Buffered(Vec<u8>),
    Mapped(memmap::Mmap),
}

impl LoadedFile {
    pub fn bytes(&self) -> &[u8] {
        match self {
            LoadedFile::Buffered(vec) => &vec,
            LoadedFile::Mapped(map) => &map,
        }
    }
}

pub fn read_file(path: &Path) -> Result<Arc<LoadedFile>> {
    const MEGA: u64 = 1024 * 1024;

    let mut fh = std::fs::File::open(path)?;
    let file_length = fh.metadata()?.len();

    let file = if file_length < 10 * MEGA {
        trace!("{} is < 10MB, reading to buffer", path.display());
        let mut buffer = Vec::new();
        fh.read_to_end(&mut buffer)?;
        LoadedFile::Buffered(buffer)
    } else {
        trace!("{} is > 10MB, memory mapping", path.display());
        let mapping = unsafe { memmap::Mmap::map(&fh)? };
        LoadedFile::Mapped(mapping)
    };

    Ok(Arc::new(file))
}

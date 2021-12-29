//! Utilities for reading files into buffers and checking magic bytes.

use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use log::*;

use crate::counters;

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
            LoadedFile::Buffered(vec) => vec,
            LoadedFile::Mapped(map) => map,
        }
    }
}

/// Reads an entire file if it's small enough, memory maps it otherwise.
pub fn read_file(path: &Path) -> Result<Arc<LoadedFile>> {
    const MEGA: u64 = 1024 * 1024;

    let mut fh = File::open(path)?;
    let file_length = fh.metadata()?.len();

    let file = if file_length < 10 * MEGA {
        trace!("{} is < 10MB, reading to buffer", path.display());
        let mut buffer = Vec::with_capacity(file_length as usize);
        fh.read_to_end(&mut buffer)?;
        counters::bump(counters::Op::FileToBuffer);
        LoadedFile::Buffered(buffer)
    } else {
        trace!("{} is > 10MB, memory mapping", path.display());
        let mapping = unsafe { memmap::Mmap::map(&fh)? };
        counters::bump(counters::Op::FileToMmap);
        LoadedFile::Mapped(mapping)
    };

    Ok(Arc::new(file))
}

#[cfg(unix)]
pub fn move_opened<P, Q>(from: P, from_fh: File, to: Q) -> Result<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let from = from.as_ref();
    let to = to.as_ref();

    // POSIX lets us rename opened files. Neat!
    match std::fs::rename(&from, &to) {
        Ok(()) => {
            debug!("Renamed {} to {}", from.display(), to.display());
            Ok(())
        },
        // Once stabilized: e.kind() == ErrorKind::CrossesDevices
        Err(e) if e.raw_os_error() == Some(18) /* EXDEV */ => {
            move_by_copy(from, from_fh, to)
        },
        Err(e) => anyhow::bail!(e),
    }
}

#[cfg(windows)]
pub fn move_opened<P, Q>(from: P, from_fh: File, to: Q) -> Result<()>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    // On Windows, we can't move an open file. Boo, Windows.
    move_by_copy(from.as_ref(), from_fh, to.as_ref())
}

fn move_by_copy(from: &Path, mut from_fh: File, to: &Path) -> Result<()> {
    from_fh.seek(std::io::SeekFrom::Start(0))?;
    safe_copy_to_file(from_fh, to)?;

    // Axe /src/foo
    std::fs::remove_file(&from).with_context(|| format!("Couldn't remove {}", from.display()))?;
    debug!("Moved {} to {}", from.display(), to.display());
    Ok(())
}

/// Copies the reader to a new file at `to + ".part"`, then renames to `to`.
///
/// This should guarantee that `to` never contains a partial file.
pub fn safe_copy_to_file<R: Read>(mut from: R, to: &Path) -> Result<()> {
    // To make things more atomic, copy to /dest/foo.part,
    // then rename to /dest/foo.
    let mut to_part = to.to_owned().into_os_string();
    to_part.push(".part");
    let to_part = Path::new(&to_part);

    // Copy the file to /dest/foo.part.
    let mut to_fh = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&to_part)
        .with_context(|| format!("Couldn't open {}", to_part.display()))?;

    std::io::copy(&mut from, &mut to_fh)
        .with_context(|| format!("Couldn't write {}", to_part.display()))?;
    drop(from);

    to_fh
        .sync_all()
        .with_context(|| format!("Couldn't sync {}", to_part.display()))?;
    drop(to_fh);

    // Rename to /dest/foo
    std::fs::rename(&to_part, to)
        .with_context(|| format!("Couldn't rename {} to {}", to_part.display(), to.display()))
}

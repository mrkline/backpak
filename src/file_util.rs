//! Utilities for reading files into buffers and checking magic bytes.

use std::fs::File;
use std::io::prelude::*;
use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use camino::Utf8Path;
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
    Mapped(memmap2::Mmap),
}

impl LoadedFile {
    pub fn bytes(&self) -> &[u8] {
        match self {
            LoadedFile::Buffered(vec) => vec,
            LoadedFile::Mapped(map) => map,
        }
    }
}

/// Read an entire file if it's small enough; memory map it otherwise.
pub fn read_file(path: &Utf8Path) -> Result<Arc<LoadedFile>> {
    const MEGA: u64 = 1024 * 1024;

    let mut fh = File::open(path)?;
    let file_length = fh.metadata()?.len();

    let file = if file_length < 10 * MEGA {
        trace!("{path} is < 10MB, reading to buffer");
        let mut buffer = Vec::with_capacity(file_length as usize);
        fh.read_to_end(&mut buffer)?;
        counters::bump(counters::Op::FileToBuffer);
        LoadedFile::Buffered(buffer)
    } else {
        trace!("{path} is > 10MB, memory mapping");
        let mapping = unsafe { memmap2::Mmap::map(&fh)? };
        counters::bump(counters::Op::FileToMmap);
        LoadedFile::Mapped(mapping)
    };

    Ok(Arc::new(file))
}

/// Move the given file `from -> to`, renaming if possible.
///
/// If a rename isn't possible, write out a copy.
/// Uses `from_fh` to write the copy as-needed.
///
/// Returns a file handle (assume at EOF) for `to`.
#[cfg(unix)]
pub fn move_opened<P, Q>(from: P, from_fh: File, to: Q) -> Result<File>
where
    P: AsRef<Utf8Path>,
    Q: AsRef<Utf8Path>,
{
    let from = from.as_ref();
    let to = to.as_ref();

    // POSIX lets us rename opened files. Neat!
    match std::fs::rename(from, to) {
        Ok(()) => {
            debug!("Renamed {from} to {to}");
            Ok(from_fh)
        },
        // Once stabilized: e.kind() == ErrorKind::CrossesDevices
        Err(e) if e.raw_os_error() == Some(18) /* EXDEV */ => {
            move_by_copy(from, from_fh, to)
        },
        Err(e) => anyhow::bail!(e),
    }
}

/// Move the given file `from -> to` via copy (boo, Windows).
#[cfg(windows)]
pub fn move_opened<P, Q>(from: P, from_fh: File, to: Q) -> Result<File>
where
    P: AsRef<Utf8Path>,
    Q: AsRef<Utf8Path>,
{
    // On Windows, we can't move an open file. Boo, Windows.
    move_by_copy(from.as_ref(), from_fh, to.as_ref())
}

fn move_by_copy(from: &Utf8Path, mut from_fh: File, to: &Utf8Path) -> Result<File> {
    from_fh.seek(std::io::SeekFrom::Start(0))?;
    let to_fh = safe_copy_to_file(from_fh, to)?;

    // Axe /src/foo
    std::fs::remove_file(from).with_context(|| format!("Couldn't remove {from}"))?;
    debug!("Moved {from} to {to}");
    Ok(to_fh)
}

/// Copies the reader to a new file at `to + ".part"`, then renames to `to`.
///
/// This should guarantee that `to` never contains a partial file.
/// Returns an open file handle for `to` (assume at EOF)
pub fn safe_copy_to_file<R: Read>(mut from: R, to: &Utf8Path) -> Result<File> {
    // To make things more atomic, copy to /dest/foo.part,
    // then rename to /dest/foo.
    let mut to_part: String = to.as_str().to_owned();
    to_part.push_str(".part");
    let to_part = Utf8Path::new(&to_part);

    // Copy the file to /dest/foo.part.
    let mut to_fh = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(to_part)
        .with_context(|| format!("Couldn't open {to_part}"))?;

    std::io::copy(&mut from, &mut to_fh).with_context(|| format!("Couldn't write {to_part}"))?;
    drop(from);

    to_fh
        .sync_all()
        .with_context(|| format!("Couldn't sync {to_part}"))?;

    if cfg!(unix) {
        // Rename to /dest/foo and return our handle
        std::fs::rename(to_part, to)
            .with_context(|| format!("Couldn't rename {to_part} to {to}"))?;

        Ok(to_fh)
    } else {
        // Windows is sad, we have to close to rename.
        // Is this a soundness/atomicity hole in the making?
        // Maybe; help me Windows friends.
        drop(to_fh);
        std::fs::rename(to_part, to)
            .with_context(|| format!("Couldn't rename {to_part} to {to}"))?;

        File::open(to).with_context(|| format!("Couldn't open {to} after moving to it"))
    }
}

/// File size but nice.
pub fn nice_size<S: Into<u128>>(s: S) -> String {
    use byte_unit::*;

    let b = Byte::from_bytes(s.into());
    let adj = b.get_appropriate_unit(true); // power of 2 units
    adj.format(2)
}

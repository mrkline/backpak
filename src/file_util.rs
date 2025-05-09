//! Utilities for reading files into buffers and checking magic bytes.

use std::fs::File;
use std::io::prelude::*;
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use camino::Utf8Path;
use tracing::*;

use crate::counters;

/// Checks for the given magic bytes at the start of the file
pub fn check_magic<R: Read>(r: &mut R, expected: &[u8]) -> Result<()> {
    let mut magic: Vec<u8> = expected.to_owned();
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
        let mut buffer = Vec::with_capacity(file_length as usize);
        fh.read_to_end(&mut buffer)?;
        counters::bump(counters::Op::FileToBuffer);
        LoadedFile::Buffered(buffer)
    } else {
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
            trace!("Renamed {from} to {to}");
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
    trace!("Moved {from} to {to}");
    Ok(to_fh)
}

/// Copies the reader to a new file at `to + ".part"`, then renames to `to`.
///
/// This should guarantee that `to` never contains a partial file.
/// Returns an open file handle for `to` (assume at EOF)
pub fn safe_copy_to_file<R: Read>(mut from: R, to: &Utf8Path) -> Result<File> {
    // To make things more atomic, copy to /dest/foo.<rando>.part,
    // then rename to /dest/foo.
    let dir = to.parent().unwrap();
    let pre = to.file_name().unwrap().to_owned() + ".";
    let mut to_fh = tempfile::Builder::new()
        .prefix(&pre)
        .suffix(".part")
        .tempfile_in(dir)
        .with_context(|| format!("Couldn't open temporary {to}.part"))?;

    let temp_path = format!("{}", to_fh.path().display());

    std::io::copy(&mut from, &mut to_fh)
        .with_context(|| format!("Couldn't write to {temp_path}"))?;
    drop(from);

    let persisted = to_fh
        .persist(to)
        .with_context(|| format!("Couldn't persist {temp_path} to {to}"))?;
    persisted
        .sync_all()
        .with_context(|| format!("Couldn't sync {to}"))?;

    Ok(persisted)
}

/// File size but nice.
pub fn nice_size(s: u64) -> String {
    use byte_unit::Unit::*;
    use byte_unit::*;

    let b = Byte::from_u64(s);
    let a = b.get_appropriate_unit(UnitType::Decimal); // Human units please.
    match a.get_unit() {
        // Don't split hairs, or KB.
        Bit | B | Kbit | Kibit | KB | KiB => format!("{a:.0}"),
        _ => format!("{a:.2}"),
    }
}

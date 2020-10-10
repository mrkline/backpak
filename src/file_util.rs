use std::fs::File;
use std::io::prelude::*;

use anyhow::*;

/// Checks for the given magic bytes at the start of the file
pub fn check_magic(fh: &mut File, expected: &[u8]) -> Result<()> {
    let mut magic: [u8; 8] = [0; 8];
    fh.read_exact(&mut magic)?;
    ensure!(
        magic == expected,
        "Expected magic bytes {}, found {}",
        unsafe { std::str::from_utf8_unchecked(expected) },
        String::from_utf8_lossy(&magic)
    );
    Ok(())
}

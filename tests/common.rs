#![allow(dead_code)]

use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use anyhow::*;
use assert_cmd::Command;
use walkdir::WalkDir;

pub fn cli_run(backup_path: &Path) -> Result<assert_cmd::Command> {
    let bin_name = env!("CARGO_PKG_NAME");
    let mut cmd = Command::cargo_bin(bin_name)?;
    cmd.arg("--repository").arg(backup_path);
    cmd.arg("-vvv");
    Ok(cmd)
}

pub fn count_directory_entries<P: AsRef<Path>>(dir: P) -> usize {
    #[allow(clippy::suspicious_map)]
    std::fs::read_dir(dir)
        .expect("Couldn't read dir")
        .map(|de| {
            let de = de.expect("Couldn't read dir entry");
            eprintln!("{}", de.path().display());
        })
        .count()
}

pub fn files_in(p: &Path) -> impl Iterator<Item = PathBuf> {
    WalkDir::new(p)
        .into_iter()
        .map(|e| e.expect("couldn't walk dir"))
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
}

// If we don't already have a big file at "tests/references",
// put one there.
pub fn setup_bigfile() {
    // Guess this makes tests Unix-only for now.
    // Add a Windows analog? Does it have sparse files?
    use std::os::unix::io::AsRawFd;

    let big_path = Path::new("tests/references/bigfile");
    if big_path.is_file() {
        return;
    }
    // We don't want to make it _too_ lest tests take too long,
    // but we want a good # of chunks out of it.
    const BIG_SIZE: i64 = 1024 * 1024 * 1024; // 1GB

    let mut fh = std::fs::File::create(&big_path).expect("Couldn't create bigfile");
    // Truncate the file so that modern filesystems can punch a hole
    // instead of having a gigabyte of zeroes.
    nix::unistd::ftruncate(fh.as_raw_fd(), BIG_SIZE).expect("ftruncate() failed");

    // Stripe it with some nonsense so it's not _just_ zeroes.
    // Write some bytes every 10MB or so.
    let mut i = 1u8;
    loop {
        fh.write_all(&std::iter::repeat(i).take(1024).collect::<Vec<u8>>())
            .expect("Writing bigfile failed");
        i = i.wrapping_add(1);
        match fh.seek(SeekFrom::Current(1024 * 1024 * 10)) {
            Ok(len) if len > BIG_SIZE as u64 => break,
            res => res,
        }
        .expect("Seek failed while writing bigfile");
    }
}

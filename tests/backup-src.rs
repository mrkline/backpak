use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::Path;

use anyhow::*;
use assert_cmd::Command;
use tempfile::tempdir;

// If we don't already have a big file at "tests/references",
// put one there.
fn setup_bigfile() {
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

fn cli_run(backup_path: &Path) -> Result<assert_cmd::Command> {
    let bin_name = env!("CARGO_PKG_NAME");
    let mut cmd = Command::cargo_bin(bin_name)?;
    cmd.arg("--repository").arg(backup_path);
    cmd.arg("-vvv");
    Ok(cmd)
}

#[test]
fn backup_src() -> Result<()> {
    setup_bigfile();

    let backup_dir = tempdir().expect("Failed to create temp test directory");
    let backup_path = backup_dir.path();

    cli_run(backup_path)?.arg("init").assert().success();

    cli_run(backup_path)?
        .args(&["backup", "--tag", "test-tag", "--tag", "another-tag"])
        .args(&["--", "src", "tests/references"])
        .assert()
        .success();

    cli_run(backup_path)?.arg("check").assert().success();

    // To examine results
    // std::mem::forget(backup_dir);

    backup_dir.close().expect("Couldn't delete test directory");
    Ok(())
}

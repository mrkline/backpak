use std::{ffi::OsStr, fs, os::unix::ffi::OsStrExt};

use anyhow::{ensure, Result};
use tempfile::tempdir;

mod common;

use common::*;

#[test]
fn bad_filename() -> Result<()> {
    let backup_dir = tempdir()?;
    let backup_path = backup_dir.path();

    let working_dir = tempdir()?;
    let working_path = working_dir.path();

    cli_run(working_path, backup_path)?
        .args(["init", "filesystem"])
        .assert()
        .success();

    fs::create_dir(working_path.join("foo"))?;

    fs::File::create(working_path.join("foo/bar"))?;

    // Create a non-UTF-8 path, borrowed from OsStr's docs.
    fs::File::create(
        working_path
            .join("foo")
            .join(OsStr::from_bytes(&[0x66, 0x6f, 0x80, 0x6f])),
    )?;

    // Let's backup our own code, and the test references.
    let fails_on_utf8 = cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(working_path.join("foo"))
        .assert()
        .failure();

    println!("{}", stderr(&fails_on_utf8));
    println!("{:?}", files_in(&working_path).collect::<Vec<_>>());

    // We should fail fast - _before_ we start the backup process and spit out
    // any pack files...
    ensure!(
        !files_in(&working_path).any(|p| p.ends_with(".pack"))
            && files_in(&working_path).count() == 2, //  foo/bar and foo/<junk>
        "Files weren't validated before backup, .pack created"
    );
    // ...or any backup files at all.
    let backup_files: Vec<_> = files_in(&backup_path).collect();
    ensure!(
        backup_files.len() == 1 && backup_files[0].ends_with("config.toml"),
        "Files weren't validated before backup, "
    );

    // To examine results
    // std::mem::forget(working_dir);
    // std::mem::forget(backup_dir);
    Ok(())
}

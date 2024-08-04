use std::{
    fs,
    os::unix::{self, fs::PermissionsExt},
    process::Command,
};

use anyhow::Result;
use tempfile::tempdir;

mod common;

use common::*;

#[test]
fn diff_src() -> Result<()> {
    let backup_dir = tempdir()?;
    let backup_path = backup_dir.path();

    let working_dir = tempdir()?;
    let working_path = working_dir.path();

    // Let's make a copy of src so we don't fudge the actual code
    assert!(Command::new("cp")
        .args(&["-a", "src"])
        .arg(working_path)
        .status()?
        .success());

    cli_run(working_path, backup_path)?
        .args(["init", "filesystem"])
        .assert()
        .success();

    // And back it up
    cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(working_path.join("src"))
        .assert()
        .success();

    let diffit = || {
        let diff_run = cli_run(working_path, backup_path)
            .unwrap()
            .args(&["diff", "--metadata", "LAST"])
            .assert()
            .success();
        let diff_output: Vec<_> = stdout(&diff_run)
            .trim()
            .lines()
            // I don't care about atime. They change if you sneeze (and based on mount opts).
            .filter(|l| !l.starts_with("A "))
            .map(str::to_owned)
            .collect();
        diff_output
    };

    let compare = |expected: &[&str]| {
        assert_eq!(expected, diffit());
    };

    compare(&[]);

    // Obvious stuff - added, removed, moved, modified, perms...
    fs::remove_file(working_path.join("src/diff.rs"))?;
    fs::File::create(working_path.join("src/aNewFile"))?;
    fs::rename(
        working_path.join("src/backend"),
        working_path.join("src/wackend"),
    )?;
    fs::write(working_path.join("src/lib.rs"), "I want some butts!")?;
    fs::set_permissions(
        working_path.join("src/main.rs"),
        fs::Permissions::from_mode(0o777),
    )?;

    compare(&[
        "+ src/aNewFile",
        "- src/backend/",
        "- src/backend/backblaze.rs",
        "- src/backend/cache.rs",
        "- src/backend/filter.rs",
        "- src/backend/fs.rs",
        "- src/backend/memory.rs",
        "- src/diff.rs",
        "C src/lib.rs",
        "P src/main.rs",
        "+ src/wackend/",
        "+ src/wackend/backblaze.rs",
        "+ src/wackend/cache.rs",
        "+ src/wackend/filter.rs",
        "+ src/wackend/fs.rs",
        "+ src/wackend/memory.rs",
        "T src/",
    ]);

    // Wipe the slate.
    cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(working_path.join("src"))
        .assert()
        .success();

    compare(&[]);

    // Changed type!
    fs::remove_file(working_path.join("src/ls.rs"))?;
    unix::fs::symlink("/dev/null", working_path.join("src/ls.rs"))?;

    compare(&["- src/ls.rs", "+ src/ls.rs -> /dev/null", "T src/"]);

    // Wipe the slate.
    cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(working_path.join("src"))
        .assert()
        .success();

    // Symlink modified (should be -/+, not M)
    fs::remove_file(working_path.join("src/ls.rs"))?;
    unix::fs::symlink("/dev/urandom", working_path.join("src/ls.rs"))?;

    compare(&[
        "- src/ls.rs -> /dev/null",
        "+ src/ls.rs -> /dev/urandom",
        "T src/",
    ]);

    Ok(())
}

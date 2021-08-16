use std::{
    fs,
    os::unix::{self, fs::PermissionsExt},
    process::Command,
};

use anyhow::*;
use tempfile::tempdir;

mod common;

use common::*;

#[test]
fn backup_src() -> Result<()> {
    setup_bigfile();

    let backup_dir = tempdir()?;
    let backup_path = backup_dir.path();

    let working_dir = tempdir()?;
    let working_path = working_dir.path();

    cli_run(working_path, backup_path)?
        .arg("init")
        .assert()
        .success();

    // Let's make a copy of src so we don't fudge the actual code
    assert!(Command::new("cp")
        .args(&["-r", "src"])
        .arg(working_path)
        .status()?
        .success());

    // And back it up
    cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(working_path.join("src"))
        .assert()
        .success();

    let diffit = || {
        let diff_run = cli_run(working_path, backup_path)
            .unwrap()
            .args(&["diff", "--metadata", "last"])
            .assert()
            .success();
        let diff_output = stdout(&diff_run).trim();
        diff_output.to_string()
    };

    assert_eq!(diffit(), "");

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

    let compare = |expected: &[&str]| {
        assert_eq!(expected, diffit().split('\n').collect::<Vec<_>>());
    };

    compare(&[
        "+ src/aNewFile",
        "- src/backend/",
        "- src/backend/fs.rs",
        "- src/backend/memory.rs",
        "- src/diff.rs",
        "M src/lib.rs",
        "U src/main.rs",
        "+ src/wackend/",
        "+ src/wackend/fs.rs",
        "+ src/wackend/memory.rs",
        "U src/",
    ]);

    // Wipe the slate.
    cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(working_path.join("src"))
        .assert()
        .success();

    assert_eq!(diffit(), "");

    // Changed type!
    fs::remove_file(working_path.join("src/ls.rs"))?;
    unix::fs::symlink("/dev/null", working_path.join("src/ls.rs"))?;

    compare(&["- src/ls.rs", "+ src/ls.rs -> /dev/null", "U src/"]);

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
        "U src/",
    ]);

    Ok(())
}

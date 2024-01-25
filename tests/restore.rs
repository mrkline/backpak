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
fn backup_src() -> Result<()> {
    let backup_dir = tempdir()?;
    let backup_path = backup_dir.path();

    let working_dir = tempdir()?;
    let working_path = working_dir.path();

    cli_run(working_path, backup_path)?
        .args(["init", "filesystem"])
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

    let restoreit = || {
        let restore_run = cli_run(working_path, backup_path)
            .unwrap()
            .args(&["restore", "--delete", "--times", "--permissions", "LAST"])
            .assert()
            .success();
        let restore_err = stderr(&restore_run).trim();
        eprintln!("{}", restore_err);
        let restore_output = stdout(&restore_run).trim();
        restore_output.to_string()
    };

    let prefix_working_path = |s: &str| -> String {
        let (code, path) = s.split_at(2);
        let prefixed = working_path.join(path);
        format!("{}{}", code, prefixed.display())
    };

    // Without any changes, restore should be a no-op
    assert_eq!(restoreit(), "");

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
        let expected = expected
            .iter()
            .map(|p| prefix_working_path(p))
            .collect::<Vec<_>>();
        let got = restoreit()
            .split('\n')
            .filter(|l| !l.is_empty())
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        println!("Expected:\n{:#?}", expected);
        println!("Got:\n{:#?}", got);
        assert_eq!(expected, got);
    };

    compare(&[
        "- src/aNewFile",
        "+ src/backend/",
        "+ src/backend/backblaze.rs",
        "+ src/backend/cache.rs",
        "+ src/backend/filter.rs",
        "+ src/backend/fs.rs",
        "+ src/backend/memory.rs",
        "+ src/diff.rs",
        "M src/lib.rs",
        "U src/main.rs",
        "- src/wackend/",
        "- src/wackend/backblaze.rs",
        "- src/wackend/cache.rs",
        "- src/wackend/filter.rs",
        "- src/wackend/fs.rs",
        "- src/wackend/memory.rs",
        "U src/",
    ]);

    // Restoring again should do nothing
    compare(&[]);

    // Changed type!
    fs::remove_file(working_path.join("src/ls.rs"))?;
    unix::fs::symlink("/dev/null", working_path.join("src/ls.rs"))?;

    compare(&["- src/ls.rs -> /dev/null", "+ src/ls.rs", "U src/"]);

    // Symlink modified (should be -/+, not M)
    fs::remove_file(working_path.join("src/ls.rs"))?;
    unix::fs::symlink("/dev/null", working_path.join("src/ls.rs"))?;
    cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(working_path.join("src"))
        .assert()
        .success();

    fs::remove_file(working_path.join("src/ls.rs"))?;
    unix::fs::symlink("/dev/urandom", working_path.join("src/ls.rs"))?;

    compare(&[
        "- src/ls.rs -> /dev/urandom",
        "+ src/ls.rs -> /dev/null",
        "U src/",
    ]);

    Ok(())
}

// TODO: Restore multiple paths

// TODO: Restore to an --output directory

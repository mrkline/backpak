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
fn restore_src() -> Result<()> {
    let backup_dir = tempdir()?;
    let backup_path = backup_dir.path();

    let working_dir = tempdir()?;
    let working_path = working_dir.path();

    cli_run(working_path, backup_path)?
        .args(["init", "filesystem"])
        .assert()
        .success();

    // Let's make a copy of src so we don't fudge the actual code
    assert!(
        Command::new("cp")
            .args(&["-r", "src"])
            .arg(working_path)
            .status()?
            .success()
    );

    // And back it up
    cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(working_path.join("src"))
        .assert()
        .success();

    let restoreit = |args: &[&str]| {
        let restore_run = cli_run(working_path, backup_path)
            .unwrap()
            .args(&["restore", "--delete", "--times", "--permissions", "LAST"])
            .args(args)
            .assert()
            .success();
        let restore_err = stderr(&restore_run).trim();
        eprintln!("{}", restore_err);
        let restore_output: Vec<_> = stdout(&restore_run)
            .trim()
            .lines()
            // Strip Opening... Building a master index... snapshot <hash>...
            .skip(3)
            // I don't care about atime. They change if you sneeze (and based on mount opts).
            .filter(|l| !l.starts_with("A "))
            .map(str::to_owned)
            .collect();
        restore_output
    };

    let prefix_working_path = |s: &str| -> String {
        let (code, path) = s.split_at(2);
        let prefixed = working_path.join(path);
        format!("{}{}", code, prefixed.display())
    };

    let compare = |expected: &[&str], args: &[&str]| {
        let expected = expected
            .iter()
            .map(|p| prefix_working_path(p))
            .collect::<Vec<_>>();
        let got = restoreit(args);
        println!("Expected:\n{:#?}", expected);
        println!("Got:\n{:#?}", got);
        assert_eq!(expected, got);
    };

    // Without any changes, restore should be a no-op
    compare(&[], &[]);

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

    compare(
        &[
            "- src/aNewFile",
            "+ src/backend/",
            "+ src/backend/backblaze.rs",
            "+ src/backend/cache.rs",
            "+ src/backend/filter.rs",
            "+ src/backend/fs.rs",
            "+ src/backend/memory.rs",
            "+ src/backend/semaphored.rs",
            "+ src/diff.rs",
            "C src/lib.rs",
            "P src/main.rs",
            "- src/wackend/",
            "- src/wackend/backblaze.rs",
            "- src/wackend/cache.rs",
            "- src/wackend/filter.rs",
            "- src/wackend/fs.rs",
            "- src/wackend/memory.rs",
            "- src/wackend/semaphored.rs",
            "T src/",
        ],
        &[],
    );

    // Restoring again should do nothing
    compare(&[], &[]);

    // Changed type!
    fs::remove_file(working_path.join("src/ls.rs"))?;
    unix::fs::symlink("/dev/null", working_path.join("src/ls.rs"))?;

    compare(&["- src/ls.rs -> /dev/null", "+ src/ls.rs", "T src/"], &[]);

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

    compare(
        &[
            "- src/ls.rs -> /dev/urandom",
            "+ src/ls.rs -> /dev/null",
            "T src/",
        ],
        &[],
    );

    // Move our directory and retarget the restore with --output
    let moved_to_path = &working_path.join("elsewhere");
    let moved_to = moved_to_path.to_str().unwrap();

    fs::rename(working_path.join("src"), moved_to)?;

    // Everything should compare cleanly,
    // except the symlink whose time got bumped by the restore above.
    compare(&["T elsewhere/ls.rs -> /dev/null"], &["--output", moved_to]);

    // Axe the backend folder and bring it back at the new target.
    fs::remove_dir_all(moved_to_path.join("backend"))?;

    compare(
        &[
            "+ elsewhere/backend/",
            "+ elsewhere/backend/backblaze.rs",
            "+ elsewhere/backend/cache.rs",
            "+ elsewhere/backend/filter.rs",
            "+ elsewhere/backend/fs.rs",
            "+ elsewhere/backend/memory.rs",
            "+ elsewhere/backend/semaphored.rs",
            "T elsewhere/",
        ],
        &["-o", moved_to],
    );

    // We should be clean now
    compare(&[], &["-o", moved_to]);

    Ok(())
}

#[test]
fn restore_multipath() -> Result<()> {
    let backup_dir = tempdir()?;
    let backup_path = backup_dir.path();

    let working_dir = tempdir()?;
    let working_path = working_dir.path();

    cli_run(working_path, backup_path)?
        .args(["init", "filesystem"])
        .assert()
        .success();

    // Back up multiple things! And try files as the top-level objects to boot!
    assert!(
        Command::new("cp")
            .args(&["README.md", "LICENSE.txt"])
            .arg(working_path)
            .status()?
            .success()
    );

    cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(working_path.join("README.md"))
        .arg(working_path.join("LICENSE.txt"))
        .assert()
        .success();

    // Copy-pasting this makes me sad but I'm too lazy to write versions that take
    // the working and backup paths and return some partially-applied Fn.
    // (Haskell is ruining me.)
    let restoreit = |args: &[&str]| {
        let restore_run = cli_run(working_path, backup_path)
            .unwrap()
            .args(&["restore", "--delete", "--times", "--permissions", "LAST"])
            .args(args)
            .assert()
            .success();
        let restore_err = stderr(&restore_run).trim();
        eprintln!("{}", restore_err);
        let restore_output: Vec<_> = stdout(&restore_run)
            .trim()
            .lines()
            // Strip Opening... Building a master index... snapshot <hash>...
            .skip(3)
            // I don't care about atime. They change if you sneeze (and based on mount opts).
            .filter(|l| !l.starts_with("A "))
            .map(str::to_owned)
            .collect();
        restore_output
    };

    let prefix_working_path = |s: &str| -> String {
        let (code, path) = s.split_at(2);
        let prefixed = working_path.join(path);
        format!("{}{}", code, prefixed.display())
    };

    let compare = |expected: &[&str], args: &[&str]| {
        let expected = expected
            .iter()
            .map(|p| prefix_working_path(p))
            .collect::<Vec<_>>();
        let got = restoreit(args);
        println!("Expected:\n{:#?}", expected);
        println!("Got:\n{:#?}", got);
        assert_eq!(expected, got);
    };

    compare(&[], &[]);

    // Multi-path restore works
    fs::write(working_path.join("LICENSE.txt"), "https://xkcd.com/225/")?;
    fs::write(
        working_path.join("README.md"),
        "We spend eternity looking at pleasant moments—like today at the zoo. \
        Isn't this a nice moment?",
    )?;
    compare(&["C LICENSE.txt", "C README.md"], &[]);

    // Multi-path retargeting:
    let out_path = &working_path.join("elsewhere");
    let out = out_path.to_str().unwrap();

    fs::create_dir(out_path)?;
    compare(
        &["+ elsewhere/LICENSE.txt", "+ elsewhere/README.md"],
        &["--output", out],
    );

    Ok(())
}

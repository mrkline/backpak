use std::fs;

use anyhow::Result;
use tempfile::tempdir;

mod common;

use common::*;
use predicates::prelude::*;

#[test]
fn backup_src() -> Result<()> {
    setup_bigfile();

    let project_dir = std::env::current_dir()?;

    let backup_dir = tempdir()?;
    let backup_path = backup_dir.path();

    let working_dir = tempdir()?;
    let working_path = working_dir.path();

    cli_run(working_path, backup_path)?
        .arg("init")
        .assert()
        .success();

    // We don't currently allow backups of directories with matching names.
    // (It would complicated the hell out of path mapping for questionable gain.)
    fs::create_dir(working_path.join("src"))?;
    cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(project_dir.join("src"))
        .arg(working_path.join("src"))
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "Backups of directories with matching names (src/) isn't currently supported",
        ));
    fs::remove_dir(working_path.join("src"))?;

    // Let's backup our own code, and the test references.
    cli_run(working_path, backup_path)?
        .args(&["backup", "--tag", "test-tag", "--tag", "another-tag", "--"])
        .args(&[
            project_dir.join("src"),
            project_dir.join("tests/references"),
        ])
        .assert()
        .success();

    // Check that everything backed up alright.
    cli_run(working_path, backup_path)?
        .args(&["check", "--read-packs"])
        .assert()
        .success();

    // One backup = one index
    let indexes_dir = backup_path.join("indexes");
    assert_eq!(count_directory_entries(&indexes_dir), 1);

    // Make a second index with another backup.
    // Use a different set to generate a different index!
    cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(project_dir.join("src"))
        .assert()
        .success();

    assert_eq!(count_directory_entries(&indexes_dir), 2);

    // Consolodate indexes
    cli_run(working_path, backup_path)?
        .arg("rebuild-index")
        .assert()
        .success();

    assert_eq!(count_directory_entries(&indexes_dir), 1);

    // Everything should be nice and reachable from the new index.
    cli_run(working_path, backup_path)?
        .args(&["check", "--read-packs"])
        .assert()
        .success();

    // To examine results
    // std::mem::forget(backup_dir);
    Ok(())
}

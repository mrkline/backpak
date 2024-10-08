use std::collections::HashSet;

use anyhow::Result;
use predicates::prelude::*;
use predicates::str::contains;
use tempfile::tempdir;

mod common;

use common::*;

#[test]
fn backup_src() -> Result<()> {
    let project_dir = std::env::current_dir()?;

    let backup_dir = tempdir()?;
    let backup_path = backup_dir.path();

    let working_dir = tempdir()?;
    let working_path = working_dir.path();

    cli_run(working_path, backup_path)?
        .args(["init", "filesystem"])
        .assert()
        .success();

    // Let's back up these the source code.
    cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(project_dir.join("src"))
        .assert()
        .success();

    // Grab the first snapshot ID.
    let snapshots = files_in(backup_path.join("snapshots")).collect::<Vec<_>>();
    assert_eq!(snapshots.len(), 1);
    let first_snapshot = snapshots[0].file_stem().unwrap().to_str().unwrap();

    // And again, but just the UI code.
    // This will share blobs with the previous backup.
    cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(project_dir.join("src/ui"))
        .assert()
        .success();

    assert_eq!(2, files_in(backup_path.join("snapshots")).count());
    assert_eq!(3, files_in(backup_path.join("packs")).count());

    // Prune shouldn't do anything until we forget some snapshots.
    cli_run(working_path, backup_path)?
        .arg("prune")
        .assert()
        .success()
        .stdout(contains("Nothing to do."));

    // Axe the first backup. This will create a situation where the pack(s)
    // can be pruned - we still need the chunks for `tests/references`
    // but not `tests/*.rs`.
    cli_run(working_path, backup_path)?
        .args(&["forget", first_snapshot])
        .assert()
        .success();

    assert_eq!(1, files_in(backup_path.join("snapshots")).count());
    let before_packs = files_in(backup_path.join("packs")).collect::<HashSet<_>>();
    assert_eq!(3, before_packs.len());

    // Dry run shouldn't do anything!
    cli_run(working_path, backup_path)?
        .args(&["prune", "-n"])
        .assert()
        .success()
        .stdout(
            contains("Keep 1 packs")
                .and(contains("rewrite 2"))
                .and(contains("drop 0 (0 B), and replace the 2 current indexes")),
        );

    // They're the same!
    let dry_run_packs = files_in(backup_path.join("packs")).collect::<HashSet<_>>();
    assert_eq!(before_packs, dry_run_packs);

    // Paranoia.
    cli_run(working_path, backup_path)?
        .arg("check")
        .assert()
        .success();

    cli_run(working_path, backup_path)?
        .arg("prune")
        .assert()
        .success()
        .stdout(
            contains("Keep 1 packs")
                .and(contains("rewrite 2"))
                .and(contains("drop 0 (0 B), and replace the 2 current indexes")),
        );

    // They're different!
    let after_packs = files_in(backup_path.join("packs")).collect::<HashSet<_>>();
    assert_ne!(before_packs, after_packs);

    cli_run(working_path, backup_path)?
        .args(&["check", "--read-packs"])
        .assert()
        .success();

    cli_run(working_path, backup_path)?
        .arg("prune")
        .assert()
        .success()
        .stdout(predicates::str::contains("Nothing to do."));

    // To examine results
    // std::mem::forget(backup_dir);
    Ok(())
}

#[test]
fn no_repacks_needed() -> Result<()> {
    let project_dir = std::env::current_dir()?;

    let backup_dir = tempdir()?;
    let backup_path = backup_dir.path();

    let working_dir = tempdir()?;
    let working_path = working_dir.path();

    cli_run(working_path, backup_path)?
        .args(["init", "filesystem"])
        .assert()
        .success();

    // Let's back up the source code
    cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(project_dir.join("src"))
        .assert()
        .success();

    // Grab the first snapshot ID.
    let snapshots = files_in(backup_path.join("snapshots")).collect::<Vec<_>>();
    assert_eq!(snapshots.len(), 1);
    let first_snapshot = snapshots[0].file_stem().unwrap().to_str().unwrap();

    // Let's back up the reference files.
    // Should be totally different packs, nothing reused.
    cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(project_dir.join("tests/references/sr71.txt"))
        .assert()
        .success();

    // Axe the first backup. This will create a situation where the pack(s)
    // can be pruned AND we don't need to repack anything - just make a new index.
    cli_run(working_path, backup_path)?
        .args(&["forget", first_snapshot])
        .assert()
        .success();

    cli_run(working_path, backup_path)?
        .arg("prune")
        .assert()
        .success()
        .stdout(
            contains("Keep 2 packs")
                .and(contains("rewrite 0 (0 B), drop 2"))
                .and(contains("and replace the 2 current indexes")),
        );

    // We were previously blowing up here because I forgot to write
    // a new index if nothing was repacked.
    // So, we deleted packs but kept them in the index. Oops.
    cli_run(working_path, backup_path)?
        .arg("check")
        .assert()
        .success();

    Ok(())
}

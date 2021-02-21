use std::collections::HashSet;

use anyhow::*;
use tempfile::tempdir;

mod common;

use common::*;

#[test]
fn backup_src() -> Result<()> {
    setup_bigfile();

    let backup_dir = tempdir().expect("Failed to create temp test directory");
    let backup_path = backup_dir.path();

    cli_run(backup_path)?.arg("init").assert().success();

    // Let's back up these tests files and the references.
    cli_run(backup_path)?
        .args(&["backup", "tests"])
        .assert()
        .success();

    // Grab the first snapshot ID.
    let snapshots = files_in(&backup_path.join("snapshots")).collect::<Vec<_>>();
    assert_eq!(snapshots.len(), 1);
    let first_snapshot = snapshots[0].file_stem().unwrap().to_str().unwrap();

    // And again, but just the references.
    // This will share blobs with the previous backup.
    cli_run(backup_path)?
        .args(&["backup", "tests/references"])
        .assert()
        .success();

    assert_eq!(2, files_in(&backup_path.join("snapshots")).count());
    assert_eq!(3, files_in(&backup_path.join("packs")).count());

    // Axe the first backup. This will create a situation where the pack(s)
    // can be pruned - we still need the chunks for `tests/references`
    // but not `tests/*.rs`.
    cli_run(backup_path)?
        .args(&["forget", first_snapshot])
        .assert()
        .success();

    assert_eq!(1, files_in(&backup_path.join("snapshots")).count());
    let before_packs = files_in(&backup_path.join("packs")).collect::<HashSet<_>>();
    assert_eq!(3, before_packs.len());

    let prune_run = cli_run(backup_path)?.arg("prune").assert().success();
    let prune_output = std::str::from_utf8(&prune_run.get_output().stderr).unwrap();
    // Expecting
    // [ INFO] Keep 1 packs, rewrite 2, and replace the 2 current indexes
    assert!(prune_output.contains("Keep 1 packs, rewrite 2, and replace the 2 current indexes"));

    // They're different!
    let after_packs = files_in(&backup_path.join("packs")).collect::<HashSet<_>>();
    assert_eq!(3, files_in(&backup_path.join("packs")).count());
    assert_ne!(before_packs, after_packs);

    // Paranoia.
    cli_run(backup_path)?
        .args(&["check", "--read-packs"])
        .assert()
        .success();

    let prune_run2 = cli_run(backup_path)?.arg("prune").assert().success();
    let prune_output2 = std::str::from_utf8(&prune_run2.get_output().stderr).unwrap();
    assert!(prune_output2.contains("No unused blobs in any packs! Nothing to do."));

    // To examine results
    // std::mem::forget(backup_dir);

    backup_dir.close().expect("Couldn't delete test directory");
    Ok(())
}

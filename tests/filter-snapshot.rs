use anyhow::Result;
use tempfile::tempdir;

mod common;

use common::*;

#[test]
fn filter_snapshot_smoke() -> Result<()> {
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

    cli_run(working_path, backup_path)?
        .args(["filter-snapshot", "--skip", "backend", "LAST"])
        .assert()
        .success();

    // It didn't screw anything up, did it?
    cli_run(working_path, backup_path)?
        .args(["check", "-r"])
        .assert()
        .success();

    // Files should match except what we filtereed
    let orig_files = cli_run(working_path, backup_path)?
        .args(["ls", "LAST~"])
        .assert()
        .success();
    let orig_files_sans_filtered: Vec<&str> = stdout(&orig_files)
        .trim()
        .lines()
        .filter(|l| !l.contains("backend"))
        .collect();

    let filtered_files = cli_run(working_path, backup_path)?
        .args(["ls", "LAST"])
        .assert()
        .success();
    let filtered_files: Vec<&str> = stdout(&filtered_files).trim().lines().collect();
    assert_eq!(orig_files_sans_filtered, filtered_files);

    // To examine results
    // std::mem::forget(backup_dir);
    // std::mem::forget(copy_dir);
    Ok(())
}

use anyhow::{Result, ensure};
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

    // Let's backup our own code, skipping anything with "back" in it
    cli_run(working_path, backup_path)?
        .args(["backup", "--skip", "backpak/src/.*back"])
        .arg(project_dir.join("src"))
        .assert()
        .success();

    // Check that did what we expect.
    let ls_src = cli_run(working_path, backup_path)?
        .args(&["ls", "HEAD"])
        .assert()
        .success();
    let ls_src_output = stdout(&ls_src);

    ensure!(
        ls_src_output.lines().count() > 0,
        "Oops; filtered everything"
    );

    let mut worked = true;
    for l in ls_src_output.lines() {
        if l.contains("back") {
            eprintln!("Expected nothing with `back`, saw {l}");
            worked = false;
        }
    }
    ensure!(worked);

    // To examine results
    // std::mem::forget(backup_dir);
    Ok(())
}

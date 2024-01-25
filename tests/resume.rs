use anyhow::Result;
use assert_cmd::Command;
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

    // And back it up
    cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(project_dir.join("src"))
        .assert()
        .success();

    // We don't resume after snapshots;
    // we should already be done when those are written!
    let snap_glob = backup_path.join("snapshots/*");
    let index_glob = backup_path.join("indexes/*");
    let setup_resume = || {
        Command::new("sh")
            .arg("-c")
            .arg("rm -v ".to_owned() + &snap_glob.to_string_lossy())
            .assert()
            .success();
        Command::new("sh")
            .arg("-c")
            .arg(
                "mv -v ".to_owned()
                    + &index_glob.to_string_lossy()
                    + " "
                    + &working_path.join("backpak-wip.index").to_string_lossy(),
            )
            .assert()
            .success();
    };

    setup_resume();

    // Packs are "uploaded"
    // Try backup, assert text contains mentions of "deduped"
    cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(project_dir.join("src"))
        .assert()
        .success()
        .stderr(predicates::str::contains("deduped"));

    setup_resume();

    // Packs aren't "uploaded" yet, upload them.
    Command::new("sh")
        .arg("-c")
        .arg(
            "mv -v ".to_owned()
                + &backup_path.join("packs/*").to_string_lossy()
                + " "
                + &working_path.to_string_lossy(),
        )
        .assert()
        .success();

    cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(project_dir.join("src"))
        .assert()
        .success()
        .stderr(predicates::str::contains("deduped"));

    Ok(())
}

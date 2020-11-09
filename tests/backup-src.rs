use std::path::PathBuf;

use anyhow::*;
use tempfile::tempdir;

use backup_test::*;

#[test]
fn backup_src() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let backup_dir = tempdir().context("Failed to create temp test directory")?;
    init::run(backup_dir.path()).context("init failed")?;

    backup::run(
        backup_dir.path(),
        backup::Args {
            files: vec![PathBuf::from("src")],
        },
    )
    .context("backup failed")?;

    check::run(backup_dir.path(), check::Args { check_packs: true }).context("check failed")?;

    // To examine results
    // std::mem::forget(backup_dir);

    backup_dir
        .close()
        .context("Couldn't delete test directory")?;
    Ok(())
}

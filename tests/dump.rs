use anyhow::Result;
use tempfile::tempdir;

mod common;

use common::*;

#[test]
fn dump_src() -> Result<()> {
    let project_dir = std::env::current_dir()?;

    let backup_dir = tempdir()?;
    let backup_path = backup_dir.path();

    let working_dir = tempdir()?;
    let working_path = working_dir.path();

    cli_run(working_path, backup_path)?
        .args(["init", "filesystem"])
        .assert()
        .success();

    // Let's backup our own code
    cli_run(working_path, backup_path)?
        .arg("backup")
        .arg(project_dir.join("src"))
        .assert()
        .success();

    // Dump a directory (works like a non-recursive ls)
    let dump_src = cli_run(working_path, backup_path)?
        .args(&["dump", "LAST", "src"])
        .assert()
        .success();
    let dump_src_output = stdout(&dump_src);

    let src_entries = dir_entries(project_dir.join("src")).collect::<Vec<_>>();

    // We should expect 1 line per entry plus "src/" at the top.
    assert!(dump_src_output.starts_with("src/"));
    assert_eq!(src_entries.len() + 1, dump_src_output.lines().count());

    // Each of those lines should be one of the entries in src/
    for src_path in src_entries {
        let src = src_path
            .strip_prefix(&project_dir)
            .unwrap()
            .to_string_lossy();
        let src: &str = &*src;
        assert!(dump_src_output.contains(src));
    }

    // Dump main.rs and compare it to the real deal
    let dump_main = cli_run(working_path, backup_path)?
        .args(&["dump", "LAST", "src/main.rs"])
        .assert()
        .success();
    let main_output = stdout(&dump_main);
    let actual_main = std::fs::read_to_string(project_dir.join("src/main.rs"))?;
    assert_eq!(main_output, actual_main);

    // Cool, we dumped a directory and a file. Let's try some errors
    let mut fail = cli_run(working_path, backup_path)?
        .args(&["dump", "LAST", "src/nope.rs"])
        .assert()
        .failure();
    let mut fail_output = stderr(&fail);
    assert!(fail_output.contains("Couldn't find src/nope.rs in the given snapshot"));

    fail = cli_run(working_path, backup_path)?
        .args(&["dump", "LAST", "src/main.rs/nope"])
        .assert()
        .failure();
    fail_output = stderr(&fail);
    assert!(fail_output.contains("src/main.rs is a file, not a directory"));

    fail = cli_run(working_path, backup_path)?
        .args(&["dump", "LAST", "src/../src/main.rs"])
        .assert()
        .failure();
    fail_output = stderr(&fail);
    assert!(fail_output.contains("dump doesn't support absolute paths, .., etc."));

    // To examine results
    // std::mem::forget(backup_dir);
    Ok(())
}

#![allow(dead_code)]

use std::path::{Path, PathBuf};

use anyhow::Result;
use assert_cmd::Command;
use walkdir::WalkDir;

pub fn cli_run(working_dir: &Path, backup_path: &Path) -> Result<assert_cmd::Command> {
    let bin_name = env!("CARGO_PKG_NAME");
    let mut cmd = Command::cargo_bin(bin_name)?;
    cmd.arg("-C").arg(working_dir);
    cmd.arg("--config").arg(""); // NB: Ignore test machine state
    cmd.arg("--repository").arg(backup_path);
    cmd.arg("-vvv");
    Ok(cmd)
}

pub fn stderr(cmd: &assert_cmd::assert::Assert) -> &str {
    std::str::from_utf8(&cmd.get_output().stderr).unwrap()
}

pub fn stdout(cmd: &assert_cmd::assert::Assert) -> &str {
    std::str::from_utf8(&cmd.get_output().stdout).unwrap()
}

pub fn count_directory_entries<P: AsRef<Path>>(dir: P) -> usize {
    std::fs::read_dir(dir)
        .expect("Couldn't read dir")
        .map(|de| {
            let de = de.expect("Couldn't read dir entry");
            eprintln!("{}", de.path().display());
        })
        .count()
}

pub fn files_in<P: AsRef<Path>>(p: P) -> impl Iterator<Item = PathBuf> {
    WalkDir::new(p)
        .into_iter()
        .map(|e| e.expect("couldn't walk dir"))
        .filter(|e| e.file_type().is_file())
        .map(|e| e.into_path())
}

pub fn dir_entries<P: AsRef<Path>>(p: P) -> impl Iterator<Item = PathBuf> {
    std::fs::read_dir(p)
        .unwrap()
        .map(|e| e.expect("couldn't read dir").path())
}

pub fn normalize(o: &str) -> Vec<&str> {
    // Strip Opening... Building a master index... snapshot <hash>...
    o.trim().lines().skip(3).collect()
}

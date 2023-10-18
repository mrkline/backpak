use super::*;

use std::fs;
use std::io;

use camino::{Utf8Path, Utf8PathBuf};

pub struct FilesystemBackend {
    base_directory: Utf8PathBuf,
}

#[inline]
fn create_dir(d: &Utf8Path) -> Result<()> {
    fs::create_dir(d).with_context(|| format!("Couldn't create {d}"))
}

#[inline]
fn ensure_exists(e: &Utf8Path) -> Result<()> {
    ensure!(e.exists(), "{e} doesn't exist");
    Ok(())
}

impl FilesystemBackend {
    pub fn initialize(repository: &Utf8Path) -> Result<()> {
        if repository.exists() {
            ensure!(
                fs::read_dir(repository)
                    .with_context(|| format!("Couldn't read {repository}"))?
                    .count()
                    == 0,
                "The directory {repository} already exists and isn't empty"
            );
        } else {
            create_dir(repository)?;
        }

        create_dir(&repository.join("packs"))?;
        create_dir(&repository.join("indexes"))?;
        create_dir(&repository.join("snapshots"))?;

        Ok(())
    }

    pub fn open(repository: &Utf8Path) -> Result<Self> {
        let base_directory = Utf8PathBuf::from(repository);
        ensure_exists(&base_directory)?;
        ensure_exists(&base_directory.join("packs"))?;
        ensure_exists(&base_directory.join("indexes"))?;
        ensure_exists(&base_directory.join("snapshots"))?;

        Ok(Self { base_directory })
    }
}

impl Backend for FilesystemBackend {
    fn read<'a>(&'a self, from: &str) -> Result<Box<dyn Read + Send + 'a>> {
        let from = self.base_directory.join(from);
        Ok(Box::new(
            fs::File::open(&from).with_context(|| format!("Couldn't open {from}"))?,
        ))
    }

    fn write(&self, from: &mut dyn Read, to: &str) -> Result<()> {
        let to = self.base_directory.join(to);
        file_util::safe_copy_to_file(from, &to)?;
        Ok(())
    }

    fn remove(&self, which: &str) -> Result<()> {
        let which = self.base_directory.join(which);
        fs::remove_file(&which).with_context(|| format!("Couldn't remove {which}"))?;
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let prefix = self.base_directory.join(prefix);

        if prefix.is_file() {
            return Ok(vec![prefix.to_string()]);
        }

        let paths: Vec<String> = walk_dir(&prefix)?
            .iter()
            .map(|p| p.strip_prefix(&self.base_directory).unwrap())
            .map(|p| p.to_string())
            .collect();

        Ok(paths)
    }
}

fn walk_dir(dir: &Utf8Path) -> io::Result<Vec<Utf8PathBuf>> {
    let mut paths = Vec::new();
    for entry in Utf8Path::read_dir_utf8(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            paths.append(&mut walk_dir(path)?);
        } else {
            paths.push(path.to_owned());
        }
    }
    Ok(paths)
}

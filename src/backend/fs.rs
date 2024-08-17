use super::*;

use std::fs;
use std::io;

use byte_unit::Byte;
use camino::{Utf8Path, Utf8PathBuf};

use crate::file_util;

pub struct FilesystemBackend {
    pub base_directory: Utf8PathBuf,
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

pub fn initialize(
    repository: &Utf8Path,
    pack_size: Byte,
    filter: Option<String>,
    unfilter: Option<String>,
) -> Result<()> {
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

    let c = super::Config {
        pack_size,
        kind: super::Kind::Filesystem,
        filter,
        unfilter,
    };
    fs::write(repository.join("config.toml"), toml::to_string(&c)?)?;

    Ok(())
}

impl FilesystemBackend {
    pub fn open(repository: &Utf8Path) -> Result<Self> {
        let base_directory = Utf8PathBuf::from(repository);
        ensure_exists(&base_directory)?;
        ensure_exists(&base_directory.join("packs"))?;
        ensure_exists(&base_directory.join("indexes"))?;
        ensure_exists(&base_directory.join("snapshots"))?;

        Ok(Self { base_directory })
    }

    pub fn path_of(&self, p: &str) -> Utf8PathBuf {
        self.base_directory.join(p)
    }
}

impl Backend for FilesystemBackend {
    fn read(&self, from: &str) -> Result<Box<dyn Read + Send + 'static>> {
        let from = self.path_of(from);
        Ok(Box::new(
            fs::File::open(&from).with_context(|| format!("Couldn't open {from}"))?,
        ))
    }

    fn write(&self, _len: u64, from: &mut (dyn Read + Send), to: &str) -> Result<()> {
        let to = self.path_of(to);
        file_util::safe_copy_to_file(from, &to)?;
        Ok(())
    }

    fn remove(&self, which: &str) -> Result<()> {
        let which = self.path_of(which);
        fs::remove_file(&which).with_context(|| format!("Couldn't remove {which}"))?;
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<(String, u64)>> {
        let prefix = self.base_directory.join(prefix);

        if prefix.is_file() {
            return Ok(vec![(prefix.to_string(), prefix.metadata()?.len())]);
        }

        let str_and_len = |(p, len): &(Utf8PathBuf, u64)| -> Result<(String, u64)> {
            let s = p.strip_prefix(&self.base_directory).unwrap().to_string();
            Ok((s, *len))
        };

        let paths: Vec<(String, u64)> = walk_dir(&prefix)?
            .iter()
            // see file_utils::safe_copy_to_file()
            // Use the fancy new atomic file crate instead?
            .filter(|(p, _len)| p.extension() != Some("part"))
            .map(str_and_len)
            .collect::<Result<Vec<_>>>()?;

        Ok(paths)
    }
}

fn walk_dir(dir: &Utf8Path) -> io::Result<Vec<(Utf8PathBuf, u64)>> {
    let mut paths = Vec::new();
    for entry in Utf8Path::read_dir_utf8(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            paths.append(&mut walk_dir(path)?);
        } else {
            let len = entry.metadata()?.len();
            paths.push((path.to_owned(), len));
        }
    }
    Ok(paths)
}

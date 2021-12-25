use super::*;

use std::io;
use std::path::{Path, PathBuf};

use async_trait::async_trait;

pub struct FilesystemBackend {
    base_directory: PathBuf,
}

#[inline]
fn create_dir(d: &Path) -> Result<()> {
    std::fs::create_dir(d).with_context(|| format!("Couldn't create {}", d.display()))
}

#[inline]
fn ensure_exists(e: &Path) -> Result<()> {
    ensure!(e.exists(), "{} doesn't exist", e.display());
    Ok(())
}

impl FilesystemBackend {
    pub fn initialize(repository: &Path) -> Result<()> {
        if repository.exists() {
            ensure!(
                std::fs::read_dir(repository)
                    .with_context(|| format!("Couldn't read {}", repository.display()))?
                    .count()
                    == 0,
                "The directory {} already exists and isn't empty",
                repository.display()
            );
        } else {
            create_dir(repository)?;
        }

        create_dir(&repository.join("packs"))?;

        for b in 0..=255 {
            let pack_bucket = repository.join(format!("packs/{:02x}", b));
            create_dir(&pack_bucket)?;
        }

        create_dir(&repository.join("indexes"))?;
        create_dir(&repository.join("snapshots"))?;

        Ok(())
    }

    pub fn open(repository: &Path) -> Result<Self> {
        let base_directory = PathBuf::from(repository);
        ensure_exists(&base_directory)?;

        for b in 0..=255 {
            ensure_exists(&base_directory.join(format!("packs/{:02x}", b)))?;
        }

        ensure_exists(&base_directory.join("indexes"))?;
        ensure_exists(&base_directory.join("snapshots"))?;

        Ok(Self { base_directory })
    }
}

#[async_trait]
impl Backend for FilesystemBackend {
    async fn read<'a>(&'a self, from: &str) -> Result<Box<dyn AsyncRead + Send + 'a>> {
        let from = self.base_directory.join(from);
        Ok(Box::new(tokio::fs::File::open(&from).await.with_context(
            || format!("Couldn't open {}", from.display()),
        )?))
    }

    async fn write(&self, from: &mut (dyn AsyncRead + Unpin + Send), to: &str) -> Result<()> {
        let to = self.base_directory.join(to);
        let mut fh = tokio::fs::File::create(&to)
            .await
            .with_context(|| format!("Couldn't create {}", to.display()))?;
        tokio::io::copy(from, &mut fh).await?;
        Ok(())
    }

    async fn remove(&self, which: &str) -> Result<()> {
        let which = self.base_directory.join(which);
        tokio::fs::remove_file(&which)
            .await
            .with_context(|| format!("Couldn't remove {}", which.display()))?;
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let prefix = self.base_directory.join(prefix);

        if prefix.is_file() {
            return Ok(vec![prefix.to_str().expect("non-UTF-8 prefix").to_owned()]);
        }

        let paths: Vec<String> = walk_dir(&prefix)?
            .iter()
            .map(|p| p.strip_prefix(&self.base_directory).unwrap())
            .map(|p| p.to_str().expect("non-UTF-8 path in fs backend").to_owned())
            .collect();

        Ok(paths)
    }
}

fn walk_dir(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            paths.append(&mut walk_dir(&path)?);
        } else {
            paths.push(path);
        }
    }
    Ok(paths)
}

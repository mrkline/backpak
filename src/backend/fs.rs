use super::*;

use std::env::set_current_dir;
use std::fs::*;
use std::io;
use std::path::{Path, PathBuf};

use anyhow::*;

pub struct FilesystemBackend {
    base_directory: PathBuf,
}

impl FilesystemBackend {
    pub fn initialize(repository: &str) -> Result<()> {
        ensure!(
            !Path::new(repository).exists(),
            "The directory {} already exists",
            repository
        );

        create_dir(repository).with_context(|| format!("Couldn't create {}", repository))?;
        set_current_dir(repository)?;
        create_dir("packs")?;
        for b in 0..=255 {
            create_dir(format!("packs/{:02x}", b))?;
        }

        create_dir("indexes")?;

        Ok(())
    }

    pub fn open(repository: &str) -> Result<Self> {
        let base_directory = PathBuf::from(repository);
        ensure!(
            base_directory.exists(),
            "The directory {} doesn't exist",
            repository
        );

        for b in 0..=255 {
            let pack_bucket = base_directory.join(format!("packs/{:02x}", b));
            ensure!(
                pack_bucket.exists(),
                "The directory {} doesn't exist",
                pack_bucket.display()
            );
        }

        Ok(Self { base_directory })
    }
}

impl Backend for FilesystemBackend {
    fn read(&mut self, from: &str) -> Result<Box<dyn Read + Send>> {
        let from = self.base_directory.join(from);
        Ok(Box::new(File::open(&from).with_context(|| {
            format!("Couldn't open {}", from.display())
        })?))
    }

    fn write(&mut self, from: &mut dyn Read, to: &str) -> Result<()> {
        let to = self.base_directory.join(to);
        let mut fh =
            File::create(&to).with_context(|| format!("Couldn't create {}", to.display()))?;
        io::copy(from, &mut fh)?;
        Ok(())
    }

    fn list(&mut self, _prefix: &str) -> Result<Vec<String>> {
        todo!();
    }
}

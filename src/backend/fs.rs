use super::*;

use std::fs::*;
use std::io;
use std::path::{Path, PathBuf};

pub struct FilesystemBackend {
    base_directory: PathBuf,
}

impl SeekableReader for File {}

impl FilesystemBackend {
    pub fn initialize(repository: &Path) -> Result<()> {
        if repository.exists() {
            ensure!(
                read_dir(repository)
                    .with_context(|| format!("Couldn't read {}", repository.display()))?
                    .count()
                    == 0,
                "The directory {} already exists and isn't empty",
                repository.display()
            );
        } else {
            create_dir(repository)
                .with_context(|| format!("Couldn't create {}", repository.display()))?;
        }

        let packs_dir = repository.join("packs");
        create_dir(&packs_dir)
            .with_context(|| format!("Couldn't create {}", packs_dir.display()))?;

        for b in 0..=255 {
            let pack_bucket = repository.join(format!("packs/{:02x}", b));
            create_dir(&pack_bucket)
                .with_context(|| format!("Couldn't create {}", pack_bucket.display()))?;
        }

        let indexes_dir = repository.join("indexes");
        create_dir(&indexes_dir)
            .with_context(|| format!("Couldn't create {}", indexes_dir.display()))?;

        Ok(())
    }

    pub fn open(repository: &Path) -> Result<Self> {
        let base_directory = PathBuf::from(repository);
        ensure!(
            base_directory.exists(),
            "The directory {} doesn't exist",
            repository.display()
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
    fn read<'a>(&'a self, from: &str) -> Result<Box<dyn SeekableReader + Send + 'a>> {
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

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
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
    for entry in fs::read_dir(dir)? {
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

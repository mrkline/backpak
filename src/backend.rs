use std::io::prelude::*;

use anyhow::Result;

mod fs;

pub enum BackendType {
    Filesystem,
    // TODO: S3, B2, etc...
}

/// Determine the repo type based on its name.
pub fn determine_type(_repository: &str) -> Result<BackendType> {
    // We're just starting with filesystem
    Ok(BackendType::Filesystem)
}

// TODO: Should we make these async? Some backends (such as S3 via Rusoto)
// are going to be async, but we could `block_on()` for each request...
pub trait Backend {
    /// Read from the given key
    fn read(&mut self, from: &str) -> Result<Box<dyn Read + Send>>;

    /// Write the given read stream to the given key
    fn write(&mut self, from: &mut dyn Read, to: &str) -> Result<()>;

    /// Lists all keys with the given prefix
    fn list(&mut self, prefix: &str) -> Result<Vec<String>>;
}

pub fn initialize(repository: &str) -> Result<()> {
    match determine_type(repository)? {
        BackendType::Filesystem => fs::FilesystemBackend::initialize(repository),
    }
}

pub fn open(repository: &str) -> Result<Box<dyn Backend + Send>> {
    let backend = match determine_type(repository)? {
        BackendType::Filesystem => Box::new(fs::FilesystemBackend::open(repository)?),
    };
    Ok(backend)
}

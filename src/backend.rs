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

pub trait Backend {}

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

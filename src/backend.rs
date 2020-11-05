use std::ffi::OsStr;
use std::io::prelude::*;
use std::path::Path;

use anyhow::*;

use crate::hashing::ObjectId;

mod fs;
mod memory;

pub enum BackendType {
    Filesystem,
    // TODO: S3, B2, etc...
}

/// Determine the repo type based on its name.
pub fn determine_type(_repository: &Path) -> Result<BackendType> {
    // We're just starting with filesystem
    Ok(BackendType::Filesystem)
}

pub trait SeekableReader: Read + Seek {}

// TODO: Should we make these async? Some backends (such as S3 via Rusoto)
// are going to be async, but we could `block_on()` for each request...
pub trait Backend {
    /// Read from the given key
    fn read<'a>(&'a self, from: &str) -> Result<Box<dyn SeekableReader + Send + 'a>>;

    /// Write the given read stream to the given key
    fn write(&mut self, from: &mut dyn Read, to: &str) -> Result<()>;

    /// Lists all keys with the given prefix
    fn list(&self, prefix: &str) -> Result<Vec<String>>;

    // Let's put all the layout-specific stuff here so that we don't have paths
    // spread throughout the codebase.

    fn read_pack<'a>(&'a self, id: &ObjectId) -> Result<Box<dyn SeekableReader + Send + 'a>> {
        let hex = id.to_string();
        let pack_path = format!("packs/{}/{}.pack", &hex[0..2], hex);
        self.read(&pack_path)
            .with_context(|| format!("Couldn't open {}", pack_path))
    }

    fn read_index<'a>(&'a self, id: &ObjectId) -> Result<Box<dyn SeekableReader + Send + 'a>> {
        let index_path = format!("indexes/{}.index", id);
        self.read(&index_path)
            .with_context(|| format!("Couldn't open {}", index_path))
    }

    fn list_indexes(&self) -> Result<Vec<String>> {
        self.list("indexes/")
    }
}

pub fn initialize(repository: &Path) -> Result<()> {
    match determine_type(repository)? {
        BackendType::Filesystem => fs::FilesystemBackend::initialize(repository),
    }
}

pub fn open(repository: &Path) -> Result<Box<dyn Backend + Send + Sync>> {
    let backend = match determine_type(repository)? {
        BackendType::Filesystem => Box::new(fs::FilesystemBackend::open(repository)?),
    };
    Ok(backend)
}

/// Returns the desitnation path for the given temp file based on its extension
pub fn destination(src: &str) -> String {
    match Path::new(src).extension().and_then(OsStr::to_str) {
        Some("pack") => format!("packs/{}/{}", &src[0..2], src),
        Some("index") => format!("indexes/{}", src),
        _ => panic!("Unexpected extension on file to upload: {}", src),
    }
}

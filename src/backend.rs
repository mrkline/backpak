use std::ffi::OsStr;
use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

use anyhow::*;
use log::*;

use crate::hashing::ObjectId;

mod fs;
mod memory;

enum BackendType {
    Filesystem,
    // TODO: S3, B2, etc...
}

/// Determine the repo type based on its name.
fn determine_type(_repository: &Path) -> BackendType {
    // We're just starting with filesystem
    BackendType::Filesystem
}

// TODO: Should we make these async? Some backends (such as S3 via Rusoto)
// are going to be async, but we could `block_on()` for each request...
trait Backend {
    /// Read from the given key
    fn read<'a>(&'a self, from: &str) -> Result<Box<dyn Read + Send + 'a>>;

    /// Write the given read stream to the given key
    fn write(&self, from: &mut dyn Read, to: &str) -> Result<()>;

    fn remove(&self, which: &str) -> Result<()>;

    /// Lists all keys with the given prefix
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
}

// Use an enum instead of trait objects because we don't forsee ever having
// more than two types here (are the files local, or do we need to cache them?)
enum WritethroughCache {
    /// Since a filesystem backend is, well, on the file system,
    /// we don't need to make and store copies, worry about eviction, ...
    /// Just keep track of the base directory and pass file handles directly.
    /// Nice.
    Local { base_directory: PathBuf },
    // Remote // TODO LOL
}

pub struct CachedBackend {
    cache: WritethroughCache,
    backend: Box<dyn Backend + Send + Sync>,
}

impl CachedBackend {
    /// Read the object at the given key into a file and return a handle to that file.
    fn read(&self, from: &str) -> Result<File> {
        match &self.cache {
            WritethroughCache::Local { base_directory } => {
                let from = base_directory.join(from);
                Ok(File::open(&from)
                    .with_context(|| format!("Couldn't open {}", from.display()))?)
            }
        }
    }

    /// Take the completed file and its `<id>.<type>` name and
    /// store it to an object with the appropriate key per
    /// [`destination()`](destination)
    pub fn write(&self, from: &str, mut from_fh: File) -> Result<()> {
        match &self.cache {
            WritethroughCache::Local { base_directory } => {
                let to = base_directory.join(destination(from));
                let to = to.to_str().unwrap();
                // On Windows, we can't move an open file. Boo, Windows.
                // Don't bother closing, moving, and reopening if moving fails.
                if cfg!(target_family = "unix") && std::fs::rename(from, to).is_ok() {
                    log::debug!("Renamed {} to {}", from, to);
                    return Ok(());
                }
                // Otherwise, copy the file.
                from_fh.seek(std::io::SeekFrom::Start(0))?;
                self.backend.write(&mut from_fh, &to)?;
                log::debug!("Backed up {}. Removing temp copy", from);
                std::fs::remove_file(&from).with_context(|| format!("Couldn't remove {}", from))?;
            }
        }
        Ok(())
    }

    pub fn remove(&self, to_remove: &str) -> Result<()> {
        match &self.cache {
            WritethroughCache::Local { .. } => {
                // Just unlink the file!
                self.backend.remove(to_remove)
            } // On a remote backend, we'd have to unlink any cached file,
              // _then_ remove it from the remote side.
        }
    }

    // Let's put all the layout-specific stuff here so that we don't have paths
    // spread throughout the codebase.

    pub fn list_indexes(&self) -> Result<Vec<String>> {
        self.backend.list("indexes/")
    }

    pub fn list_snapshots(&self) -> Result<Vec<String>> {
        self.backend.list("snapshots/")
    }

    pub fn list_packs(&self) -> Result<Vec<String>> {
        self.backend.list("packs/")
    }

    pub fn probe_pack(&self, id: &ObjectId) -> Result<()> {
        let hex = id.to_string();
        let pack_path = format!("packs/{}/{}.pack", &hex[0..2], hex);
        let found_packs = self
            .backend
            .list(&pack_path)
            .with_context(|| format!("Couldn't find {}", pack_path))?;
        match found_packs.len() {
            0 => bail!("Couldn't find pack {}", hex),
            1 => Ok(()),
            multiple => panic!(
                "Expected one pack at {}, found several! {:?}",
                pack_path, multiple
            ),
        }
    }

    pub fn probe_snapshot(&self, id: &ObjectId) -> Result<()> {
        let snapshot_path = format!("snapshots/{}.snapshot", id);
        let found_snapshots = self
            .backend
            .list(&snapshot_path)
            .with_context(|| format!("Couldn't find {}", snapshot_path))?;
        match found_snapshots.len() {
            0 => bail!("Couldn't find snapshot {}", id),
            1 => Ok(()),
            multiple => panic!(
                "Expected one snapshot at {}, found several! {:?}",
                snapshot_path, multiple
            ),
        }
    }

    pub fn read_pack(&self, id: &ObjectId) -> Result<File> {
        let hex = id.to_string();
        let pack_path = format!("packs/{}/{}.pack", &hex[0..2], hex);
        self.read(&pack_path)
            .with_context(|| format!("Couldn't open {}", pack_path))
    }

    pub fn read_index(&self, id: &ObjectId) -> Result<File> {
        let index_path = format!("indexes/{}.index", id);
        self.read(&index_path)
            .with_context(|| format!("Couldn't open {}", index_path))
    }

    pub fn read_snapshot(&self, id: &ObjectId) -> Result<File> {
        let snapshot_path = format!("snapshots/{}.snapshot", id);
        self.read(&snapshot_path)
            .with_context(|| format!("Couldn't open {}", snapshot_path))
    }

    pub fn remove_pack(&self, id: &ObjectId) -> Result<()> {
        let hex = id.to_string();
        let pack_path = format!("packs/{}/{}.pack", &hex[0..2], hex);
        self.remove(&pack_path)
    }

    pub fn remove_index(&self, id: &ObjectId) -> Result<()> {
        let index_path = format!("indexes/{}.index", id);
        self.remove(&index_path)
    }

    pub fn remove_snapshot(&self, id: &ObjectId) -> Result<()> {
        let snapshot_path = format!("snapshots/{}.snapshot", id);
        self.remove(&snapshot_path)
    }
}

/// Initializes the appropriate type of backend from the repository path
pub fn initialize(repository: &Path) -> Result<()> {
    match determine_type(repository) {
        BackendType::Filesystem => fs::FilesystemBackend::initialize(repository),
    }
}

/// Factory function to open the appropriate type of backend from the repository path
pub fn open(repository: &Path) -> Result<CachedBackend> {
    info!("Opening repository '{}'", repository.display());
    let cached_backend = match determine_type(repository) {
        BackendType::Filesystem => {
            let backend = Box::new(fs::FilesystemBackend::open(repository)?);
            let base_directory = PathBuf::from(repository);
            CachedBackend {
                cache: WritethroughCache::Local { base_directory },
                backend,
            }
        }
    };
    Ok(cached_backend)
}

/// Returns the desitnation path for the given temp file based on its extension
pub fn destination(src: &str) -> String {
    match Path::new(src).extension().and_then(OsStr::to_str) {
        Some("pack") => format!("packs/{}/{}", &src[0..2], src),
        Some("index") => format!("indexes/{}", src),
        Some("snapshot") => format!("snapshots/{}", src),
        _ => panic!("Unexpected extension on file to upload: {}", src),
    }
}

/// Returns the ID of the object given its name
/// (assumed to be its `some/compontents/<Object ID>.<extension>`)
pub fn id_from_path<P: AsRef<Path>>(path: P) -> Result<ObjectId> {
    use std::str::FromStr;
    path.as_ref()
        .file_stem()
        .ok_or_else(|| anyhow!("Couldn't determine ID from {}", path.as_ref().display()))
        .and_then(|stem| ObjectId::from_str(stem.to_str().unwrap()))
}

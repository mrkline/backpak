//! Places where we can make a backup repository - the local filesystem,
//! (eventually) cloud hosts, etc.

use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, prelude::*};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, ensure, Context, Error, Result};
use log::*;

use crate::{counters, file_util, hashing::ObjectId};

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
    #[allow(unused)]
    Remote {
        cache_directory: PathBuf,
        max_size: usize,
    },
}

fn prune_cache(_cache_directory: &Path, _max_size: usize) -> Result<()> {
    todo!()
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
            WritethroughCache::Remote {
                cache_directory,
                max_size,
            } => {
                let cached = cache_directory.join(from);
                match File::open(&cached) {
                    // If it's in the cache, awesome!
                    Ok(f) => {
                        counters::bump(counters::Op::FileCacheHit);
                        Ok(f)
                    }
                    // Otherwise, first copy it into the cache,
                    // prune the cache, then serve up that cached copy.
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {
                        counters::bump(counters::Op::FileCacheMiss);
                        let backend_reader = self.backend.read(from)?;
                        let mut f = file_util::safe_copy_to_file(backend_reader, &cached)?;
                        f.seek(std::io::SeekFrom::Start(0))?;
                        prune_cache(cache_directory, *max_size)?;
                        Ok(f)
                    }
                    Err(e) => {
                        return Err(Error::from(e).context(format!("Couldn't open {}", from)));
                    }
                }
            }
        }
    }

    /// Take the completed file and its `<id>.<type>` name and
    /// store it to an object with the appropriate key per
    /// [`destination()`](destination)
    pub fn write(&self, from: &str, from_fh: File) -> Result<()> {
        match &self.cache {
            WritethroughCache::Local { base_directory } => {
                let to = base_directory.join(destination(from));
                file_util::move_opened(from, from_fh, &to)?;
            }
            // Write through! Write it into the cache,
            // copy the cached version to the backend, and prune the cache.
            WritethroughCache::Remote {
                cache_directory,
                max_size,
            } => {
                let cached = cache_directory.join(from);
                let mut f = file_util::move_opened(from, from_fh, &cached)?;
                f.seek(std::io::SeekFrom::Start(0))?;
                self.backend.write(&mut f, from)?;
                prune_cache(cache_directory, *max_size)?;
            }
        }
        Ok(())
    }

    fn remove(&self, to_remove: &str) -> Result<()> {
        match &self.cache {
            WritethroughCache::Local { .. } => {
                // Let backend.remove() unlink the file below
            }
            WritethroughCache::Remote {
                cache_directory, ..
            } => {
                // Remove it from the cache too. No worries if it isn't there.
                let cached = cache_directory.join(to_remove);
                match std::fs::remove_file(&cached) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => {
                        return Err(
                            Error::from(e).context(format!("Couldn't remove {}", cached.display()))
                        );
                    }
                }
            }
        }
        self.backend.remove(to_remove)
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
fn destination(src: &str) -> String {
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

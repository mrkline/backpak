//! Places where we can make a backup repository - the local filesystem,
//! (eventually) cloud hosts, etc.

use std::fs::File;
use std::io::prelude::*;
use std::io::Cursor;
use std::sync::Mutex;

use anyhow::{anyhow, bail, ensure, Context, Result};
use camino::Utf8Path;
use log::*;

use crate::{
    counters::{bump, Op},
    file_util,
    hashing::ObjectId,
};

mod cache;
mod fs;
mod memory;

use cache::Cache;

enum BackendType {
    Filesystem,
    // TODO: S3, B2, etc...
}

/// Determine the repo type based on its name.
fn determine_type(_repository: &Utf8Path) -> BackendType {
    // We're just starting with filesystem
    BackendType::Filesystem
}

/// A backend is anything we can read from, write to, list, and remove items from.
pub trait Backend {
    /// Read from the given key
    fn read<'a>(&'a self, from: &str) -> Result<Box<dyn Read + Send + 'a>>;

    /// Write the given read stream to the given key
    fn write(&self, from: &mut dyn Read, to: &str) -> Result<()>;

    fn remove(&self, which: &str) -> Result<()>;

    /// Lists all keys with the given prefix
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
}

/// Cached backends do what they say on the tin,
/// _or_ for the narrow case when we're writing unfiltered content to the filesystem,
/// a direct passthrough for that.
///
/// In the former case, the backend is also responsible for unlinking the files
/// it's given once they're safely backed up.
pub enum CachedBackend {
    /// Since a filesystem backend is, well, on the file system,
    /// we don't need to make and store copies, worry about eviction, ...
    /// Just keep track of the base directory and pass file handles directly.
    /// Nice.
    Direct { backend: fs::FilesystemBackend },
    Cached {
        cache: Mutex<Cache>,
        backend: Box<dyn Backend + Send + Sync>,
    },
}

impl CachedBackend {
    /// Read the object at the given key and return its buffer.
    ///
    /// This could just be the Vec<u8>, but most users are streaming and seeking.
    /// Provide a cursor for convenience; we can get the buf with an .into_inner()
    fn read(&self, name: &str) -> Result<Cursor<Vec<u8>>> {
        match &self {
            CachedBackend::Direct { backend } => {
                let from = backend.path_of(name);
                Ok(Cursor::new(
                    std::fs::read(&from).with_context(|| format!("Couldn't read {from}"))?,
                ))
            }
            CachedBackend::Cached { cache, backend } => {
                let tr = cache.lock().unwrap().try_read(name)?;
                if let Some(hit) = tr {
                    bump(Op::FileCacheHit);
                    Ok(Cursor::new(hit))
                } else {
                    bump(Op::FileCacheMiss);
                    let mut buf = vec![];
                    backend.read(name)?.read_to_end(&mut buf)?;
                    let mut c = cache.lock().unwrap();
                    // TODO: Should these just be one transaction?
                    // Or do we get better perf and no downsides this way?
                    c.insert(name, &buf)?;
                    c.prune()?;
                    Ok(Cursor::new(buf))
                }
            }
        }
    }

    /// Take the completed file and its `<id>.<type>` name and
    /// store it to an object with the appropriate key per
    /// `destination()`
    pub fn write(&self, name: &str, mut fh: File) -> Result<()> {
        match &self {
            CachedBackend::Direct { backend } => {
                let to = backend.path_of(name);
                file_util::move_opened(name, fh, to)?;
            }
            CachedBackend::Cached { cache, backend } => {
                // Write through!
                // Seek fh to the beginning, read it all to a buf.
                fh.seek(std::io::SeekFrom::Start(0))?;
                let mut buf = vec![];
                fh.read_to_end(&mut buf)?;
                drop(fh);
                // Write it through to the backend.
                backend.write(&mut Cursor::new(&buf), name)?;
                // Insert it into the cache.
                let mut c = cache.lock().unwrap();
                c.insert(name, &buf)?;
                // Prune the cache.
                c.prune()?;
                drop(c);
                // _Then_ unlink the file once we've persisted it in both places.
                std::fs::remove_file(name)?;
            }
        }
        Ok(())
    }

    fn remove(&self, name: &str) -> Result<()> {
        match &self {
            CachedBackend::Direct { backend } => backend.remove(name),
            CachedBackend::Cached { cache, backend } => {
                // Remove it from the cache too.
                // No worries if it isn't there, no need to prune.
                cache.lock().unwrap().evict(name)?;
                backend.remove(name)?;
                Ok(())
            }
        }
    }

    // Let's put all the layout-specific stuff here so that we don't have paths
    // spread throughout the codebase.

    fn list(&self, which: &str) -> Result<Vec<String>> {
        match &self {
            CachedBackend::Direct { backend } => backend.list(which),
            CachedBackend::Cached { backend, .. } => backend.list(which),
        }
    }

    pub fn list_indexes(&self) -> Result<Vec<String>> {
        self.list("indexes/")
    }

    pub fn list_snapshots(&self) -> Result<Vec<String>> {
        self.list("snapshots/")
    }

    pub fn list_packs(&self) -> Result<Vec<String>> {
        self.list("packs/")
    }

    pub fn probe_pack(&self, id: &ObjectId) -> Result<()> {
        let base32 = id.to_string();
        let pack_path = format!("packs/{}.pack", base32);
        let found_packs = self
            .list(&pack_path)
            .with_context(|| format!("Couldn't find {}", pack_path))?;
        match found_packs.len() {
            0 => bail!("Couldn't find pack {}", base32),
            1 => Ok(()),
            multiple => panic!(
                "Expected one pack at {}, found several! {:?}",
                pack_path, multiple
            ),
        }
    }

    pub fn read_pack(&self, id: &ObjectId) -> Result<Cursor<Vec<u8>>> {
        let base32 = id.to_string();
        let pack_path = format!("{}.pack", base32);
        self.read(&pack_path)
            .with_context(|| format!("Couldn't open {}", pack_path))
    }

    pub fn read_index(&self, id: &ObjectId) -> Result<Cursor<Vec<u8>>> {
        let index_path = format!("{}.index", id);
        self.read(&index_path)
            .with_context(|| format!("Couldn't open {}", index_path))
    }

    pub fn read_snapshot(&self, id: &ObjectId) -> Result<Cursor<Vec<u8>>> {
        let snapshot_path = format!("{}.snapshot", id);
        self.read(&snapshot_path)
            .with_context(|| format!("Couldn't open {}", snapshot_path))
    }

    pub fn remove_pack(&self, id: &ObjectId) -> Result<()> {
        let base32 = id.to_string();
        let pack_path = format!("{}.pack", base32);
        self.remove(&pack_path)
    }

    pub fn remove_index(&self, id: &ObjectId) -> Result<()> {
        let index_path = format!("{}.index", id);
        self.remove(&index_path)
    }

    pub fn remove_snapshot(&self, id: &ObjectId) -> Result<()> {
        let snapshot_path = format!("{}.snapshot", id);
        self.remove(&snapshot_path)
    }
}

/// Initializes the appropriate type of backend from the repository path
pub fn initialize(repository: &Utf8Path) -> Result<()> {
    match determine_type(repository) {
        BackendType::Filesystem => fs::FilesystemBackend::initialize(repository),
    }
}

/// Factory function to open the appropriate type of backend from the repository path
pub fn open(repository: &Utf8Path) -> Result<CachedBackend> {
    info!("Opening repository '{repository}'");
    let cached_backend = match determine_type(repository) {
        BackendType::Filesystem => {
            let backend = fs::FilesystemBackend::open(repository)?;
            CachedBackend::Direct { backend }
        }
    };
    Ok(cached_backend)
}

/// Returns the desitnation path for the given temp file based on its extension
fn destination(src: &str) -> String {
    match Utf8Path::new(src).extension() {
        Some("pack") => format!("packs/{}", src),
        Some("index") => format!("indexes/{}", src),
        Some("snapshot") => format!("snapshots/{}", src),
        _ => panic!("Unexpected extension on file: {}", src),
    }
}

/// Returns the ID of the object given its name
/// (assumed to be its `some/compontents/<Object ID>.<extension>`)
pub fn id_from_path<P: AsRef<Utf8Path>>(path: P) -> Result<ObjectId> {
    use std::str::FromStr;
    path.as_ref()
        .file_stem()
        .ok_or_else(|| anyhow!("Couldn't determine ID from {}", path.as_ref()))
        .and_then(ObjectId::from_str)
}

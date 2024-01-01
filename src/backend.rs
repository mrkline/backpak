//! Places where we can make a backup repository - the local filesystem,
//! (eventually) cloud hosts, etc.

use std::fs::File;
use std::io::{self, prelude::*};

use anyhow::{anyhow, bail, ensure, Context, Result};
use camino::Utf8Path;
use log::*;
use serde::{Deserialize, Serialize};

use crate::{
    counters::{bump, Op},
    file_util,
    hashing::ObjectId,
    pack,
};

mod cache;
mod filter;
mod fs;
mod memory;

use cache::Cache;

// lol: Serde wants a function to call for defaults.
fn defsize() -> u64 {
    pack::DEFAULT_PACK_SIZE
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub enum Kind {
    Filesystem,
    // ...?
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "defsize")]
    pub pack_size: u64,
    #[serde(rename = "type")]
    pub kind: Kind,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub unfilter: Option<String>,
}

fn read_config(p: &Utf8Path) -> Result<Config> {
    let s = std::fs::read_to_string(p).with_context(|| format!("Couldn't read config from {p}"))?;
    let c = toml::from_str(&s)?;
    Ok(c)
}

/// A backend is anything we can read from, write to, list, and remove items from.
pub trait Backend {
    /// Read from the given key
    fn read(&self, from: &str) -> Result<Box<dyn Read + Send + 'static>>;

    /// Write the given read stream to the given key
    fn write(&self, from: &mut (dyn Read + Send), to: &str) -> Result<()>;

    fn remove(&self, which: &str) -> Result<()>;

    /// Lists all keys with the given prefix
    fn list(&self, prefix: &str) -> Result<Vec<String>>;
}

/// Cached backends do what they say on the tin,
/// _or_ for the narrow case when we're writing unfiltered content to the filesystem,
/// a direct passthrough for that.
///
/// The backend is also responsible for unlinking the files
/// it's given once they're safely backed up.
/// (Bad separation of concerns? Perhaps. Convenient API? Yes.)
pub enum CachedBackend {
    /// Since a filesystem backend is, well, on the file system,
    /// we don't win anything by caching.
    /// Just read and write files directly. Nice.
    File {
        backend: fs::FilesystemBackend,
    },
    // The usual case: the backend is some remotely-hosted storage,
    // or local, but the files are filtered first.
    // Here we can benefit from a write-through cache.
    Cached {
        cache: Cache,
        backend: Box<dyn Backend + Send + Sync>,
    },
    // Test backend please ignore
    Memory {
        backend: memory::MemoryBackend,
    },
}

pub trait SeekableRead: Read + Seek + Send + 'static {}
impl<T> SeekableRead for T where T: Read + Seek + Send + 'static {}

impl CachedBackend {
    /// Read the object at the given key and return its file.
    fn read(&self, name: &str) -> Result<Box<dyn SeekableRead>> {
        match &self {
            CachedBackend::File { backend } => {
                let from = backend.path_of(name);
                let fd = File::open(&from).with_context(|| format!("Couldn't open {from}"))?;
                Ok(Box::new(fd))
            }
            CachedBackend::Cached { cache, backend } => {
                let tr = cache.try_read(name)?;
                if let Some(hit) = tr {
                    bump(Op::BackendCacheHit);
                    Ok(Box::new(hit))
                } else {
                    bump(Op::BackendCacheMiss);
                    let mut inserted = cache.insert(name, &mut *backend.read(name)?)?;
                    cache.prune()?;
                    inserted.seek(io::SeekFrom::Start(0))?;
                    Ok(Box::new(inserted))
                }
            }
            CachedBackend::Memory { backend } => Ok(Box::new(backend.read_cursor(name)?)),
        }
    }

    /// Take the completed file and its `<id>.<type>` name and
    /// store it to an object with the appropriate key per
    /// `destination()`
    pub fn write(&self, name: &str, mut fh: File) -> Result<()> {
        match &self {
            CachedBackend::File { backend } => {
                let to = backend.path_of(name);
                file_util::move_opened(name, fh, to)?;
            }
            CachedBackend::Cached { cache, backend } => {
                // Write through!
                bump(Op::BackendCacheWrite);
                fh.seek(std::io::SeekFrom::Start(0))?;
                // Write it through to the backend.
                backend.write(&mut fh, name)?;
                // Insert it into the cache.
                cache.insert_file(name, fh)?;
                // Prune the cache.
                cache.prune()?;
            }
            CachedBackend::Memory { backend } => {
                fh.seek(std::io::SeekFrom::Start(0))?;
                backend.write(&mut fh, name)?;
                std::fs::remove_file(name)?;
            }
        }
        Ok(())
    }

    fn remove(&self, name: &str) -> Result<()> {
        match &self {
            CachedBackend::File { backend } => backend.remove(name),
            CachedBackend::Cached { cache, backend } => {
                // Remove it from the cache too.
                // No worries if it isn't there, no need to prune.
                bump(Op::BackendCacheEviction);
                cache.evict(name)?;
                backend.remove(name)?;
                Ok(())
            }
            CachedBackend::Memory { backend } => backend.remove(name),
        }
    }

    // Let's put all the layout-specific stuff here so that we don't have paths
    // spread throughout the codebase.

    fn list(&self, which: &str) -> Result<Vec<String>> {
        match &self {
            CachedBackend::File { backend } => backend.list(which),
            CachedBackend::Cached { backend, .. } => backend.list(which),
            CachedBackend::Memory { backend } => backend.list(which),
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

    pub fn read_pack(&self, id: &ObjectId) -> Result<Box<dyn SeekableRead>> {
        let base32 = id.to_string();
        let pack_path = format!("{}.pack", base32);
        self.read(&pack_path)
            .with_context(|| format!("Couldn't open {}", pack_path))
    }

    pub fn read_index(&self, id: &ObjectId) -> Result<Box<dyn SeekableRead>> {
        let index_path = format!("{}.index", id);
        self.read(&index_path)
            .with_context(|| format!("Couldn't open {}", index_path))
    }

    pub fn read_snapshot(&self, id: &ObjectId) -> Result<Box<dyn SeekableRead>> {
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
    fs::FilesystemBackend::initialize(repository)
}

/// Initializes an in-memory cache for testing purposes.
pub fn in_memory() -> CachedBackend {
    CachedBackend::Memory {
        backend: memory::MemoryBackend::new(),
    }
}

/// Factory function to open the appropriate type of backend from the repository path
pub fn open(repository: &Utf8Path) -> Result<(Config, CachedBackend)> {
    info!("Opening repository {repository}");
    let stat =
        std::fs::metadata(repository).with_context(|| format!("Couldn't stat {repository}"))?;
    let c = if stat.is_dir() {
        let cfg_file = repository.join("config.toml");
        read_config(&cfg_file)
    } else if stat.is_file() {
        read_config(repository)
    } else {
        bail!("{repository} is not a file or directory")
    }?;
    debug!("Read config: {c:?}");
    ensure!(
        c.filter.is_some() == c.unfilter.is_some(),
        "{repository} config should set `filter` and `unfilter` or neither."
    );
    let backend = match c.kind {
        Kind::Filesystem => fs::FilesystemBackend::open(repository)?,
    };
    // Don't bother checking unfilter; we ensure both are set if one is above.
    let cached_backend = if c.filter.is_some() {
        let filtered = Box::new(filter::BackendFilter {
            filter: c.filter.clone().unwrap(),
            unfilter: c.unfilter.clone().unwrap(),
            raw: Box::new(backend),
        });
        CachedBackend::Cached {
            backend: filtered,
            // TODO: load global cache
            cache: cache::Cache::new(Utf8Path::new("cache"))?,
        }
    } else {
        CachedBackend::File { backend }
    };
    Ok((c, cached_backend))
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

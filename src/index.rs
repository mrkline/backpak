//! Build, read, and write [indexes](Index) of packs' contents.
//!
//! An index file contains magic bytes followed by a zstd-compressed CBOR record with:
//!
//! 1. A map of packs IDs to their manifests
//!
//! 2. A list of previous indexes that this one supersedes.
//!    (This is a safety for `prune` and `rebuild_index` so that if they're interrupted
//!    after uploading the new index but *before* deleting the old ones,
//!    future commands will safely ignore the old indexes.)
//!
//! Each backup makes an index of the packs it uploaded.
//! By gathering all of these (minus superseded ones) into a master index,
//! we get the contents of every pack in the repo without having to download them
//! and read their manifests.
//! From there we can make hash maps for constant-time lookup of any blob in the repo.
//!
//! If anything ever happens to the index, we still have the same information in packs' manifests,
//! so we can rebuild it.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::prelude::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, anyhow, bail, ensure};
use rustc_hash::{FxHashMap, FxHashSet};
use serde_derive::{Deserialize, Serialize};
use tracing::*;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::backend;
use crate::concurrently::concurrently;
use crate::counters;
use crate::file_util::{check_magic, nice_size};
use crate::hashing::{HashingReader, HashingWriter, ObjectId};
use crate::pack::{PackManifest, PackMetadata};

const MAGIC_BYTES: &[u8] = b"MKBAKIDX1";

// Persist WIP (but valid) indexes to a known name so that an interrupted
// backup can read it in and know what we've already backed up.
pub const WIP_NAME: &str = "backpak-wip.index";

/// Maps a pack's ID to the manifest of blobs it holds.
pub type PackMap = BTreeMap<ObjectId, PackManifest>;

/// Maps packs to the blobs they contain,
/// and lists any previous indexes they supersede.
#[derive(Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Index {
    pub supersedes: BTreeSet<ObjectId>,
    pub packs: PackMap,
}

impl Index {
    #[inline]
    fn is_empty(&self) -> bool {
        self.supersedes.is_empty() && self.packs.is_empty()
    }
}

/// Should indexing be resumable?
/// Saving a WIP index is the main machinery we have for resuming backups.
/// But sometimes, especially when running rebuild-index,
/// we already know about all the packs and just want to cram them in as quick as possible.
#[derive(Debug, PartialEq, Eq)]
pub enum Resumable {
    No,
    Yes,
}

/// Gather metadata for completed packs from `rx` into an index file,
/// and upload the index files when they reach a sufficient size.
pub async fn index(
    resumable: Resumable,
    starting_index: Index,
    mut rx: Receiver<PackMetadata>,
    to_upload: Sender<(String, File)>,
    indexed_packs: Arc<AtomicU64>,
) -> Result<bool> {
    let mut index = starting_index;
    let mut persisted = None;

    // If we're given a non-empty index, write that out to start with.
    // (For example, it could be an index from `prune` that omits packs
    // we no longer need. If we don't write it but delete those packs anyways...)
    if !index.is_empty() && resumable == Resumable::Yes {
        persisted = Some(to_temp_file(&index)?);
    }

    // For each pack...
    while let Some(PackMetadata { id, manifest }) = rx.recv().await {
        ensure!(
            index.packs.insert(id, manifest).is_none(),
            "Duplicate pack received: {}",
            id
        );

        indexed_packs.fetch_add(1, Ordering::Relaxed);

        if resumable == Resumable::Yes {
            // Rewrite the index every time we get a pack.
            // That way the temp index should always contain a complete list of packs,
            // allowing us to resume a backup from the last finished pack.
            persisted = Some(to_temp_file(&index)?);
        }
    }
    // If we haven't been saving a WIP index, write it all out now.
    if !index.is_empty() && resumable == Resumable::No {
        persisted = Some(to_temp_file(&index)?);
    }

    if let Some((index_id, mut fh)) = persisted {
        // We want to keep the WIP file around until we're sure we're uploaded it
        // so that we're resumable all the way to the end.
        // A simple but slightly kludgey way is to just copy the file and delete it
        // once we know everything is uploaded.
        // (Other options would complicated `CachedBackend` which assumes that files
        // we're writing are passed over with their current name equalling the final one.
        // It's not a big deal; indexes are small.
        fh.seek(std::io::SeekFrom::Start(0))?;
        let index_name = format!("{}.index", index_id);
        let mut renamed = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&index_name)
            .with_context(|| format!("Couldn't open {index_name} to write the final index"))?;
        std::io::copy(&mut fh, &mut renamed)?;
        renamed.sync_all()?;
        drop(fh);

        debug!(
            "Index {} finished ({})",
            index_id,
            nice_size(renamed.metadata()?.len())
        );

        to_upload
            .send((index_name, renamed))
            .await
            .context("indexer -> uploader channel exited early")?;
        Ok(true)
    } else {
        debug!("No new indexes created - nothing changed");
        Ok(false)
    }
}

fn to_temp_file(index: &Index) -> Result<(ObjectId, File)> {
    // Could we speed things up by reusing the same file handle instead of
    // opening, writing, and closing each time we update the WIP index file?
    // Probably, but we'd have to seek back to the beginning each time,
    // _and_ we'd be assuming that the file grows larger each time.
    // (This _might_ not be true since its contents are compressed...)
    let mut tf = tempfile::Builder::new()
        .prefix("temp-backpak-")
        .suffix(".index")
        .tempfile_in(".")
        .context("Couldn't open temporary index for writing")?;

    let id = to_file(tf.as_file_mut(), index)?;
    let f = tf
        .persist(WIP_NAME)
        .with_context(|| format!("Couldn't persist WIP index to {}", WIP_NAME))?;
    Ok((id, f))
}

fn to_file(fh: &mut fs::File, index: &Index) -> Result<ObjectId> {
    fh.write_all(MAGIC_BYTES)?;

    let mut zstd = zstd::stream::write::Encoder::new(fh, 0)?;
    zstd.multithread(num_cpus::get_physical() as u32)?;

    let mut hasher = HashingWriter::new(zstd);

    ciborium::into_writer(index, &mut hasher)?;

    let (id, zstd) = hasher.finalize();
    let fh = zstd.finish()?;
    fh.sync_all()?;

    Ok(id)
}

/// Load all indexes from the provided backend and combines them into a master
/// index, removing any superseded ones.
pub async fn build_master_index(cached_backend: Arc<backend::CachedBackend>) -> Result<Index> {
    build_master_index_with_sizes(cached_backend)
        .await
        .map(|(mi, _ts)| mi)
}

/// [`build_master_index`] plus the size for each loaded index.
///
/// Nice for usage reporting, since it saves us another backend query.
pub async fn build_master_index_with_sizes(
    cached_backend: Arc<backend::CachedBackend>,
) -> Result<(Index, Vec<u64>)> {
    info!("Building a master index");

    #[derive(Debug, Default)]
    struct Results {
        superseded_indexes: BTreeSet<ObjectId>,
        loaded_indexes: BTreeMap<ObjectId, PackMap>,
        sizes: Vec<u64>,
    }

    let shared = Arc::new(Mutex::new(Results::default()));

    let todos = cached_backend
        .list_indexes()?
        .into_iter()
        .map(|(index_file, index_len)| {
            let s = shared.clone();
            let cb = cached_backend.clone();
            async move {
                let index_id = backend::id_from_path(&index_file)?;
                let mut loaded_index = load(&index_id, &*cb)?;
                let mut guard = s.lock().unwrap();
                guard.sizes.push(index_len);
                guard
                    .superseded_indexes
                    .append(&mut loaded_index.supersedes);
                ensure!(
                    guard
                        .loaded_indexes
                        .insert(index_id, loaded_index.packs)
                        .is_none(),
                    "Duplicate index {} read from backend!",
                    index_file
                );
                Ok(())
            }
        });
    concurrently(todos).await?;

    let mut shared = Arc::into_inner(shared).unwrap().into_inner().unwrap();

    // Strip out superseded indexes.
    for superseded in &shared.superseded_indexes {
        if shared.loaded_indexes.remove(superseded).is_some() {
            debug!("Index {} is superseded and can be deleted.", superseded);
        }
    }

    let mut master_pack_map = BTreeMap::new();
    for index in shared.loaded_indexes.values_mut() {
        master_pack_map.append(index);
    }

    Ok((
        Index {
            supersedes: shared.superseded_indexes,
            packs: master_pack_map,
        },
        shared.sizes,
    ))
}

/// A result of [`blob_to_pack_map()`],
/// mapping [`Blob`](crate::blob::Blob) IDs to the the pack where each is stored
pub type BlobMap = FxHashMap<ObjectId, ObjectId>;

/// Given an index, produce a mapping that traces [`Blob`](crate::blob::Blob)s
/// to the packs where they're stored
pub fn blob_to_pack_map(index: &Index) -> Result<BlobMap> {
    debug!("Building a blob -> pack map");
    let mut mapping = FxHashMap::default();

    for (pack_id, manifest) in &index.packs {
        for blob in manifest {
            if let Some(other_pack) = mapping.insert(blob.id, *pack_id) {
                // TODO: Should this just be a warning?
                // This might happen in weird cases like concurrent backups
                // but isn't a huge issue so long as the blobs are valid...
                bail!(
                    "Duplicate blob {} in pack {}, previously seen in pack {}",
                    blob.id,
                    pack_id,
                    other_pack
                );
            }
        }
    }

    Ok(mapping)
}

/// Gather the set of all blob IDs in a given index.
pub fn blob_id_set(index: &Index) -> Result<FxHashSet<ObjectId>> {
    debug!("Building a set of all blob IDs");
    let mut blobs = FxHashSet::default();

    for (pack_id, manifest) in &index.packs {
        for blob in manifest {
            if !blobs.insert(blob.id) {
                // TODO: Ditto - just warn?
                bail!("Duplicate blob {} in pack {}", blob.id, pack_id);
            }
        }
    }

    Ok(blobs)
}

/// Map all blob IDs to their blob size.
pub fn blob_to_size_map(index: &Index) -> Result<FxHashMap<ObjectId, u32>> {
    debug!("Mapping blobs IDs to their size");
    let mut size_map = FxHashMap::default();

    for (pack_id, manifest) in &index.packs {
        for blob in manifest {
            if size_map.insert(blob.id, blob.length).is_some() {
                bail!("Duplicate blob {} in pack {}", blob.id, pack_id);
            }
        }
    }

    Ok(size_map)
}

/// Load the index from the given reader,
/// also returning its calculated ID.
fn from_reader<R: Read>(r: &mut R) -> Result<(Index, ObjectId)> {
    check_magic(r, MAGIC_BYTES).context("Wrong magic bytes for index file")?;

    let decoder =
        zstd::stream::read::Decoder::new(r).context("Decompression of index file failed")?;
    let mut hasher = HashingReader::new(decoder);
    let index = ciborium::from_reader(&mut hasher).context("CBOR decoding of index file failed")?;
    let (id, _) = hasher.finalize();
    Ok((index, id))
}

pub fn read_wip() -> Result<Option<Index>> {
    let mut fd = match File::open(WIP_NAME) {
        Ok(w) => w,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                return Ok(None);
            }
            let e = anyhow!(e).context(format!("Couldn't open {WIP_NAME}"));
            return Err(e);
        }
    };
    let (index, _) = from_reader(&mut fd)?;
    Ok(Some(index))
}

/// Load the index with the given ID from the backend,
/// verifying its contents match its ID.
pub fn load(id: &ObjectId, cached_backend: &backend::CachedBackend) -> Result<Index> {
    let (index, calculated_id) = from_reader(&mut cached_backend.read_index(id)?)
        .with_context(|| format!("Couldn't load index {}", id))?;
    ensure!(
        *id == calculated_id,
        "Index {}'s now hashes to {} - Consider running backpak rebuild-index.",
        id,
        calculated_id
    );
    counters::bump(counters::Op::IndexLoad);
    Ok(index)
}

#[cfg(test)]
mod test {
    use super::*;

    use tempfile::tempfile;

    use crate::blob;
    use crate::pack::PackManifestEntry;

    fn build_test_index() -> Index {
        let mut supersedes = BTreeSet::new();
        supersedes.insert(ObjectId::hash(b"Some previous index"));
        supersedes.insert(ObjectId::hash(b"Another previous index"));

        let mut packs = BTreeMap::new();
        packs.insert(
            ObjectId::hash(b"pack o' chunks"),
            vec![
                PackManifestEntry {
                    blob_type: blob::Type::Chunk,
                    length: 42,
                    id: ObjectId::hash(b"a chunk"),
                },
                PackManifestEntry {
                    blob_type: blob::Type::Chunk,
                    length: 9001,
                    id: ObjectId::hash(b"another chunk"),
                },
            ],
        );
        packs.insert(
            ObjectId::hash(b"pack o'trees"),
            vec![
                PackManifestEntry {
                    blob_type: blob::Type::Tree,
                    length: 182,
                    id: ObjectId::hash(b"first tree"),
                },
                PackManifestEntry {
                    blob_type: blob::Type::Tree,
                    length: 22,
                    id: ObjectId::hash(b"second tree"),
                },
                PackManifestEntry {
                    blob_type: blob::Type::Tree,
                    length: 11,
                    id: ObjectId::hash(b"third tree"),
                },
            ],
        );
        Index { supersedes, packs }
    }

    #[test]
    /// Pack manifest and ID remains stable from build to build.
    fn stability() -> Result<()> {
        let index = build_test_index();

        /*
        let mut fh = File::create("tests/references/index.stability")?;
        let mut hasher = HashingWriter::new(fh);
        ciborium::into_writer(&index, &mut hasher)?;
        let (id, _fh) = hasher.finalize();
        */

        let mut index_cbor = Vec::new();
        ciborium::into_writer(&index, &mut index_cbor)?;
        let id = ObjectId::hash(&index_cbor);

        // ID remains stable
        assert_eq!(
            format!("{}", id),
            "e3vr9p4gmumq8i1dafgum50iirupu6ahk9fn6c781b09a"
        );
        // Contents remain stable
        // (We could just use the ID and length,
        // but having some example CBOR in the repo seems helpful.)
        let from_example = fs::read("tests/references/index.stability")?;
        assert_eq!(index_cbor, from_example);
        Ok(())
    }

    #[test]
    fn round_trip() -> Result<()> {
        let index = build_test_index();
        let mut fh = tempfile()?;
        let written_id = to_file(&mut fh, &index)?;

        fh.seek(std::io::SeekFrom::Start(0))?;
        let (read_index, read_id) = from_reader(&mut fh)?;

        assert_eq!(index, read_index);
        assert_eq!(written_id, read_id);
        Ok(())
    }
}

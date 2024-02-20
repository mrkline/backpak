use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{anyhow, ensure, Context, Result};
use camino::Utf8Path;
use clap::Parser;
use log::*;
use rayon::prelude::*;
use rustc_hash::FxHashSet;

use crate::backend;
use crate::backup;
use crate::blob;
use crate::hashing::ObjectId;
use crate::index;
use crate::pack;
use crate::read;
use crate::snapshot;
use crate::tree;

/// Garbage collect: condense the backup, throwing out unused data.
///
/// Forgetting a snapshot doesn't delete any data it references,
/// since many snapshots might share the same backed-up data.
/// (Letting snapshots reuse data like this is what makes them small
/// and backups fast!) To actually delete things, we need to *prune*.
///
/// Packs are searched for chunks (of backed up files) and trees
/// (i.e., directories) no longer used by any snapshot.
/// Those with without *any* data referenced by snapshots are deleted,
/// and those with *some* data referenced by snapshots are repacked.
#[derive(Debug, Parser)]
#[command(verbatim_doc_comment)]
pub struct Args {
    #[clap(short = 'n', long)]
    pub dry_run: bool,
}

pub fn run(repository: &Utf8Path, args: Args) -> Result<()> {
    // Build the usual suspects.
    let (backend_config, cached_backend) =
        backend::open(repository, backend::CacheBehavior::Normal)?;
    let index = index::build_master_index(&cached_backend)?;
    let blob_map = index::blob_to_pack_map(&index)?;

    let snapshots_and_forests = load_snapshots_and_forests(
        &cached_backend,
        &mut tree::Cache::new(&index, &blob_map, &cached_backend),
    )?;

    let reachable_chunks = reachable_chunks(snapshots_and_forests.par_iter().map(|s| &s.forest));
    let (reusable_packs, packs_to_prune) =
        partition_reusable_packs(&index, &snapshots_and_forests, &reachable_chunks);
    let (droppable_packs, sparse_packs) =
        partition_droppable_packs(&packs_to_prune, &snapshots_and_forests, &reachable_chunks);

    // Once we've partitioned packs, we don't need every single reachable chunk.
    // Drop that, since it could be huge.
    drop(reachable_chunks);

    if packs_to_prune.is_empty() {
        info!("No unused blobs in any packs! Nothing to do.");
        return Ok(());
    }

    // TODO: Should build_master_index() return some set of all packs read
    // so we don't have to traverse the backend twice?
    let superseded = cached_backend
        .list_indexes()?
        .iter()
        .map(backend::id_from_path)
        .collect::<Result<BTreeSet<ObjectId>>>()?;

    // We care much less about packs in use
    trace!(
        "Packs [{}] are entirely in use",
        reusable_packs
            .keys()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    debug!(
        "Packs [{}] could be repacked",
        sparse_packs
            .keys()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    debug!(
        "Packs [{}] can be dropped",
        droppable_packs
            .keys()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    debug!(
        "Indexes [{}] could be replaced",
        superseded
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    info!(
        "Keep {} packs, rewrite {}, drop {}, and replace the {} current indexes",
        reusable_packs.len(),
        sparse_packs.len(),
        droppable_packs.len(),
        superseded.len()
    );

    // We just needed these for diagnostics; axe em.
    drop(sparse_packs);
    drop(droppable_packs);

    let reusable_packs: BTreeMap<ObjectId, pack::PackManifest> = reusable_packs
        .into_iter()
        .map(|(id, manifest)| (*id, manifest.clone()))
        .collect();
    let new_index = index::Index {
        packs: reusable_packs,
        supersedes: superseded.clone(),
    };

    // As we repack our snapshots, skip blobs in the 100% reachable packs.
    let mut packed_blobs = index::blob_id_set(&new_index)?;

    let backend_config = Arc::new(backend_config);
    let cached_backend = Arc::new(cached_backend);
    let mut backup = (!args.dry_run)
        .then(|| backup::spawn_backup_threads(backend_config, cached_backend.clone(), new_index));

    // Get a reader to load the chunks we're repacking.
    let mut reader = read::BlobReader::new(&cached_backend, &index, &blob_map);

    walk_snapshots(
        &snapshots_and_forests,
        &mut reader,
        &mut packed_blobs,
        &mut backup,
    )?;

    // NB: Before deleting the old indexes, we make sure the new one's been written.
    //     This ensures there's no point in time when we don't have a valid index
    //     of reachable blobs in packs. rebuild-index plays the same game.
    //
    //     Any concurrent writers (writing a backup at the same time)
    //     will upload their own index only after all packs are uploaded,
    //     making sure indexes never refer to missing packs. (I hope...)
    if let Some(b) = backup {
        b.join()?;
    }

    if !args.dry_run {
        // Remove old indexes _before_ removing packs such that we don't have
        // indexes referring to missing packs.
        for old_index in &superseded {
            cached_backend.remove_index(old_index)?;
        }
        for old_pack in packs_to_prune.keys() {
            cached_backend.remove_pack(old_pack)?;
        }
    }

    Ok(())
}

struct SnapshotAndForest {
    id: ObjectId,
    snapshot: snapshot::Snapshot,
    forest: tree::Forest,
}

fn load_snapshots_and_forests(
    cached_backend: &backend::CachedBackend,
    tree_cache: &mut tree::Cache,
) -> Result<Vec<SnapshotAndForest>> {
    snapshot::load_chronologically(cached_backend)?
        .into_iter()
        .map(|(snapshot, id)| {
            let forest = tree::forest_from_root(&snapshot.tree, tree_cache)?;
            Ok(SnapshotAndForest {
                id,
                snapshot,
                forest,
            })
        })
        .collect()
}

/// Collect all file chunks from the provided forests
fn reachable_chunks<'a, I: ParallelIterator<Item = &'a tree::Forest>>(
    forests: I,
) -> FxHashSet<&'a ObjectId> {
    forests
        .map(tree::chunks_in_forest)
        .reduce(FxHashSet::default, |mut a, b| {
            a.extend(b);
            a
        })
}

/// Partition packs into those that have 100% reachable blobs
/// and those that don't.
///
/// We'll reuse the former, and repack blobs from the latter.
#[allow(clippy::type_complexity)]
fn partition_reusable_packs<'a>(
    index: &'a index::Index,
    snapshots_and_forests: &[SnapshotAndForest],
    reachable_chunks: &FxHashSet<&ObjectId>,
) -> (
    BTreeMap<&'a ObjectId, &'a pack::PackManifest>,
    BTreeMap<&'a ObjectId, &'a pack::PackManifest>,
) {
    index.packs.iter().partition(|(_pack_id, manifest)| {
        // Reusable packs are ones where all blobs are reachable.
        manifest
            .iter()
            .map(|entry| &entry.id)
            .all(|id| blob_is_reachable(id, snapshots_and_forests, reachable_chunks))
    })
}

/// Partition packs into those that have 0% reachable blobs
/// and those that have _some_.
///
/// This is just so that we can accurately report which packs will be dropped
/// completely.
#[allow(clippy::type_complexity)]
fn partition_droppable_packs<'a>(
    packs_to_prune: &BTreeMap<&'a ObjectId, &'a pack::PackManifest>,
    snapshots_and_forests: &[SnapshotAndForest],
    reachable_chunks: &FxHashSet<&ObjectId>,
) -> (
    BTreeMap<&'a ObjectId, &'a pack::PackManifest>,
    BTreeMap<&'a ObjectId, &'a pack::PackManifest>,
) {
    packs_to_prune.iter().partition(|(_pack_id, manifest)| {
        // Droppable packs are ones where no blobs are reachable
        !manifest
            .iter()
            .map(|entry| &entry.id)
            .any(|id| blob_is_reachable(id, snapshots_and_forests, reachable_chunks))
    })
}

fn blob_is_reachable(
    blob: &ObjectId,
    snapshots_and_forests: &[SnapshotAndForest],
    reachable_chunks: &FxHashSet<&ObjectId>,
) -> bool {
    // A blob is reachable if it's either a chunk
    // (Isn't it nice we conveniently precomputed the set of all chunks?)
    // or it's a tree in the forst.
    reachable_chunks.contains(blob)
        || snapshots_and_forests
            .iter()
            .map(|snap_and_forest| &snap_and_forest.forest)
            .any(|forest| forest.contains_key(blob))
}

fn walk_snapshots(
    snapshots_and_forests: &[SnapshotAndForest],
    reader: &mut read::BlobReader,
    packed_blobs: &mut FxHashSet<ObjectId>,
    backup: &mut Option<backup::Backup>,
) -> Result<()> {
    // Walk from newest to oldest snapshots so that we prioritize the locality of chunks
    // in newer snapshots. This is probably a horse a piece - you could argue that
    // older snapshots are more important - but all the blobs will get packed up regardless.
    for snapshot in snapshots_and_forests.iter().rev() {
        walk_snapshot(snapshot, reader, packed_blobs, backup)?
    }
    Ok(())
}

fn walk_snapshot(
    snapshot_and_forest: &SnapshotAndForest,
    reader: &mut read::BlobReader,
    packed_blobs: &mut FxHashSet<ObjectId>,
    backup: &mut Option<backup::Backup>,
) -> Result<()> {
    debug!(
        "Repacking any loose blobs from snapshot {}",
        snapshot_and_forest.id
    );
    walk_tree(
        &snapshot_and_forest.snapshot.tree,
        &snapshot_and_forest.forest,
        reader,
        packed_blobs,
        backup,
    )
    .with_context(|| format!("In snapshot {}", snapshot_and_forest.id))
}

fn walk_tree(
    tree_id: &ObjectId,
    forest: &tree::Forest,
    reader: &mut read::BlobReader,
    packed_blobs: &mut FxHashSet<ObjectId>,
    backup: &mut Option<backup::Backup>,
) -> Result<()> {
    let tree: &tree::Tree = forest
        .get(tree_id)
        .ok_or_else(|| anyhow!("Missing tree {}", tree_id))
        .unwrap();

    for (path, node) in tree {
        match &node.contents {
            tree::NodeContents::File { chunks } => {
                for chunk in chunks {
                    if packed_blobs.insert(*chunk) {
                        repack_chunk(chunk, path, reader, backup)?;
                    } else {
                        trace!("Skipping chunk {}; already packed", chunk);
                    }
                }
            }
            tree::NodeContents::Symlink { .. } => {
                // Nothing to repack for symlinks.
            }
            tree::NodeContents::Directory { subtree } => {
                walk_tree(subtree, forest, reader, packed_blobs, backup)?
            }
        }
    }

    if packed_blobs.insert(*tree_id) {
        repack_tree(tree_id, tree, backup)?;
    } else {
        trace!("Skipping tree {}; already packed", tree_id);
    }
    Ok(())
}

fn repack_chunk(
    id: &ObjectId,
    path: &Utf8Path,
    reader: &mut read::BlobReader,
    backup: &mut Option<backup::Backup>,
) -> Result<()> {
    trace!("Repacking chunk {id} from {path}");
    if let Some(b) = backup {
        let contents = blob::Contents::Buffer(reader.read_blob(id)?);
        b.chunk_tx.send(blob::Blob {
            contents,
            id: *id,
            kind: blob::Type::Chunk,
        })?;
    }
    Ok(())
}

fn repack_tree(
    id: &ObjectId,
    tree: &tree::Tree,

    backup: &mut Option<backup::Backup>,
) -> Result<()> {
    trace!("Repacking tree {}", id);

    let (reserialized, check_id) = tree::serialize_and_hash(tree)?;
    // Sanity check:
    ensure!(
        check_id == *id,
        "Tree {} has a different ID ({}) when reserialized",
        id,
        check_id
    );

    if let Some(b) = backup {
        let contents = blob::Contents::Buffer(reserialized);
        b.tree_tx.send(blob::Blob {
            contents,
            id: *id,
            kind: blob::Type::Tree,
        })?;
    }

    Ok(())
}

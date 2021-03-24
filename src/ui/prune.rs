use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;
use std::sync::Arc;

use anyhow::*;
use log::*;
use rayon::prelude::*;
use structopt::StructOpt;

use crate::backend;
use crate::backup;
use crate::blob;
use crate::hashing::ObjectId;
use crate::index;
use crate::pack;
use crate::read;
use crate::snapshot;
use crate::tree;

#[derive(Debug, StructOpt)]
pub struct Args {
    #[structopt(short = "n", long)]
    pub dry_run: bool,
}

pub fn run(repository: &Path, args: Args) -> Result<()> {
    // Build the usual suspects.
    let cached_backend = Arc::new(backend::open(repository)?);
    let index = index::build_master_index(&cached_backend)?;
    let blob_map = index::blob_to_pack_map(&index)?;

    let snapshots_and_forests = load_snapshots_and_forests(
        &cached_backend,
        &mut tree::Cache::new(&index, &blob_map, &cached_backend),
    )?;

    let reachable_chunks = reachable_chunks(snapshots_and_forests.par_iter().map(|s| &s.forest));
    let (reusable_packs, sparse_packs) =
        partition_packs(&index, &snapshots_and_forests, &reachable_chunks);

    // Once we've partitioned packs, we don't need every single reachable chunk.
    // Drop that, since it could be huge.
    drop(reachable_chunks);

    if sparse_packs.is_empty() {
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

    debug!(
        "Packs {} are entirely in use",
        reusable_packs
            .keys()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    debug!(
        "Packs {} could be repacked",
        sparse_packs
            .keys()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    debug!(
        "Indexes {} could be replaced",
        superseded
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    info!(
        "Keep {} packs, rewrite {}, and replace the {} current indexes",
        reusable_packs.len(),
        sparse_packs.len(),
        superseded.len()
    );

    let reusable_packs: BTreeMap<ObjectId, pack::PackManifest> = reusable_packs
        .into_iter()
        .map(|(id, manifest)| (*id, manifest.clone()))
        .collect();
    let new_index = index::Index {
        packs: reusable_packs,
        supersedes: superseded.clone(),
    };

    // As we repack our snapshots, skip blobs in the 100% reachable packs.
    let mut packed_blobs = index::blob_set(&new_index)?;

    let mut backup =
        (!args.dry_run).then(|| backup::spawn_backup_threads(cached_backend.clone(), new_index));

    // Get a reader to load the chunks we're repacking.
    let mut reader = read::BlobReader::new(&cached_backend, &index, &blob_map);

    walk_snapshots(
        &snapshots_and_forests,
        &mut reader,
        &mut packed_blobs,
        &mut backup,
    )?;

    if let Some(b) = backup {
        b.join()?;
    }

    if !args.dry_run {
        for old_index in &superseded {
            cached_backend.remove_index(old_index)?;
        }
        for old_pack in sparse_packs.keys() {
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
) -> HashSet<&'a ObjectId> {
    forests
        .map(|f| tree::chunks_in_forest(f))
        .reduce(HashSet::new, |mut a, b| {
            a.extend(b);
            a
        })
}

/// Partition packs into those that have 100% reachable blobs
/// and those that don't.
///
/// We'll reuse the former, and repack blobs from the latter.
#[allow(clippy::type_complexity)]
fn partition_packs<'a>(
    index: &'a index::Index,
    snapshots_and_forests: &[SnapshotAndForest],
    reachable_chunks: &HashSet<&ObjectId>,
) -> (
    BTreeMap<&'a ObjectId, &'a pack::PackManifest>,
    BTreeMap<&'a ObjectId, &'a pack::PackManifest>,
) {
    let partitioned = index.packs.iter().partition(|(_pack_id, manifest)| {
        // Reusable packs are ones where all blobs...
        manifest.iter().map(|entry| &entry.id).all(|id| {
            // Are reachable chunks...
            reachable_chunks.contains(id) ||
                // Or reachable trees.
                snapshots_and_forests
                    .iter()
                    .map(|snap_and_forest| &snap_and_forest.forest)
                    .any(|forest| forest.contains_key(id))
        })
    });

    partitioned
}

fn walk_snapshots(
    snapshots_and_forests: &[SnapshotAndForest],
    reader: &mut read::BlobReader,
    packed_blobs: &mut HashSet<ObjectId>,
    backup: &mut Option<backup::Backup>,
) -> Result<()> {
    for snapshot in snapshots_and_forests.iter().rev() {
        walk_snapshot(snapshot, reader, packed_blobs, backup)?
    }
    Ok(())
}

fn walk_snapshot(
    snapshot_and_forest: &SnapshotAndForest,
    reader: &mut read::BlobReader,
    packed_blobs: &mut HashSet<ObjectId>,
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
    packed_blobs: &mut HashSet<ObjectId>,
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
    path: &Path,
    reader: &mut read::BlobReader,
    backup: &mut Option<backup::Backup>,
) -> Result<()> {
    trace!("Repacking chunk {} from {}", id, path.display());
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

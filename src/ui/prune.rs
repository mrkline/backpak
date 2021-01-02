use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::Path;

use anyhow::*;
use log::*;
use rayon::prelude::*;
use structopt::StructOpt;

use crate::backend;
use crate::hashing::ObjectId;
use crate::index;
use crate::pack;
use crate::snapshot;
use crate::tree;

#[derive(Debug, StructOpt)]
pub struct Args {
    #[structopt(short = "n", long)]
    pub dry_run: bool,
}

pub fn run(repository: &Path, args: Args) -> Result<()> {
    // Build the usual suspects.
    let cached_backend = backend::open(repository)?;
    let index = index::build_master_index(&cached_backend)?;
    let blob_map = index::blob_to_pack_map(&index)?;
    let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);

    let snapshots_and_forests = load_snapshots_and_forests(&cached_backend, &mut tree_cache)?;

    let reachable_chunks = reachable_chunks(snapshots_and_forests.iter().map(|s| &s.forest));
    let (reusable_packs, partially_unused_packs) =
        partition_packs(&index, &snapshots_and_forests, &reachable_chunks)?;

    // Once we've partitioned packs, we don't need every single reachable chunk.
    // Drop that, since it could be huge.
    drop(reachable_chunks);

    if partially_unused_packs.is_empty() {
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
        partially_unused_packs
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
        partially_unused_packs.len(),
        superseded.len()
    );

    if args.dry_run {
        return Ok(());
    }

    // As we repack our snapshots, skip blobs in the 100% reachable packs.
    let reusable_blobs = blobs_in_packs(reusable_packs.values().copied().par_bridge());

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
fn reachable_chunks<'a, I: Iterator<Item = &'a tree::Forest>>(forests: I) -> HashSet<&'a ObjectId> {
    forests
        .map(|f| tree::chunks_in_forest(f))
        .fold(HashSet::new(), |mut a, b| {
            a.extend(b);
            a
        })
}

/// Partition packs into those that have 100% reachable blobs
/// and those that don't.
///
/// We'll reuse the former, and repack blobs from the latter.
fn partition_packs<'a>(
    index: &'a index::Index,
    snapshots_and_forests: &[SnapshotAndForest],
    reachable_chunks: &HashSet<&ObjectId>,
) -> Result<(
    BTreeMap<&'a ObjectId, &'a pack::PackManifest>,
    BTreeMap<&'a ObjectId, &'a pack::PackManifest>,
)> {
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

    Ok(partitioned)
}

/// Collect a set of all blobs in the given pack manifests.
///
/// Copies the IDs since we'll be passing them to [`pack::pack()`](pack::pack)
fn blobs_in_packs<'a, I: ParallelIterator<Item = &'a pack::PackManifest>>(
    packs: I,
) -> HashSet<ObjectId> {
    packs
        .fold_with(HashSet::new(), |mut set, manifest| {
            for entry in manifest {
                set.insert(entry.id);
            }
            set
        })
        .reduce_with(|mut a, b| {
            a.extend(b);
            a
        })
        .unwrap_or_else(HashSet::new)
}

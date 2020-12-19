use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

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

/// Check the repository for errors
///
/// By default this assumes file integrity of the backup,
/// and only ensure that needed files can be found and downloaded.
/// If --read-packs is specified, ensure that each pack has the expected blobs,
/// that those blobs match its manifest, and that those blobs match the index.
#[derive(Debug, StructOpt)]
#[structopt(verbatim_doc_comment)]
pub struct Args {
    /// Check all blobs in all packs
    #[structopt(short, long)]
    pub read_packs: bool,
}

pub fn run(repository: &Path, args: Args) -> Result<()> {
    let mut trouble = false;

    let cached_backend = backend::open(repository)?;

    let index = index::build_master_index(&cached_backend)?;

    info!("Checking all packs listed in indexes");
    let borked_packs = AtomicUsize::new(0);
    index.packs.par_iter().for_each(|(pack_id, manifest)| {
        if let Err(e) = check_pack(&cached_backend, pack_id, manifest, args.read_packs) {
            error!("Problem with pack {}: {:?}", pack_id, e);
            borked_packs.fetch_add(1, Ordering::Relaxed);
        }
    });
    let borked_packs = borked_packs.load(Ordering::SeqCst);
    if borked_packs != 0 {
        error!("{} broken packs", borked_packs);
        trouble = true;
    }

    info!("Checking that all chunks in snapshots are reachable");
    let blob_map = index::blob_to_pack_map(&index)?;
    let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);

    // Map the chunks that belong in each snapshot.
    let mut chunks_to_snapshots: HashMap<ObjectId, HashSet<ObjectId>> = HashMap::new();

    for snapshot_path in cached_backend.backend.list_snapshots()? {
        let snapshot_id = backend::id_from_path(&snapshot_path)?;
        let snapshot = snapshot::load(&snapshot_id, &cached_backend)?;

        let snapshot_tree = tree::forest_from_root(&snapshot.tree, &mut tree_cache)?;

        for chunks in snapshot_tree.values().map(|tree| chunks_in_tree(&*tree)) {
            for chunk in chunks {
                let needed_by = chunks_to_snapshots.entry(chunk).or_insert(HashSet::new());
                needed_by.insert(snapshot_id);
            }
        }
    }

    let mut missing_chunks = 0;
    for (chunk, snapshots) in &chunks_to_snapshots {
        if blob_map.contains_key(chunk) {
            trace!(
                "Chunk {} is reachable (used by {} snapshots)",
                chunk,
                snapshots.len()
            );
        } else {
            error!(
                "Chunk {} is unreachable! (Used by snapshots {})",
                chunk,
                snapshots
                    .iter()
                    .map(|id| id.short_name())
                    .collect::<Vec<String>>()
                    .join(", ")
            );
            missing_chunks += 1;
        }
    }
    if missing_chunks != 0 {
        error!("{} missing chunks", missing_chunks);
        trouble = true;
    }

    if trouble {
        bail!("Check failed!");
    } else {
        Ok(())
    }
}

fn chunks_in_tree(tree: &tree::Tree) -> HashSet<ObjectId> {
    tree.par_iter()
        .map(|(_, node)| chunks_in_node(node))
        .fold_with(HashSet::new(), |mut set, node_chunks| {
            for chunk in node_chunks {
                set.insert(*chunk);
            }
            set
        })
        .reduce_with(|a, b| a.union(&b).cloned().collect())
        .unwrap_or(HashSet::new())
}

fn chunks_in_node(node: &tree::Node) -> &[ObjectId] {
    match &node.contents {
        tree::NodeContents::Directory { .. } => &[],
        tree::NodeContents::File { chunks, .. } => chunks,
    }
}

#[inline]
fn check_pack(
    cached_backend: &backend::CachedBackend,
    pack_id: &ObjectId,
    manifest: &[pack::PackManifestEntry],
    read_packs: bool,
) -> Result<()> {
    if read_packs {
        let mut pack = cached_backend.read_pack(pack_id)?;
        pack::verify(&mut pack, manifest)?;
        debug!("Pack {} verified", pack_id);
    } else {
        cached_backend.backend.probe_pack(pack_id)?;
        debug!("Pack {} found", pack_id);
    }
    Ok(())
}

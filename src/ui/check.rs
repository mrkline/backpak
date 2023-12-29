use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{bail, Result};
use clap::Parser;
use log::*;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::backend;
use crate::hashing::ObjectId;
use crate::index;
use crate::pack;
use crate::snapshot;
use crate::tree;

/// Check the repository for errors
///
/// By default this assumes integrity of the backup,
/// and only ensures that needed files can be found and downloaded.
/// If `--read-packs` is specified, ensure that each pack has the expected blobs,
/// that those blobs match its manifest, and that those blobs match the index.
#[derive(Debug, Parser)]
#[clap(verbatim_doc_comment)]
pub struct Args {
    /// Check the contents of packs, not just that they exist
    #[clap(short, long)]
    pub read_packs: bool,
}

pub fn run(repository: &camino::Utf8Path, args: Args) -> Result<()> {
    let mut trouble = false;

    let (_cfg, cached_backend) = backend::open(repository)?;

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

    info!("Checking for packs not listed in indexes");
    let pack_ids = cached_backend
        .list_packs()?
        .iter()
        .map(backend::id_from_path)
        .collect::<Result<Vec<_>>>()?;
    let mut unlisted_packs: usize = 0;
    for pack_id in pack_ids {
        if !index.packs.contains_key(&pack_id) {
            warn!("Pack {pack_id} not listed in any index");
            unlisted_packs += 1;
        }
    }
    if unlisted_packs > 0 {
        warn!("{unlisted_packs} are not listed in any index. Someone is backing up concurrently or you should consider running backpak rebuild-index");
    }

    info!("Checking that all chunks in snapshots are reachable");
    let blob_map = index::blob_to_pack_map(&index)?;
    let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);

    // Map the chunks that belong in each snapshot.
    let chunks_to_snapshots = map_chunks_to_snapshots(&cached_backend, &mut tree_cache)?;

    let mut missing_chunks: usize = 0;
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
    if missing_chunks > 0 {
        error!("{} missing chunks", missing_chunks);
        trouble = true;
    }

    if trouble {
        bail!("Check failed!");
    } else {
        Ok(())
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
        cached_backend.probe_pack(pack_id)?;
        debug!("Pack {} found", pack_id);
    }
    Ok(())
}

/// Maps all reachable chunks to the set of snapshots that use them
fn map_chunks_to_snapshots(
    cached_backend: &backend::CachedBackend,
    tree_cache: &mut tree::Cache,
) -> Result<FxHashMap<ObjectId, FxHashSet<ObjectId>>> {
    let mut chunks_to_snapshots = FxHashMap::default();

    for snapshot_path in cached_backend.list_snapshots()? {
        let snapshot_id = backend::id_from_path(&snapshot_path)?;
        let snapshot = snapshot::load(&snapshot_id, cached_backend)?;

        let snapshot_tree = tree::forest_from_root(&snapshot.tree, tree_cache)?;

        for chunks in snapshot_tree
            .values()
            .map(|tree| tree::chunks_in_tree(tree))
        {
            for chunk in chunks {
                let needed_by: &mut FxHashSet<ObjectId> =
                    chunks_to_snapshots.entry(*chunk).or_default();
                needed_by.insert(snapshot_id);
            }
        }
    }

    Ok(chunks_to_snapshots)
}

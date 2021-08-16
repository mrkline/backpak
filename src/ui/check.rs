use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::*;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use log::*;
use rustc_hash::{FxHashMap, FxHashSet};
use structopt::StructOpt;
use tokio::task::spawn;

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

pub async fn run(repository: &Path, args: Args) -> Result<()> {
    let mut trouble = false;

    let cached_backend = Arc::new(backend::open(repository)?);

    let index = index::build_master_index(&*cached_backend).await?;

    info!("Checking all packs listed in indexes");
    let borked_packs = Arc::new(AtomicUsize::new(0));

    let checkers = FuturesUnordered::new();

    if args.read_packs {
        for (pack_id, manifest) in &index.packs {
            let pack_id = pack_id.clone();
            let manifest = manifest.clone();
            let cached_backend = cached_backend.clone();
            let borked_packs = borked_packs.clone();
            checkers.push(spawn(async move {
                let mut pack = cached_backend.read_pack(&pack_id)?;
                if let Err(e) = pack::verify(&mut pack, &manifest) {
                    error!("Problem with pack {}: {:?}", pack_id, e);
                    borked_packs.fetch_add(1, Ordering::Relaxed);
                }
                debug!("Pack {} verified", pack_id);
                Result::<()>::Ok(())
            }));
        }

        checkers
            .fold::<Result<()>, _, _>(Ok(()), |acc, res| async move {
                acc.and(res.expect("pack checking task panicked"))
            })
            .await?;
    } else {
        for (pack_id, _manifest) in &index.packs {
            cached_backend.probe_pack(pack_id).await?;
            debug!("Pack {} found", pack_id);
        }
    }

    let borked_packs = borked_packs.load(Ordering::SeqCst);
    if borked_packs != 0 {
        error!("{} broken packs", borked_packs);
        trouble = true;
    }

    info!("Checking that all chunks in snapshots are reachable");
    let blob_map = index::blob_to_pack_map(&index)?;
    let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);

    // Map the chunks that belong in each snapshot.
    let chunks_to_snapshots = map_chunks_to_snapshots(&cached_backend, &mut tree_cache).await?;

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

/// Maps all reachable chunks to the set of snapshots that use them
async fn map_chunks_to_snapshots(
    cached_backend: &backend::CachedBackend,
    tree_cache: &mut tree::Cache<'_>,
) -> Result<FxHashMap<ObjectId, FxHashSet<ObjectId>>> {
    let mut chunks_to_snapshots = FxHashMap::default();

    for snapshot_path in cached_backend.list_snapshots().await? {
        let snapshot_id = backend::id_from_path(&snapshot_path)?;
        let snapshot = snapshot::load(&snapshot_id, cached_backend)?;

        let snapshot_tree = tree::forest_from_root(&snapshot.tree, tree_cache)?;

        for chunks in snapshot_tree
            .values()
            .map(|tree| tree::chunks_in_tree(&*tree))
        {
            for chunk in chunks {
                let needed_by = chunks_to_snapshots
                    .entry(*chunk)
                    .or_insert_with(FxHashSet::default);
                needed_by.insert(snapshot_id);
            }
        }
    }

    Ok(chunks_to_snapshots)
}

use std::sync::{
    Arc,
    atomic::{AtomicU32, AtomicU64, Ordering},
};

use anyhow::{Result, bail};
use clap::Parser;
use console::Term;
use rustc_hash::{FxHashMap, FxHashSet};
use tracing::*;

use crate::backend;
use crate::concurrently;
use crate::config::Configuration;
use crate::hashing::ObjectId;
use crate::index;
use crate::pack;
use crate::progress::{ProgressTask, print_download_line, spinner};
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
    read_packs: bool,
}

#[derive(Default)]
pub struct ReadStatus {
    packs_total: u32,
    packs_read: AtomicU32,
    blobs_total: u64,
    blobs_read: AtomicU64,
}

pub async fn run(config: &Configuration, repository: &camino::Utf8Path, args: Args) -> Result<()> {
    let mut trouble = false;

    // NB: We always want to read when checking the backend!
    // Just because it's in-cache doesn't mean it's backed up.
    let (_cfg, cached_backend) = backend::open(
        repository,
        config.cache_size,
        backend::CacheBehavior::AlwaysRead,
    )?;
    let cached_backend = Arc::new(cached_backend);

    let index = Arc::new(index::build_master_index(cached_backend.clone()).await?);

    info!("Downloading pack list");
    let all_packs = cached_backend.list_packs()?;
    let borked_packs = Arc::new(AtomicU32::new(0));
    if args.read_packs {
        let stats = Arc::new(ReadStatus {
            packs_total: index.packs.len() as u32,
            blobs_total: index
                .packs
                .values()
                .map(|manifest| manifest.len() as u64)
                .sum(),
            ..Default::default()
        });
        let progress = {
            let cb = cached_backend.clone();
            ProgressTask::spawn(move |i| {
                print_progress(i, &Term::stdout(), &stats, &cb.bytes_downloaded)
            })
        };
        // Actually read the packs; do this in parallel as much as the backend allows
        let checks = index.clone().packs.iter().map(|(pack_id, manifest)| {
            let cb = cached_backend.clone();
            let s = stats.clone();
            let b = borked_packs.clone();
            async move {
                match check_pack(&*cb, &pack_id, &manifest, &s.blobs_read) {
                    Ok(()) => debug!("Pack {pack_id} verified"),
                    Err(e) => {
                        error!("Pack {pack_id}: {e:?}");
                        b.fetch_add(1, Ordering::Relaxed);
                    }
                };
                s.packs_read.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        });
        concurrently::concurrently(checks).await?;

        progress.join();
    } else {
        // If we don't have to read the packs, just list them all
        // and make sure we find the indexed ones in that list.
        info!("Checking that all indexed packs are present");
        for pack_id in index.packs.keys() {
            match backend::probe_pack(&all_packs, pack_id) {
                Ok(()) => debug!("Pack {} found", pack_id),
                Err(e) => {
                    error!("{e:?}"); // Error already has a message about specific pack
                    borked_packs.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
    let borked_packs = borked_packs.load(Ordering::SeqCst);
    if borked_packs != 0 {
        error!("{} broken packs", borked_packs);
        trouble = true;
    }

    info!("Checking for unreachable packs (not listed in indexes)");
    warn_on_unreachable_packs(&index, &all_packs)?;

    info!("Checking that all chunks in snapshots are reachable");
    let blob_map = index::blob_to_pack_map(&index)?;

    // Map the chunks that belong in each snapshot.
    let chunks_to_snapshots = map_chunks_to_snapshots(
        &cached_backend,
        &mut tree::Cache::new(&index, &blob_map, &cached_backend),
    )?;

    let mut missing_chunks: usize = 0;
    for (chunk, snapshots) in &chunks_to_snapshots {
        if !blob_map.contains_key(chunk) {
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
        info!("Check complete");
        Ok(())
    }
}

fn check_pack(
    cached_backend: &backend::CachedBackend,
    pack_id: &ObjectId,
    manifest: &[pack::PackManifestEntry],
    blobs_read: &AtomicU64,
) -> Result<()> {
    let mut pack = cached_backend.read_pack(pack_id)?;
    pack::verify(&mut pack, manifest, blobs_read)?;
    Ok(())
}

/// Warns about unreachable packs. Returns the total pack size for usage stats.
pub fn warn_on_unreachable_packs(index: &index::Index, all_packs: &[(String, u64)]) -> Result<u64> {
    let mut total_pack_size = 0u64;
    let pack_ids = all_packs
        .iter()
        .map(|(pack, pack_len)| {
            total_pack_size += *pack_len;
            pack
        })
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
        warn!(
            "{unlisted_packs} {} unreachable. Consider running `rebuild-index` if you aren't running `backup` right now.",
            if unlisted_packs == 1 {
                "pack is"
            } else {
                "packs are"
            }
        );
    }
    Ok(total_pack_size)
}

/// Maps all reachable chunks to the set of snapshots that use them
fn map_chunks_to_snapshots(
    cached_backend: &backend::CachedBackend,
    tree_cache: &mut tree::Cache,
) -> Result<FxHashMap<ObjectId, FxHashSet<ObjectId>>> {
    let mut chunks_to_snapshots = FxHashMap::default();

    for (snapshot_path, _snapshot_len) in cached_backend.list_snapshots()? {
        let snapshot_id = backend::id_from_path(&snapshot_path)?;
        let snapshot = snapshot::load(&snapshot_id, cached_backend)?;

        let snapshot_tree = tree::forest_from_root(&snapshot.tree, tree_cache)?;

        for chunks in snapshot_tree
            .values()
            .map(|tree| tree::chunks_in_tree(tree))
        {
            for chunk in chunks {
                let needed_by: &mut FxHashSet<ObjectId> =
                    chunks_to_snapshots.entry(chunk).or_default();
                needed_by.insert(snapshot_id);
            }
        }
    }

    Ok(chunks_to_snapshots)
}

fn print_progress(i: usize, term: &Term, stats: &ReadStatus, down: &AtomicU64) -> Result<()> {
    if i > 0 {
        term.clear_last_lines(2)?;
    }

    let s = spinner(i);
    let p = stats.packs_read.load(Ordering::Relaxed);
    let tp = stats.packs_total;
    let b = stats.blobs_read.load(Ordering::Relaxed);
    let tb = stats.blobs_total;
    let perc = b as f64 / tb as f64 * 100.0;
    println!("{s} {p}/{tp} packs | {b}/{tb} blobs ({perc:.0}%)");

    let db = down.load(Ordering::Relaxed);
    print_download_line(db);
    Ok(())
}

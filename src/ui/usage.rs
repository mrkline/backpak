use anyhow::Result;

use log::warn;
use rustc_hash::FxHashSet;

use crate::{backend, file_util::nice_size, index, snapshot, tree};

pub fn run(repository: &camino::Utf8Path) -> Result<()> {
    // Build the usual suspects.
    let (_, cached_backend) = backend::open(repository, backend::CacheBehavior::Normal)?;
    let index = index::build_master_index(&cached_backend)?;
    let blob_map = index::blob_to_pack_map(&index)?;
    let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);
    let size_map = index::blob_to_size_map(&index)?;

    let mut reachable_blobs = FxHashSet::default();
    let mut reachable_size = 0u64;

    // TODO: To display raw sizes; it would be really nice for these functions
    // (snapshot::load_chrono, index::build_master_index, etc.)
    // to return either a hash map of sizes or a total size.
    // That's annoying because we'd have to update all the call sites.
    // But equally dumb would be to backend::list/read files multiple times
    // because we didn't do the accounting first time around.
    // (This is true even if we change backend list to get sizes too.)
    let snapshots = snapshot::load_chronologically(&cached_backend)?;
    if snapshots.is_empty() {
        println!("0 snapshots");
    } else {
        println!(
            "{} snapshots, from {} to {}",
            snapshots.len(),
            snapshots.first().unwrap().0.time.date_naive(),
            snapshots.last().unwrap().0.time.date_naive()
        );
        let mut totals = tree::ForestSizes::default();

        for (snapshot, _snap_id) in &snapshots {
            totals += tree::forest_sizes(
                &tree::forest_from_root(&snapshot.tree, &mut tree_cache)?,
                &size_map,
                &mut reachable_blobs,
            )?;
        }

        // Refactor out of ui/snapshots.rs (into snapshots.rs itself?)
        reachable_size = totals.introduced;
        let u = nice_size(totals.introduced);
        let r = nice_size(totals.reused);
        println!("Snapshots reference {u} unique data, saving {r} by reuse.");
    }

    // Compare all blobs in the index to the reachable_blobs set populated above.
    // If some blobs are unused...
    let num_packs = index.packs.len();
    let pack_size: u64 = index
        .packs
        .values()
        .map(|manifest| manifest.iter().map(|me| me.length as u64).sum::<u64>())
        .sum();
    print!("{num_packs} packs");
    if pack_size > reachable_size {
        let ds = nice_size(pack_size - reachable_size);
        println!(", including {ds} unused data. Consider running `backpak prune`.");
    } else {
        println!();
    }
    if pack_size < reachable_size {
        let ds = nice_size(reachable_size - pack_size);
        warn!("Snapshots contain {ds} more than packs! Consider running `backpak check`.")
    }

    Ok(())
}

use anyhow::Result;

use rustc_hash::FxHashSet;
use tracing::warn;

use crate::{backend, file_util::nice_size, index, snapshot, tree};

pub fn run(repository: &camino::Utf8Path) -> Result<()> {
    // Build the usual suspects.
    let (config, cached_backend) = backend::open(repository, backend::CacheBehavior::Normal)?;
    let (index, index_sizes) = index::build_master_index_with_sizes(&cached_backend)?;
    let blob_map = index::blob_to_pack_map(&index)?;
    let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);
    let size_map = index::blob_to_size_map(&index)?;

    let mut reachable_blobs = FxHashSet::default();
    let reachable_blob_size;

    // TODO: To display raw sizes; it would be really nice for these functions
    // (snapshot::load_chrono, index::build_master_index, etc.)
    // to return either a hash map of sizes or a total size.
    // That's annoying because we'd have to update all the call sites.
    // But equally dumb would be to backend::list/read files multiple times
    // because we didn't do the accounting first time around.
    // (This is true even if we change backend list to get sizes too.)
    let (snapshots, snapshot_size) =
        snapshot::load_chronologically_with_total_size(&cached_backend)?;
    if snapshots.is_empty() {
        reachable_blob_size = 0u64;
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
        reachable_blob_size = totals.introduced;
        let u = nice_size(totals.introduced);
        let r = nice_size(totals.reused);
        println!("{u} unique data");
        println!("{r} reused (deduplicated)");
    }

    let num_indexes = index_sizes.len();
    let index_str = if num_indexes == 1 { "index" } else { "indexes" };
    // Compare all blobs in the index to the reachable_blobs set populated above.
    // If some blobs are unused...
    let reachable_packs = index.packs.len();
    let packed_blob_size: u64 = index
        .packs
        .values()
        .map(|manifest| manifest.iter().map(|me| me.length as u64).sum::<u64>())
        .sum();
    print!("\n{num_indexes} {index_str} reference {reachable_packs} packs");
    if packed_blob_size > reachable_blob_size {
        let ds = nice_size(packed_blob_size - reachable_blob_size);
        println!(", including {ds} unused data.\nConsider running `backpak prune`.");
    } else {
        println!();
    }
    if packed_blob_size < reachable_blob_size {
        let ds = nice_size(reachable_blob_size - packed_blob_size);
        warn!("Snapshots contain {ds} more than packs! Consider running `backpak check`.")
    }
    let pack_size = super::check::warn_on_unreachable_packs(&index, &cached_backend)?;
    let index_size = index_sizes.iter().sum();

    let backend_kind = match config.kind {
        backend::Kind::Filesystem { .. } => "Filesystem",
        backend::Kind::Backblaze { .. } => "Backblaze",
    };
    let filter_str = if let Some(f) = &config.filter {
        let fname = f.split_whitespace().next().expect("empty filter");
        " and ".to_owned() + &fname
    } else {
        String::new()
    };
    println!("\n{backend_kind} usage after zstd compression{filter_str}:");
    println!("snapshots: {}", nice_size(snapshot_size));
    println!("indexes:   {}", nice_size(index_size));
    println!("packs:     {}", nice_size(pack_size));
    #[rustfmt::skip]
    println!("total:     {}", nice_size(pack_size + index_size + snapshot_size) );

    Ok(())
}

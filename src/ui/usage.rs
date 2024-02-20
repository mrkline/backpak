use anyhow::Result;

use crate::{backend, snapshot};

pub fn run(repository: &camino::Utf8Path) -> Result<()> {
    // Build the usual suspects.
    let (_, cached_backend) = backend::open(repository, backend::CacheBehavior::Normal)?;

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
    }
    // Load index, then...

    // Refactor out of ui/snapshots.rs (into snapshots.rs itself?)
    println!("contain <SIZE> unique data, saving <SIZE> by reuse");
    println!("saved in <NUM> packs");

    // Compare all blobs in the index to the visited_blobs set populated above.
    // If some blobs are unused...
    println!("<SIZE> bytes are no longer used, consider running `backpak prune`");

    Ok(())
}

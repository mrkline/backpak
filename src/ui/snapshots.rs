use anyhow::Result;
use clap::Parser;
use rustc_hash::FxHashSet;

use crate::{backend, file_util::nice_size, hashing::ObjectId, index, snapshot, tree};

/// List the snapshots in this repository from oldest to newest
#[derive(Debug, Parser)]
pub struct Args {
    /// Print newest to oldest
    #[clap(short, long)]
    reverse: bool,
}

pub fn run(repository: &camino::Utf8Path, args: Args) -> Result<()> {
    unsafe {
        crate::prettify::prettify_serialize();
    }

    let (_cfg, cached_backend) = backend::open(repository, backend::CacheBehavior::Normal)?;
    let snapshots = snapshot::load_chronologically(&cached_backend)?;
    let index = index::build_master_index(&cached_backend)?;
    let blob_map = index::blob_to_pack_map(&index)?;
    let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);
    let size_map = index::blob_to_size_map(&index)?;

    struct DecoratedSnapshot {
        snapshot: snapshot::Snapshot,
        id: ObjectId,
        sizes: tree::ForestSizes,
    }

    let mut visited_blobs = FxHashSet::default();
    // NB: We collect at the end because our mapping is stateful;
    // we keep track of the visited blobs as we go.
    // (We do *not* want the DoubleEndedIterator from Map!)
    let snaps = snapshots
        .into_iter()
        .map(|(snapshot, id)| {
            let sizes = tree::forest_sizes(
                &tree::forest_from_root(&snapshot.tree, &mut tree_cache)?,
                &size_map,
                &mut visited_blobs,
            )?;
            Ok(DecoratedSnapshot {
                snapshot,
                id,
                sizes,
            })
        })
        .collect::<Vec<_>>();

    let it: Box<dyn Iterator<Item = Result<DecoratedSnapshot>>> = if !args.reverse {
        Box::new(snaps.into_iter())
    } else {
        Box::new(snaps.into_iter().rev())
    };

    for decorated in it {
        let DecoratedSnapshot {
            snapshot,
            id,
            sizes,
        } = decorated?;
        print!("snapshot {}", id);
        if snapshot.tags.is_empty() {
            println!();
        } else {
            println!(
                " ({})",
                snapshot.tags.into_iter().collect::<Vec<String>>().join(" ")
            );
        }
        let t = nice_size(sizes.tree_bytes + sizes.chunk_bytes);
        let m = nice_size(sizes.tree_bytes);
        let c = nice_size(sizes.chunk_bytes);
        let i = nice_size(sizes.introduced);
        let r = nice_size(sizes.reused);
        println!("Sizes: {t} total ({c} files, {m} metadata / {i} new data, {r} reused)");
        println!("Author: {}", snapshot.author);

        println!("Date:   {}", snapshot.time.format("%a %F %H:%M:%S %z"));
        for path in snapshot.paths {
            println!("    - {path}");
        }

        println!();
    }

    Ok(())
}

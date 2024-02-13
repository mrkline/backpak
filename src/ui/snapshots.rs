use anyhow::Result;
use clap::Parser;

use crate::{backend, file_util::nice_size, hashing, index, snapshot, tree};

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

    let it: Box<dyn Iterator<Item = (snapshot::Snapshot, hashing::ObjectId)>> = if !args.reverse {
        Box::new(snapshots.into_iter())
    } else {
        Box::new(snapshots.into_iter().rev())
    };

    for (snapshot, id) in it {
        print!("snapshot {}", id);
        if snapshot.tags.is_empty() {
            println!();
        } else {
            println!(
                " ({})",
                snapshot.tags.into_iter().collect::<Vec<String>>().join(" ")
            );
        }
        let sizes = tree::forest_sizes(
            &tree::forest_from_root(&snapshot.tree, &mut tree_cache)?,
            &size_map,
        )?;
        let total_size = nice_size(sizes.tree_bytes + sizes.chunk_bytes);
        let tree_bytes = nice_size(sizes.tree_bytes);
        let chunk_bytes = nice_size(sizes.chunk_bytes);
        println!("Size: {total_size} ({chunk_bytes} files, {tree_bytes} metadata)");
        println!("Author: {}", snapshot.author);

        println!("Date:   {}", snapshot.time.format("%a %F %H:%M:%S %z"));
        for path in snapshot.paths {
            println!("    - {path}");
        }

        println!();
    }

    Ok(())
}

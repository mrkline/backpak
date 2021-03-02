use std::path::Path;

use anyhow::*;
use log::*;
use structopt::StructOpt;

use crate::backend;
use crate::hashing::ObjectId;
use crate::index;
use crate::snapshot;
use crate::tree;

/// List the files in a snapshot
#[derive(Debug, StructOpt)]
pub struct Args {
    snapshot_prefix: String,
}

pub fn run(repository: &Path, args: Args) -> Result<()> {
    unsafe {
        crate::prettify::prettify_serialize();
    }

    let cached_backend = backend::open(repository)?;
    let (snapshot, id) = snapshot::find_and_load(&args.snapshot_prefix, &cached_backend)?;
    let index = index::build_master_index(&cached_backend)?;
    let blob_map = index::blob_to_pack_map(&index)?;
    let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);

    info!("Listing files for snapshot {}", id);

    let snapshot_tree = tree::forest_from_root(&snapshot.tree, &mut tree_cache)?;
    printer_recursor(&snapshot.tree, &snapshot_tree, 0);

    Ok(())
}

fn printer_recursor(tree_id: &ObjectId, forest: &tree::Forest, level: usize) {
    let next_level = level + 1;
    let current_tree = forest.get(tree_id);
    assert!(
        current_tree.is_some(),
        "Missing tree {} in the forest",
        tree_id
    );

    for (path, node) in current_tree.unwrap().iter() {
        print!("{}{}", " ".repeat(level * 2), path.display());
        match &node.contents {
            tree::NodeContents::Directory { subtree } => {
                println!("/");
                printer_recursor(subtree, forest, next_level);
            }
            tree::NodeContents::File { .. } => {
                println!();
            }
        };
    }
}

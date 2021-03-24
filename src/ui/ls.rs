use std::path::{Path, PathBuf};

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
    printer_recursor(&snapshot.tree, &snapshot_tree, &Path::new(""));

    Ok(())
}

fn printer_recursor(tree_id: &ObjectId, forest: &tree::Forest, prefix: &Path) {
    let tree: &tree::Tree = forest
        .get(tree_id)
        .ok_or_else(|| anyhow!("Missing tree {}", tree_id))
        .unwrap();

    for (path, node) in tree {
        if !prefix.as_os_str().is_empty() {
            print!("{}{}", prefix.display(), std::path::MAIN_SEPARATOR);
        }
        print!("{}", path.display());
        match &node.contents {
            tree::NodeContents::Directory { subtree } => {
                println!("{}", std::path::MAIN_SEPARATOR);
                let mut new_prefix: PathBuf = prefix.to_owned();
                new_prefix.push(path);
                printer_recursor(subtree, forest, &new_prefix);
            }
            tree::NodeContents::File { .. } => {
                println!();
            }
        };
    }
}

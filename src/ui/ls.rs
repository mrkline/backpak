use std::path::Path;

use anyhow::*;
use log::*;
use structopt::StructOpt;

use crate::backend;
use crate::index;
use crate::ls;
use crate::snapshot;
use crate::tree;

/// List the files in a snapshot
#[derive(Debug, StructOpt)]
pub struct Args {
    snapshot_prefix: String,
}

pub fn run(repository: &Path, args: Args) -> Result<()> {
    let cached_backend = backend::open(repository)?;
    let (snapshot, id) = snapshot::find_and_load(&args.snapshot_prefix, &cached_backend)?;
    let index = index::build_master_index(&cached_backend)?;
    let blob_map = index::blob_to_pack_map(&index)?;
    let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);

    info!("Listing files for snapshot {}", id);

    let snapshot_tree = tree::forest_from_root(&snapshot.tree, &mut tree_cache)?;
    ls::print_tree("", Path::new(""), &snapshot.tree, &snapshot_tree);

    Ok(())
}

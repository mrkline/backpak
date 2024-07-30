use anyhow::Result;
use camino::Utf8Path;
use clap::Parser;
use tracing::*;

use crate::backend;
use crate::index;
use crate::ls;
use crate::snapshot;
use crate::tree;

/// List the files in a snapshot
#[derive(Debug, Parser)]
pub struct Args {
    snapshot: String,
}

pub fn run(repository: &Utf8Path, args: Args) -> Result<()> {
    let (_cfg, cached_backend) = backend::open(repository, backend::CacheBehavior::Normal)?;
    let (snapshot, id) = snapshot::find_and_load(&args.snapshot, &cached_backend)?;
    let index = index::build_master_index(&cached_backend)?;
    let blob_map = index::blob_to_pack_map(&index)?;
    let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);

    info!("Listing files for snapshot {}", id);

    let snapshot_tree = tree::forest_from_root(&snapshot.tree, &mut tree_cache)?;
    ls::print_tree("", Utf8Path::new(""), &snapshot.tree, &snapshot_tree);

    Ok(())
}

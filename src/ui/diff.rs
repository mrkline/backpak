use std::collections::BTreeSet;
use std::path::Path;

use anyhow::*;
use log::*;
use structopt::StructOpt;

use crate::backend;
use crate::fs_tree;
use crate::hashing::ObjectId;
use crate::index;
use crate::ls;
use crate::snapshot;
use crate::tree;

/// Compare two snapshots
///
/// TODO: Compare snapshots to a path!
#[derive(Debug, StructOpt)]
pub struct Args {
    first_snapshot: String,
    second_snapshot: Option<String>,
}

pub fn run(repository: &Path, args: Args) -> Result<()> {
    let cached_backend = backend::open(repository)?;
    let index = index::build_master_index(&cached_backend)?;
    let blob_map = index::blob_to_pack_map(&index)?;
    let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);

    let (snapshot1, id1) = snapshot::find_and_load(&args.first_snapshot, &cached_backend)?;
    let snapshot1_forest = tree::forest_from_root(&snapshot1.tree, &mut tree_cache)?;

    let (id2, forest2) = load_snapshot2_or_paths(
        &id1,
        &snapshot1,
        &snapshot1_forest,
        &args.second_snapshot,
        &cached_backend,
        &mut tree_cache,
    )?;

    compare_trees(
        (&snapshot1.tree, &snapshot1_forest),
        (&id2, &forest2),
        Path::new(""),
    );

    Ok(())
}

fn load_snapshot2_or_paths(
    id1: &ObjectId,
    snapshot1: &snapshot::Snapshot,
    snapshot1_forest: &tree::Forest,
    second_snapshot: &Option<String>,
    cached_backend: &backend::CachedBackend,
    tree_cache: &mut tree::Cache,
) -> Result<(ObjectId, tree::Forest)> {
    if let Some(second_snapshot) = second_snapshot {
        let (snapshot2, id2) = snapshot::find_and_load(&second_snapshot, &cached_backend)?;
        let snapshot2_forest = tree::forest_from_root(&snapshot2.tree, tree_cache)?;

        info!("Comparing snapshot {} to {}", id1, id2);

        Ok((snapshot2.tree, snapshot2_forest))
    } else {
        info!(
            "Comparing snapshot {} to its paths, {:?}",
            id1, snapshot1.paths
        );
        fs_tree::forest_from_fs(&snapshot1.paths, Some(&snapshot1.tree), snapshot1_forest)
    }
}

fn compare_trees(
    (id1, forest1): (&ObjectId, &tree::Forest),
    (id2, forest2): (&ObjectId, &tree::Forest),
    tree_path: &Path,
) {
    let tree1: &tree::Tree = forest1
        .get(id1)
        .ok_or_else(|| anyhow!("Missing tree {}", id1))
        .unwrap();

    let tree2: &tree::Tree = forest2
        .get(id2)
        .ok_or_else(|| anyhow!("Missing tree {}", id2))
        .unwrap();

    let all_paths = tree1.keys().chain(tree2.keys()).collect::<BTreeSet<_>>();
    for path in all_paths {
        let mut node_path = tree_path.to_owned();
        node_path.push(path);
        match (tree1.get(path), tree2.get(path)) {
            (None, None) => unreachable!(),
            (None, Some(new_node)) => ls::print_node("+ ", &node_path, new_node, forest2),
            (Some(old_node), None) => ls::print_node("- ", &node_path, old_node, forest1),
            (Some(l), Some(r)) => {
                compare_nodes((l, forest1), (r, forest2), &node_path);
            }
        };
    }
}

pub fn compare_nodes(
    (node1, forest1): (&tree::Node, &tree::Forest),
    (node2, forest2): (&tree::Node, &tree::Forest),
    path: &Path,
) {
    match (node1.is_directory(), node2.is_directory()) {
        (false, false) => {
            // Both are files.
            if node1.contents != node2.contents {
                ls::print_node("M ", path, node1, &tree::Forest::new());
            } else if node1.metadata != node2.metadata {
                // TODO: atime is being a PITA. Do we want it?
                // Should we ignore it for diffing purposes?
                // trace!("{:#?} != {:#?}", node1.metadata, node2.metadata);
                ls::print_node("U ", path, node1, &tree::Forest::new());
            }
        }
        (true, true) => {
            // Both are directories
            compare_trees(
                (node1.contents.subtree(), forest1),
                (node2.contents.subtree(), forest2),
                path,
            );
        }
        _ => {
            // If we changed from file to directory or directory to file,
            // just - the old and + the new
            ls::print_node("- ", &path, node1, forest1);
            ls::print_node("+ ", &path, node2, forest2);
        }
    }
}

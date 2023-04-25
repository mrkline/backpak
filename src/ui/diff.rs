use anyhow::*;
use camino::Utf8Path;
use clap::Parser;
use log::*;

use crate::backend;
use crate::diff;
use crate::fs_tree;
use crate::hashing::ObjectId;
use crate::index;
use crate::ls;
use crate::snapshot;
use crate::tree::{self, Forest, Node, NodeType};

/// Compare two snapshots, or a snapshot to the current tree
#[derive(Debug, Parser)]
pub struct Args {
    #[clap(short, long)]
    metadata: bool,

    first_snapshot: String,
    second_snapshot: Option<String>,
}

pub fn run(repository: &Utf8Path, args: Args) -> Result<()> {
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

    diff::compare_trees(
        (&snapshot1.tree, &snapshot1_forest),
        (&id2, &forest2),
        Utf8Path::new(""),
        &mut PrintDiffs {
            metadata: args.metadata,
        },
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
        let (snapshot2, id2) = snapshot::find_and_load(second_snapshot, cached_backend)?;
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

#[derive(Debug, Default)]
pub struct PrintDiffs {
    pub metadata: bool,
}

impl diff::Callbacks for PrintDiffs {
    fn node_added(&mut self, node_path: &Utf8Path, new_node: &Node, forest: &Forest) -> Result<()> {
        ls::print_node("+ ", node_path, new_node, ls::Recurse::Yes(forest));
        Ok(())
    }

    fn node_removed(
        &mut self,
        node_path: &Utf8Path,
        old_node: &Node,
        forest: &Forest,
    ) -> Result<()> {
        ls::print_node("- ", node_path, old_node, ls::Recurse::Yes(forest));
        Ok(())
    }

    fn contents_changed(
        &mut self,
        node_path: &Utf8Path,
        old_node: &Node,
        new_node: &Node,
    ) -> Result<()> {
        assert!(old_node.kind() == NodeType::File || old_node.kind() == NodeType::Symlink);
        assert_eq!(old_node.kind(), new_node.kind());

        if old_node.kind() == NodeType::Symlink {
            ls::print_node("- ", node_path, old_node, ls::Recurse::No);
            ls::print_node("+ ", node_path, new_node, ls::Recurse::No);
        } else {
            ls::print_node("M ", node_path, old_node, ls::Recurse::No);
        }
        Ok(())
    }

    fn metadata_changed(&mut self, node_path: &Utf8Path, node: &Node) -> Result<()> {
        if self.metadata {
            ls::print_node("U ", node_path, node, ls::Recurse::No);
        }
        Ok(())
    }
}

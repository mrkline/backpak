use std::path::{Path, PathBuf};

use anyhow::*;
use log::*;
use structopt::StructOpt;

use crate::backend;
use crate::diff;
use crate::fs_tree;
use crate::hashing::ObjectId;
use crate::index;
use crate::ls;
use crate::snapshot;
use crate::tree::{self, Forest, Node, NodeType};

/// Compare two snapshots
#[derive(Debug, StructOpt)]
pub struct Args {
    #[structopt(short, long)]
    output: Option<PathBuf>,

    #[structopt(short = "n", long)]
    dry_run: bool,

    #[structopt(short, long)]
    delete: bool,

    #[structopt(short, long)]
    times: bool,

    #[structopt(short = "U", long)]
    atimes: bool,

    restore_from: String,
}

pub fn run(repository: &Path, args: Args) -> Result<()> {
    let cached_backend = backend::open(repository)?;
    let index = index::build_master_index(&cached_backend)?;
    let blob_map = index::blob_to_pack_map(&index)?;
    let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);

    let (snapshot, id) = snapshot::find_and_load(&args.restore_from, &cached_backend)?;
    let snapshot_forest = tree::forest_from_root(&snapshot.tree, &mut tree_cache)?;

    let (fs_id, fs_forest) = load_fs_tree(&id, &snapshot, &snapshot_forest, &args.output)?;

    let mut res = Restorer {
        dry_run: args.dry_run,
        delete: args.delete,
        restore_times: args.times,
        restore_atimes: args.atimes,
        // Do we need to map top-level dirs to snapshot.paths when output is None,
        // so that we can restore to each source folder?
    };

    // The filesystem tree is the "older" one,
    // since the backup is the desired end state.
    diff::compare_trees(
        (&fs_id, &fs_forest),
        (&snapshot.tree, &snapshot_forest),
        Path::new(""),
        &mut res,
    );

    Ok(())
}

fn load_fs_tree(
    id: &ObjectId,
    snapshot: &snapshot::Snapshot,
    snapshot_forest: &tree::Forest,
    restore_to: &Option<PathBuf>,
) -> Result<(ObjectId, tree::Forest)> {
    if let Some(to) = restore_to {
        info!("Comparing snapshot {} to {}", id, to.display());

        let paths = snapshot
            .paths
            .iter()
            .map(|p| {
                let mut root: PathBuf = to.clone();
                root.push(p.file_name().unwrap());
                root
            })
            .filter(|p| p.exists())
            .collect();

        fs_tree::forest_from_fs(&paths, Some(&snapshot.tree), snapshot_forest)
    } else {
        info!(
            "Restoring snapshot {} to its paths, {:?}",
            id, snapshot.paths
        );
        fs_tree::forest_from_fs(&snapshot.paths, Some(&snapshot.tree), snapshot_forest)
    }
}

#[derive(Debug)]
struct Restorer {
    dry_run: bool,
    delete: bool,
    restore_times: bool,
    restore_atimes: bool,
}

impl diff::Callbacks for Restorer {
    fn node_added(&mut self, node_path: &Path, new_node: &Node, forest: &Forest) -> Result<()> {
        ls::print_node("+ ", &node_path, new_node, ls::Recurse::Yes(forest));

        if self.dry_run {
            return Ok(());
        }
        Ok(())
    }

    fn node_removed(&mut self, node_path: &Path, old_node: &Node, forest: &Forest) -> Result<()> {
        if !self.delete {
            return Ok(());
        }
        ls::print_node("- ", node_path, old_node, ls::Recurse::Yes(forest));

        if self.dry_run {
            return Ok(());
        }
        Ok(())
    }

    fn contents_changed(
        &mut self,
        node_path: &Path,
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

        if self.dry_run {
            return Ok(());
        }
        self.set_metadata(node_path, new_node)
    }

    fn metadata_changed(&mut self, node_path: &Path, node: &Node) -> Result<()> {
        ls::print_node("U ", node_path, node, ls::Recurse::No);

        if self.dry_run {
            return Ok(());
        }
        self.set_metadata(node_path, node)
    }

    fn type_changed(
        &mut self,
        node_path: &Path,
        old_node: &Node,
        old_forest: &Forest,
        new_node: &Node,
        new_forest: &Forest,
    ) -> Result<()> {
        ls::print_node("- ", &node_path, old_node, ls::Recurse::Yes(old_forest));
        ls::print_node("+ ", &node_path, new_node, ls::Recurse::Yes(new_forest));

        if self.dry_run {
            return Ok(());
        }

        // rsync will remove empty directories to replace them with a file,
        // but without --delete will refuse to nuke a directory.

        Ok(())
    }
}

impl Restorer {
    fn set_metadata(&self, _node_path: &Path, _node: &Node) -> Result<()> {
        if self.restore_times {
            todo!();
        }
        if self.restore_atimes {
            todo!();
        }
        Ok(())
    }
}

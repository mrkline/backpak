use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::*;
use log::*;
use structopt::StructOpt;

use crate::backend;
use crate::diff;
use crate::fs_tree;
use crate::hashing::ObjectId;
use crate::index;
use crate::snapshot;
use crate::tree::{self, Forest, Node, NodeType};

/// Compare two snapshots
#[derive(Debug, StructOpt)]
pub struct Args {
    #[structopt(short, long)]
    output: Option<PathBuf>,

    #[structopt(short = "n", long)]
    dry_run: bool,

    // Args based on rsync's
    #[structopt(short, long)]
    delete: bool,

    #[structopt(short, long)]
    times: bool,

    #[structopt(short = "U", long)]
    atimes: bool,

    #[structopt(short, long)]
    permissions: bool,

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

    let metadata = args.times || args.atimes || args.permissions;

    let mut res = Restorer {
        printer: super::diff::PrintDiffs { metadata },
        args: &args,
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
struct Restorer<'a> {
    printer: super::diff::PrintDiffs,
    args: &'a Args,
}

impl<'a> diff::Callbacks for Restorer<'a> {
    fn node_added(&mut self, node_path: &Path, new_node: &Node, forest: &Forest) -> Result<()> {
        self.printer.node_added(node_path, new_node, forest)?;

        if self.args.dry_run {
            return Ok(());
        }
        Ok(())
    }

    fn node_removed(&mut self, node_path: &Path, old_node: &Node, forest: &Forest) -> Result<()> {
        if !self.args.delete {
            return Ok(());
        }
        self.printer.node_removed(node_path, old_node, forest)?;

        if self.args.dry_run {
            return Ok(());
        }
        if old_node.kind() == NodeType::Directory {
            fs::remove_dir(node_path)?
        }
        Ok(())
    }

    fn contents_changed(
        &mut self,
        node_path: &Path,
        old_node: &Node,
        new_node: &Node,
    ) -> Result<()> {
        self.printer
            .contents_changed(node_path, old_node, new_node)?;

        if self.args.dry_run {
            return Ok(());
        }
        self.set_metadata(node_path, new_node)
    }

    fn metadata_changed(&mut self, node_path: &Path, node: &Node) -> Result<()> {
        self.printer.metadata_changed(node_path, node)?;

        if self.args.dry_run {
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
        self.printer
            .type_changed(node_path, old_node, old_forest, new_node, new_forest)?;

        if self.args.dry_run {
            return Ok(());
        }

        // rsync will remove empty directories to replace them with a file,
        // but without --delete will refuse to nuke a directory.

        Ok(())
    }
}

impl<'a> Restorer<'a> {
    fn set_metadata(&self, _node_path: &Path, _node: &Node) -> Result<()> {
        if self.args.times {
            todo!();
        }
        if self.args.atimes {
            todo!();
        }
        Ok(())
    }
}

use std::{
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;
use log::*;
use rustc_hash::FxHashMap;
use structopt::StructOpt;

use crate::{
    backend, diff, fs_tree,
    hashing::ObjectId,
    index, snapshot,
    tree::{self, Forest, Node, NodeType},
};

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

    let tree_and_mapping =
        load_fs_tree_and_mapping(&id, &snapshot, &snapshot_forest, &args.output)?;

    let metadata = args.times || args.atimes || args.permissions;

    let mut res = Restorer {
        printer: super::diff::PrintDiffs { metadata },
        path_map: tree_and_mapping.path_map,
        args: &args,
    };

    // The filesystem tree is the "older" one,
    // since the backup is the desired end state.
    diff::compare_trees(
        (&tree_and_mapping.fs_id, &tree_and_mapping.fs_forest),
        (&snapshot.tree, &snapshot_forest),
        Path::new(""),
        &mut res,
    );

    Ok(())
}

struct FsTreeAndMapping<'a> {
    fs_id: ObjectId,
    fs_forest: tree::Forest,
    path_map: FxHashMap<&'a OsStr, PathBuf>,
}

fn load_fs_tree_and_mapping<'a>(
    id: &ObjectId,
    snapshot: &'a snapshot::Snapshot,
    snapshot_forest: &tree::Forest,
    restore_to: &Option<PathBuf>,
) -> Result<FsTreeAndMapping<'a>> {
    let mut path_map =
        FxHashMap::with_capacity_and_hasher(snapshot.paths.len(), Default::default());

    if let Some(to) = restore_to {
        info!("Comparing snapshot {} to {}", id, to.display());

        let paths = snapshot
            .paths
            .iter()
            .map(|p| to.join(p.file_name().unwrap()))
            .filter(|p| p.exists())
            .collect();

        let (fs_id, fs_forest) =
            fs_tree::forest_from_fs(&paths, Some(&snapshot.tree), snapshot_forest)?;

        for path in &snapshot.paths {
            let last_dir = path.file_name().unwrap();
            let to = to.join(last_dir);
            assert!(path_map.insert(last_dir, to).is_none());
        }

        Ok(FsTreeAndMapping {
            fs_id,
            fs_forest,
            path_map,
        })
    } else {
        info!(
            "Restoring snapshot {} to its paths, {:?}",
            id, snapshot.paths
        );
        let (fs_id, fs_forest) =
            fs_tree::forest_from_fs(&snapshot.paths, Some(&snapshot.tree), snapshot_forest)?;
        for path in &snapshot.paths {
            assert!(path_map
                .insert(path.file_name().unwrap(), path.clone())
                .is_none());
        }

        Ok(FsTreeAndMapping {
            fs_id,
            fs_forest,
            path_map,
        })
    }
}

#[derive(Debug)]
struct Restorer<'a> {
    printer: super::diff::PrintDiffs,
    path_map: FxHashMap<&'a OsStr, PathBuf>,
    args: &'a Args,
}

impl Restorer<'_> {
    fn translate_path(&self, node_path: &Path) -> PathBuf {
        let first_component = node_path.iter().next().unwrap();
        self.path_map
            .get(first_component)
            .unwrap()
            .join(node_path.strip_prefix(first_component).unwrap())
    }
}

impl diff::Callbacks for Restorer<'_> {
    fn node_added(&mut self, node_path: &Path, new_node: &Node, forest: &Forest) -> Result<()> {
        let node_path = self.translate_path(node_path);

        self.printer.node_added(&node_path, new_node, forest)?;

        if self.args.dry_run {
            return Ok(());
        }
        Ok(())
    }

    fn node_removed(&mut self, node_path: &Path, old_node: &Node, forest: &Forest) -> Result<()> {
        if !self.args.delete {
            return Ok(());
        }

        let node_path = self.translate_path(node_path);

        self.printer.node_removed(&node_path, old_node, forest)?;

        if self.args.dry_run {
            return Ok(());
        }
        if old_node.kind() == NodeType::Directory {
            fs::remove_dir(&node_path)?
        }
        Ok(())
    }

    fn contents_changed(
        &mut self,
        node_path: &Path,
        old_node: &Node,
        new_node: &Node,
    ) -> Result<()> {
        let node_path = self.translate_path(node_path);

        self.printer
            .contents_changed(&node_path, old_node, new_node)?;

        if self.args.dry_run {
            return Ok(());
        }
        self.set_metadata(&node_path, new_node)
    }

    fn metadata_changed(&mut self, node_path: &Path, node: &Node) -> Result<()> {
        let node_path = self.translate_path(node_path);

        self.printer.metadata_changed(&node_path, node)?;

        if self.args.dry_run {
            return Ok(());
        }
        self.set_metadata(&node_path, node)
    }

    fn type_changed(
        &mut self,
        node_path: &Path,
        old_node: &Node,
        old_forest: &Forest,
        new_node: &Node,
        new_forest: &Forest,
    ) -> Result<()> {
        let node_path = self.translate_path(node_path);

        self.printer
            .type_changed(&node_path, old_node, old_forest, new_node, new_forest)?;

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

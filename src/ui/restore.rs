use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::prelude::*,
    sync::Arc,
};

use anyhow::{anyhow, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use chrono::prelude::*;
use clap::Parser;
use rustc_hash::FxHashMap;
use rustix::fs::Timespec;
use tracing::*;

use crate::{
    backend, diff, fs_tree,
    hashing::ObjectId,
    index,
    read::BlobReader,
    snapshot,
    tree::{self, Forest, Node, NodeContents, NodeMetadata, NodeType, Tree},
};

/// Restore the given snapshot to the filesystem
///
/// Prints changes made using the same codes as the `diff` command:
/// + added/file/or/dir
/// - removed
/// C contents changed
/// O ownership changed
/// P permissions changed
/// T modify time changed
/// A access time changed
/// M other metadata changed
///
/// Type changes (e.g. dir -> file, or file -> symlink)
/// are modeled as removing one and adding the other.
/// Same goes for symlinks so we can show
///   - some/symlink -> previous/target
///   + some/symlink -> new/target
#[derive(Debug, Parser)]
#[command(verbatim_doc_comment)]
#[allow(clippy::doc_lazy_continuation)] // It's a verbatim doc comment, shut up Clippy.
pub struct Args {
    /// Restore the snapshot to the given directory
    /// instead of the absolute path(s) in the snapshot.
    ///
    /// A snapshot of a single directory, it is mapped directly:
    /// one of `/foo/bar/baz` restored with `--output ./bob`
    /// will restore baz's contents into `./bob`
    ///
    /// If the snapshot consists of multiple directories,
    /// they are dumped into the output:
    /// one of `/home/me/src/backpak` and `/home/me/src/mcap`
    /// restored with `--output /tmp` will restore to
    /// `/tmp/backpak` and `/tmp/mcap`
    ///
    /// Both assumes the output dir already exists;
    /// it does not create it.
    ///
    /// More sophisticated options, e.g.,
    /// specifying multiple snapshot dir -> output dir mappings,
    /// could be added in the future.
    #[clap(short, long, verbatim_doc_comment)]
    output: Option<Utf8PathBuf>,

    #[clap(short = 'n', long)]
    dry_run: bool,

    /// Delete files not contained in the snapshot
    ///
    /// This includes deleting some directory `foo/` and all its contents
    /// to replace it with some file `foo` in the snapshot.
    /// (Without this flag, `foo/` will be left alone
    /// and `foo` in the snapshot will be ignored.)
    #[clap(short, long, verbatim_doc_comment)]
    delete: bool,

    /// Restore modification and access times
    #[clap(short, long)]
    times: bool,

    /// Restore file permissions
    #[clap(short, long)]
    permissions: bool,

    #[clap(name = "SNAPSHOT")]
    restore_from: String,
}

pub fn run(repository: &Utf8Path, args: Args) -> Result<()> {
    let (_cfg, cached_backend) = backend::open(repository, backend::CacheBehavior::Normal)?;
    let index = index::build_master_index(&cached_backend)?;
    let blob_map = index::blob_to_pack_map(&index)?;

    let (snapshot, id) = snapshot::find_and_load(&args.restore_from, &cached_backend)?;
    let snapshot_forest = tree::forest_from_root(
        &snapshot.tree,
        &mut tree::Cache::new(&index, &blob_map, &cached_backend),
    )?;

    let tree_and_mapping =
        load_fs_tree_and_mapping(&id, &snapshot, &snapshot_forest, &args.output)?;

    let metadata = args.times || args.permissions;

    let mut res = Restorer {
        printer: super::diff::PrintDiffs { metadata },
        path_map: tree_and_mapping.path_map,
        blob_reader: BlobReader::new(&cached_backend, &index, &blob_map),
        args: &args,
    };

    // The filesystem tree is the "older" one,
    // since the backup is the desired end state.
    diff::compare_trees(
        (&tree_and_mapping.fs_id, &tree_and_mapping.fs_forest),
        (&snapshot.tree, &snapshot_forest),
        Utf8Path::new(""),
        &mut res,
    )
}

struct FsTreeAndMapping<'a> {
    fs_id: ObjectId,
    fs_forest: tree::Forest,
    /// Trees in backpak have a name with a single component - If I back up /home/me and /etc/,
    /// my top-level tree is just `{ "me" -> subtree, "etc" -> subtree }`.
    /// (See [`Snapshot`](crate::snapshot::Snapshot) for a discussion on this.)
    ///
    /// This remaps those top-level nodes to their absolute paths.
    path_map: FxHashMap<&'a str, Utf8PathBuf>,
}

fn load_fs_tree_and_mapping<'a>(
    id: &ObjectId,
    snapshot: &'a snapshot::Snapshot,
    snapshot_forest: &tree::Forest,
    restore_to: &Option<Utf8PathBuf>,
) -> Result<FsTreeAndMapping<'a>> {
    let mut path_map =
        FxHashMap::with_capacity_and_hasher(snapshot.paths.len(), Default::default());

    if let Some(to) = restore_to {
        info!("Comparing snapshot {id} to {to}");

        // See the --help doc above: If the snapshot is a single directory,
        // map it directoy to `<DIR>` in `--output <DIR>`
        let (fs_id, fs_forest) = if snapshot.paths.len() == 1 {
            // Map the last component of the snapshot dir
            // (i.e., the one entry in our top-level tree) to the output dir.
            let last_dir = snapshot.paths.iter().next().unwrap().file_name().unwrap();
            assert!(path_map.insert(last_dir, to.clone()).is_none());

            // We need to canonicalize the path since the user might have specified "./"
            // or ".." or "./foo/bar/.." or anything else that might trip up calling file_name()
            // to get the last component in forest_from_fs().
            let canonical_to = to
                .canonicalize_utf8()
                .with_context(|| format!("Couldn't canonicalize {to}"))?;

            let (fs_id, mut fs_forest) = fs_tree::forest_from_fs(
                // NB: We do *NOT* want to dereference symbolic links when we're
                // building our current understanding of the filesystem - if it's a symlink now
                // and it's something else in the backup
                // (either because we backed up with -L or contents actually changed),
                // we want to axe that symlink and replace it with our backup's content.
                // This matches the behavior of `cp -L` (which only derefs symlinks in the source)
                // and `rsync -L` without `-K`.
                //
                // Read the rsync man page, weep at the complexity of -K,
                // and behold all its warnings about using that flag.
                // tl;dr:
                //
                // 1. The directory I'm restoring to has a symlink `foo -> /etc/`
                //
                // 2. My snapshot has a directory named `foo` with arbitrary contents.
                //
                // 3. Restoring my snapshot while following destination symlinks
                //    nukes /etc/ and paves it over with foo's contents.
                //
                // 4. ????
                //
                // 5. Great Sorrow
                //
                // Let's not mess with that.
                tree::Symlink::Read,
                &BTreeSet::from([canonical_to.clone()]),
                Some(&snapshot.tree),
                snapshot_forest,
            )?;

            // Fix up the forest so its top-level tree name matches the snapshot's.
            // (This lets them compare cleanly.)
            // Say we have a snapshot of `/home/me` and we're restoring to `./you`:

            // Remove the previous top-level tree from the `./you` forest.
            let fs_top = fs_forest.remove(&fs_id).unwrap();
            // Make a copy, replacing { "you" -> subtree } with { "me" -> subtree }
            // so it matches the snapshot.
            let mut fixed_top: Tree = (*fs_top).clone();
            let node = fixed_top
                .remove(Utf8Path::new(canonical_to.file_name().unwrap()))
                .unwrap();
            fixed_top.insert(last_dir.into(), node);
            // Put that into the forest and return its ID as the new top-level tree.
            let (_bytes, fixed_id) = tree::serialize_and_hash(&fixed_top)?;
            fs_forest.insert(fixed_id, Arc::new(fixed_top));

            (fixed_id, fs_forest)
        }
        // If the snapshot is multiple directories,
        // map them as subdirectories of `<DIR>` in `--output <DIR>`.
        else {
            // Map the last components of the snapshot dirs (i.e., our top-level subtree names)
            // to subdirectories of the output dir.
            for path in &snapshot.paths {
                let last_dir = path.file_name().unwrap();
                let to = to.join(last_dir);
                assert!(path_map.insert(last_dir, to).is_none());
            }

            // If subdirectories of the output directory match our snapshot dir names,
            // walk those.
            // Here we don't need to canonicalize (unlike above) since we're adding a component.
            let paths = snapshot
                .paths
                .iter()
                .map(|p| to.join(p.file_name().unwrap()))
                .filter(|p| p.exists())
                .collect();

            fs_tree::forest_from_fs(
                tree::Symlink::Read, // See above
                &paths,
                Some(&snapshot.tree),
                snapshot_forest,
            )?
        };

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
        let (fs_id, fs_forest) = fs_tree::forest_from_fs(
            tree::Symlink::Read, // See above
            &snapshot.paths,
            Some(&snapshot.tree),
            snapshot_forest,
        )?;
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

#[cfg(unix)]
fn to_timespec(c: DateTime<Utc>) -> Timespec {
    Timespec {
        tv_sec: c.timestamp(),
        tv_nsec: c.timestamp_subsec_nanos().into(),
    }
}

struct Restorer<'a> {
    printer: super::diff::PrintDiffs,
    path_map: FxHashMap<&'a str, Utf8PathBuf>,
    blob_reader: BlobReader<'a>,
    args: &'a Args,
}

impl Restorer<'_> {
    fn translate_path(&self, node_path: &Utf8Path) -> Utf8PathBuf {
        let first_component = node_path.iter().next().unwrap();
        self.path_map
            .get(first_component)
            .unwrap_or_else(|| panic!("No key {first_component} in path map {:?}", self.path_map))
            .join(node_path.strip_prefix(first_component).unwrap())
    }

    // NB: node_path is already translated for all of thse

    #[cfg(unix)]
    fn set_metadata(&self, node_path: &Utf8Path, node: &Node) -> Result<()> {
        let mtime = node.metadata.modification_time();
        let atime = node.metadata.access_time();

        if self.args.times {
            if mtime.is_none() && atime.is_none() {
                trace!("--times given but {node_path} has no time metadata");
            } else {
                let atime = atime.unwrap_or_else(Utc::now);
                let mtime = mtime.unwrap_or_else(Utc::now);
                trace!("setting timestamps for {node_path}");
                // trace!("    atime: {:?}", atime);
                // trace!("    tmtime: {:?}", mtime);
                use rustix::fs::*;
                let stamps = Timestamps {
                    last_access: to_timespec(atime),
                    last_modification: to_timespec(mtime),
                };
                utimensat(CWD, node_path.as_str(), &stamps, AtFlags::SYMLINK_NOFOLLOW)
                    .with_context(|| format!("Couldn't set timestamps for {node_path}"))?;
            }
        }
        // chmod is unsupported on Linux symlinks (without dereferencing). The more you know.
        if self.args.permissions && node.kind() != tree::NodeType::Symlink {
            use std::os::unix::fs::PermissionsExt;
            let permissions = match &node.metadata {
                NodeMetadata::Posix(p) => fs::Permissions::from_mode(p.mode),
                NodeMetadata::Windows(_w) => todo!("Windows -> Posix perms mapping"),
            };
            trace!("chmod {:o} {node_path}", permissions.mode());
            fs::set_permissions(node_path, permissions)
                .with_context(|| format!("Couldn't chmod {node_path}"))?;
        }
        Ok(())
    }

    #[cfg(windows)]
    fn set_metadata(&self, _node_path: &Utf8Path, _node: &Node) -> Result<()> {
        todo!("lol windows metadata");
    }

    fn remove_node(&mut self, node_path: &Utf8Path, old_node: &Node) -> Result<()> {
        if old_node.kind() == NodeType::Directory {
            trace!("Removing whole dir {node_path}");
            fs::remove_dir_all(node_path)?;
        } else {
            trace!("Removing {node_path}");
            // NB: Because we model node type changing as "remove old, add new"
            // (see the defaulted type_changed() callback in diff.rs),
            // destination symlinks are *NOT* followed.
            // See the comments on forest_from_fs() above.
            fs::remove_file(node_path)?;
        }
        Ok(())
    }

    fn add_node(&mut self, node_path: &Utf8Path, new_node: &Node, forest: &Forest) -> Result<()> {
        match &new_node.contents {
            NodeContents::File { .. } => {
                let fh = File::create(node_path)
                    .with_context(|| format!("Couldn't create file {node_path}"))?;
                fill_file(fh, new_node, &mut self.blob_reader)?;
            }
            NodeContents::Symlink { target } => {
                symlink(target, node_path)?;
            }
            NodeContents::Directory { subtree } => {
                fs::create_dir(node_path)
                    .with_context(|| format!("Couldn't create dir {node_path}"))?;

                let subtree: &tree::Tree = forest
                    .get(subtree)
                    .ok_or_else(|| anyhow!("Missing tree {subtree}"))
                    .unwrap();

                for (path, child_node) in subtree {
                    let mut child_path = node_path.to_owned();
                    child_path.push(path);
                    self.add_node(&child_path, child_node, forest)?;
                }
            }
        };
        if !matches!(&new_node.contents, NodeContents::Symlink { .. }) {
            self.set_metadata(node_path, new_node)?;
        }
        Ok(())
    }

    fn change_node_contents(
        &mut self,
        node_path: &Utf8Path,
        _old_node: &Node,
        new_node: &Node,
    ) -> Result<()> {
        match &new_node.contents {
            NodeContents::File { .. } => {
                let fh = File::create(node_path)
                    .with_context(|| format!("Couldn't create file {node_path}"))?;
                fill_file(fh, new_node, &mut self.blob_reader)?;

                // Don't try to set metadata on a symlink! We can't lol
                self.set_metadata(node_path, new_node)?;
            }
            NodeContents::Symlink { target } => {
                fs::remove_file(node_path)
                    .with_context(|| format!("Couldn't remove previous symlink at {node_path}"))?;
                symlink(target, node_path)?;
            }
            NodeContents::Directory { .. } => {
                // This callback isn't called on directories
                unreachable!();
            }
        };
        Ok(())
    }
}

fn fill_file(mut fh: File, node: &Node, bl: &mut BlobReader<'_>) -> Result<()> {
    let chunks = node.contents.chunks();
    for c in chunks {
        fh.write_all(&bl.read_blob(c)?)?;
    }
    Ok(())
}

#[cfg(windows)]
fn symlink(_target: &Utf8Path, _from: &Utf8Path) -> Result<()> {
    // Uhh, we need to figure out if it's a directory?
    // This is likely to fail without elevated perms?
    // https://doc.rust-lang.org/std/os/windows/fs/fn.symlink_file.html
    todo!("Windows symlink creation is tricky");
}

#[cfg(unix)]
fn symlink(target: &Utf8Path, from: &Utf8Path) -> Result<()> {
    std::os::unix::fs::symlink(target, from)
        .with_context(|| format!("Couldn't create symlink {from}"))?;
    Ok(())
}

impl diff::Callbacks for Restorer<'_> {
    fn node_added(&mut self, node_path: &Utf8Path, new_node: &Node, forest: &Forest) -> Result<()> {
        let node_path = self.translate_path(node_path);

        self.printer.node_added(&node_path, new_node, forest)?;

        if self.args.dry_run {
            Ok(())
        } else {
            self.add_node(&node_path, new_node, forest)
        }
    }

    fn node_removed(
        &mut self,
        node_path: &Utf8Path,
        old_node: &Node,
        forest: &Forest,
    ) -> Result<()> {
        if !self.args.delete {
            return Ok(());
        }

        let node_path = self.translate_path(node_path);

        self.printer.node_removed(&node_path, old_node, forest)?;

        if self.args.dry_run {
            Ok(())
        } else {
            self.remove_node(&node_path, old_node)
        }
    }

    fn contents_changed(
        &mut self,
        node_path: &Utf8Path,
        old_node: &Node,
        new_node: &Node,
    ) -> Result<()> {
        let node_path = self.translate_path(node_path);

        self.printer
            .contents_changed(&node_path, old_node, new_node)?;

        if self.args.dry_run {
            Ok(())
        } else {
            self.change_node_contents(&node_path, old_node, new_node)
        }
    }

    fn metadata_changed(
        &mut self,
        node_path: &Utf8Path,
        old_node: &Node,
        new_node: &Node,
    ) -> Result<()> {
        let node_path = self.translate_path(node_path);

        self.printer
            .metadata_changed(&node_path, old_node, new_node)?;

        if self.args.dry_run {
            return Ok(());
        }
        self.set_metadata(&node_path, new_node)
    }

    fn type_changed(
        &mut self,
        node_path: &Utf8Path,
        old_node: &Node,
        old_forest: &Forest,
        new_node: &Node,
        new_forest: &Forest,
    ) -> Result<()> {
        let node_path = self.translate_path(node_path);

        // rsync will remove empty directories to replace them with a file,
        // but without --delete will refuse to nuke a directory.
        let was_dir = matches!(&old_node.contents, NodeContents::Directory { .. });
        if was_dir && !self.args.delete {
            let replacement = match &new_node.contents {
                NodeContents::File { .. } => "file",
                NodeContents::Symlink { .. } => "symlink",
                NodeContents::Directory { .. } => unreachable!(),
            };

            // debug? Eh, let's start loud.
            info!("Won't replace dir {node_path} with {replacement} without --delete");
            return Ok(());
        }

        self.printer
            .type_changed(&node_path, old_node, old_forest, new_node, new_forest)?;

        if self.args.dry_run {
            return Ok(());
        }

        self.remove_node(&node_path, old_node)?;
        self.add_node(&node_path, new_node, new_forest)?;
        Ok(())
    }
}

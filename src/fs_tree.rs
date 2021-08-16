//! Walk filesystem trees and indicate if files have changed.

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::*;
use log::*;

use crate::chunk;
use crate::hashing::ObjectId;
use crate::tree;

/// Compares the FS entry with the given path and metadata to previous_node
/// and returns true if it's changed.
pub fn file_changed(
    path: &Path,
    metadata: &tree::NodeMetadata,
    previous_node: Option<&tree::Node>,
) -> bool {
    assert_eq!(metadata.kind(), tree::NodeType::File);

    if previous_node.is_none() {
        trace!("No previous node for {}", path.display());
        return true;
    }
    let previous_node = previous_node.unwrap();
    if previous_node.kind() != metadata.kind() {
        trace!(
            "{} was a {} before and is a file now",
            path.display(),
            format!("{:?}", previous_node.kind()).to_lowercase(),
        );
        return true;
    }

    let previous_metadata = &previous_node.metadata;
    if metadata.modification_time() != previous_metadata.modification_time() {
        trace!("{} was changed since its backup", path.display());
        return true;
    }

    if metadata.size() != previous_metadata.size() {
        trace!("{} is a different size than its backup", path.display());
        return true;
    }

    trace!(
        "{} matches its previous size and modification time. Reuse previous chunks",
        path.display()
    );
    false
}

/// Information about a directory entry when walking a filesystem tree,
/// comparing it to a previous tree.
pub enum DirectoryEntry<T> {
    /// A directory with the data [`walk_fs()`] gathered from it.
    Directory(T),
    Symlink {
        target: PathBuf,
    },
    UnchangedFile,
    ChangedFile,
}

/// Recursively walk the filesystem at the given paths,
/// (optionally) comparing to the given previous tree.
///
/// `visit` is called once per path with that entry's metadata,
/// previous node (if any), and results of the recursive call for directories
/// (see [`DirectoryEntry`]). It appends to some intermediate value,
/// such a tree representing the directory we're traversing.
///
/// `finalize` is responsible for taking that intermediate value and converting
/// it to the desired return value, e.g., calulating the ID of the tree representing
/// the directory we're traversing.
///
/// See [`forest_from_fs`] or [`backup_tree`](crate::ui::backup::backup_tree) for examples.
pub fn walk_fs<T, Intermediate, Visit, Finalize>(
    paths: &BTreeSet<PathBuf>,
    previous_tree: Option<&ObjectId>,
    previous_forest: &tree::Forest,
    visit: &mut Visit,
    finalize: &mut Finalize,
) -> Result<T>
where
    Visit: FnMut(
        &mut Intermediate,
        &Path,
        tree::NodeMetadata,
        Option<&tree::Node>,
        DirectoryEntry<T>,
    ) -> Result<()>,
    Finalize: FnMut(Intermediate) -> Result<T>,
    Intermediate: Default,
{
    let mut intermediate = Intermediate::default();

    let previous_tree = previous_tree.and_then(|id| previous_forest.get(id));

    for path in paths {
        let entry_name = path.file_name().expect("Given path ended in ..");

        let previous_node = previous_tree
            .as_ref()
            .and_then(|tree| tree.get(Path::new(entry_name)));

        let metadata = tree::get_metadata(path)?;

        let subnode = match metadata.kind() {
            tree::NodeType::Directory => {
                // Gather the dir entries in `path`, recurse into it,
                // and add the subtree to the tree.
                let subpaths = fs::read_dir(path)?
                    .map(|entry| entry.map(|e| e.path()))
                    .collect::<io::Result<BTreeSet<PathBuf>>>()
                    .with_context(|| format!("Failed iterating subdirectory {}", path.display()))?;

                let previous_subtree = previous_node.and_then(|n| match &n.contents {
                    tree::NodeContents::Directory { subtree } => Some(subtree),
                    tree::NodeContents::File { .. } => {
                        trace!("{} was a file, but is now a directory", path.display());
                        None
                    }
                    tree::NodeContents::Symlink { target } => {
                        trace!(
                            "{} was a file, but is now a symlink to {}",
                            path.display(),
                            target.display()
                        );
                        None
                    }
                });

                let sub_result: T = walk_fs(
                    &subpaths,
                    previous_subtree,
                    previous_forest,
                    visit,
                    finalize,
                )
                .with_context(|| format!("Failed to walk subdirectory {}", path.display()))?;

                DirectoryEntry::Directory(sub_result)
            }
            tree::NodeType::Symlink => {
                let target = fs::read_link(&path).context("Couldn't get symlink target")?;
                DirectoryEntry::Symlink { target }
            }
            tree::NodeType::File => {
                if !file_changed(path, &metadata, previous_node) {
                    DirectoryEntry::UnchangedFile
                } else {
                    DirectoryEntry::ChangedFile
                }
            }
        };

        visit(&mut intermediate, path, metadata, previous_node, subnode)?;
    }
    finalize(intermediate)
}

/// Hashes the forest for the given paths,
/// reusing chunks from the previous tree when able.
pub fn forest_from_fs(
    paths: &BTreeSet<PathBuf>,
    previous_tree: Option<&ObjectId>,
    previous_forest: &tree::Forest,
) -> Result<(ObjectId, tree::Forest)> {
    fn visit(
        (tree, forest): &mut (tree::Tree, tree::Forest),
        path: &Path,
        metadata: tree::NodeMetadata,
        previous_node: Option<&tree::Node>,
        entry: DirectoryEntry<(ObjectId, tree::Forest)>,
    ) -> Result<()> {
        let node = match entry {
            DirectoryEntry::Directory((subtree, subforest)) => {
                forest.extend(subforest);

                trace!(
                    "{}{} hashed to {}",
                    path.display(),
                    std::path::MAIN_SEPARATOR,
                    subtree
                );
                info!("finished {}{}", path.display(), std::path::MAIN_SEPARATOR);

                tree::Node {
                    metadata,
                    contents: tree::NodeContents::Directory { subtree },
                }
            }
            DirectoryEntry::Symlink { target } => {
                info!("{:>8} {}", "symlink", path.display());
                tree::Node {
                    metadata,
                    contents: tree::NodeContents::Symlink { target },
                }
            }
            DirectoryEntry::UnchangedFile => {
                info!("{:>8} {}", "skip", path.display());
                tree::Node {
                    metadata,
                    contents: previous_node.unwrap().contents.clone(),
                }
            }
            DirectoryEntry::ChangedFile => {
                let chunks = chunk::chunk_file(&path)?;

                let mut chunk_ids = Vec::new();
                for chunk in chunks {
                    chunk_ids.push(chunk.id);
                }
                info!("{:>8} {}", "hash", path.display());

                tree::Node {
                    metadata,
                    contents: tree::NodeContents::File { chunks: chunk_ids },
                }
            }
        };
        ensure!(
            tree.insert(PathBuf::from(path.file_name().unwrap()), node)
                .is_none(),
            "Duplicate tree entries"
        );
        Ok(())
    }

    // Turn the tree into its ID and add it to the forest.
    fn finalize(
        (tree, mut forest): (tree::Tree, tree::Forest),
    ) -> Result<(ObjectId, tree::Forest)> {
        let (_bytes, id) = tree::serialize_and_hash(&tree)?;

        let tree = Arc::new(tree);

        if let Some(previous) = forest.insert(id, tree.clone()) {
            debug_assert_eq!(*previous, *tree);
            trace!("tree {} already hashed", id);
        }
        Ok((id, forest))
    }

    walk_fs(
        paths,
        previous_tree,
        previous_forest,
        &mut visit,
        &mut finalize,
    )
}

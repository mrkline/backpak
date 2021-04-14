//! Tree walking - compare repo trees to the filesystem.

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

pub fn forest_from_fs(
    paths: &BTreeSet<PathBuf>,
    previous_tree: Option<&ObjectId>,
    previous_forest: &tree::Forest,
) -> Result<(ObjectId, tree::Forest)> {
    let previous_tree = previous_tree.and_then(|id| previous_forest.get(&id));

    let mut tree = tree::Tree::new();
    let mut forest = tree::Forest::new();

    for path in paths {
        let entry_name = path.file_name().expect("Given path ended in ..");

        let previous_node = previous_tree
            .as_ref()
            .and_then(|tree| tree.get(Path::new(entry_name)));

        let metadata = tree::get_metadata(path)?;

        let node = if metadata.kind() == tree::NodeType::Directory {
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

            let (subtree, subforest) = forest_from_fs(&subpaths, previous_subtree, previous_forest)
                .with_context(|| format!("Failed to pack subdirectory {}", path.display()))?;
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
        } else if !file_changed(path, &metadata, previous_node) {
            info!("{:>8} {}", "skip", path.display());

            tree::Node {
                metadata,
                contents: previous_node.unwrap().contents.clone(),
            }
        } else {
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
        };
        ensure!(
            tree.insert(PathBuf::from(entry_name), node).is_none(),
            "Duplicate tree entries"
        );
    }
    let (_bytes, id) = tree::serialize_and_hash(&tree)?;

    let tree = Arc::new(tree);

    if let Some(previous) = forest.insert(id, tree.clone()) {
        debug_assert_eq!(*previous, *tree);
        trace!("tree {} already hashed", id);
    }
    Ok((id, forest))
}

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
        "{} matches its previous size and modification time. Reuse parent chunks",
        path.display()
    );
    false
}

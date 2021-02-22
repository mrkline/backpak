use super::*;

use std::collections::HashSet;
use std::io;

use crate::blob::{self, Blob};
use crate::chunk;
use crate::hashing::ObjectId;
use crate::tree;

pub fn pack_tree(
    paths: &BTreeSet<PathBuf>,
    previous_tree: Option<&ObjectId>,
    previous_forest: &tree::Forest,
    packed_blobs: &mut HashSet<ObjectId>,
    chunk_tx: &mut Sender<Blob>,
    tree_tx: &mut Sender<Blob>,
) -> Result<ObjectId> {
    let mut nodes = tree::Tree::new();

    let previous_tree = previous_tree.and_then(|id| previous_forest.get(&id));

    for path in paths {
        let entry_name = path.file_name().expect("Given path ended in ..");

        let previous_node = previous_tree
            .as_ref()
            .and_then(|tree| tree.get(Path::new(entry_name)));

        let metadata = tree::get_metadata(path)?;

        let node = if metadata.is_directory() {
            // Gather the dir entries in `path`, call pack_tree with them,
            // and add an entry to `nodes` for the subtree.
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
            });

            let subtree: ObjectId = pack_tree(
                &subpaths,
                previous_subtree,
                previous_forest,
                packed_blobs,
                chunk_tx,
                tree_tx,
            )
            .with_context(|| format!("Failed to pack subdirectory {}", path.display()))?;
            debug!("Subtree {}/ packed as {}", path.display(), subtree);

            tree::Node {
                metadata,
                contents: tree::NodeContents::Directory { subtree },
            }
        } else if !file_changed(path, &metadata, previous_node) {
            tree::Node {
                metadata,
                contents: previous_node.unwrap().contents.clone(),
            }
        } else {
            let chunks = chunk::chunk_file(&path)?;
            let mut chunk_ids = Vec::new();
            for chunk in chunks {
                chunk_ids.push(chunk.id);

                if packed_blobs.insert(chunk.id) {
                    chunk_tx
                        .send(chunk)
                        .context("backup -> chunk packer channel exited early")?;
                } else {
                    trace!("Skipping chunk {}; already packed", chunk.id);
                }
            }
            tree::Node {
                metadata,
                contents: tree::NodeContents::File { chunks: chunk_ids },
            }
        };
        ensure!(
            nodes.insert(PathBuf::from(entry_name), node).is_none(),
            "Duplicate tree entries"
        );
    }
    let (bytes, id) = tree::serialize_and_hash(&nodes)?;
    if packed_blobs.insert(id) {
        tree_tx
            .send(Blob {
                contents: blob::Contents::Buffer(bytes),
                id,
                kind: blob::Type::Tree,
            })
            .context("backup -> tree packer channel exited early")?;
    } else {
        trace!("Skipping tree {}; already packed", id);
    }
    Ok(id)
}

fn file_changed(
    path: &Path,
    metadata: &tree::NodeMetadata,
    previous_node: Option<&tree::Node>,
) -> bool {
    assert!(!metadata.is_directory());

    if previous_node.is_none() {
        trace!("No previous node for {}", path.display());
        return true;
    }
    let previous_node = previous_node.unwrap();
    let previous_metadata = &previous_node.metadata;

    if previous_metadata.is_directory() {
        trace!(
            "{} was a directory before and is a file now",
            path.display()
        );
        return true;
    }
    if let tree::NodeContents::Directory { .. } = previous_node.contents {
        // That's not right...
        warn!("{}'s previous metadata has directory contents but flags say it was a file. Re-evaluating", path.display());
        return true;
    }

    if metadata.modification_time() != previous_metadata.modification_time() {
        trace!("{} was changed since its last backup", path.display());
        return true;
    }

    if metadata.size() != previous_metadata.size() {
        trace!(
            "{} is a different size than it was last backup",
            path.display()
        );
        return true;
    }

    debug!(
        "{} matches its previous size and modification time. Reusing previous chunks",
        path.display()
    );
    false
}

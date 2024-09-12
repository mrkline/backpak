//! Shared utilities to repack blobs,
//! either loose ones in `backpak prune` or to another repo in `backpak copy`
use anyhow::{anyhow, Context, Result};
use camino::Utf8Path;
use rustc_hash::FxHashSet;
use tracing::*;

use crate::{backup, blob, hashing::ObjectId, read, snapshot::Snapshot, tree};

pub struct SnapshotAndForest {
    pub id: ObjectId,
    pub snapshot: Snapshot,
    pub forest: tree::Forest,
}

/// Load all trees for each given snapshot
pub fn load_forests(
    snapshots: Vec<(Snapshot, ObjectId)>,
    tree_cache: &mut tree::Cache,
) -> Result<Vec<SnapshotAndForest>> {
    snapshots
        .into_iter()
        .map(|(snapshot, id)| {
            let forest = tree::forest_from_root(&snapshot.tree, tree_cache)?;
            Ok(SnapshotAndForest {
                id,
                snapshot,
                forest,
            })
        })
        .collect()
}

/// What we're doing (different log messages make more sense for each)
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum Op {
    Copy,
    Prune,
}

/// Walk each snapshot and its forest, copying as-needed to the given backup.
///
/// This returns a new list of snapshots since filtering a tree changes its contents,
/// which changes its ID, which changes the IDs of all trees above it.
/// Aren't Merkle trees fun?
pub fn walk_snapshots<Filter>(
    op: Op,
    snapshots_and_forests: &[SnapshotAndForest],
    mut filter: Filter,
    reader: &mut read::ChunkReader,
    packed_blobs: &mut FxHashSet<ObjectId>,
    backup: &mut Option<backup::Backup>,
) -> Result<Vec<Snapshot>>
where
    Filter: FnMut(
        &Utf8Path,
        // More some day?
    ) -> Result<bool>,
{
    let new_snaps = snapshots_and_forests
        .iter()
        .map(|snf| walk_snapshot(op, snf, &mut filter, reader, packed_blobs, backup))
        .collect::<Result<Vec<_>>>()?;
    Ok(new_snaps)
}

/// Walk the given snapshot, copying to the given backup. Return the new (filtered) snapshot.
fn walk_snapshot<Filter>(
    op: Op,
    snapshot_and_forest: &SnapshotAndForest,
    filter: &mut Filter,
    reader: &mut read::ChunkReader,
    packed_blobs: &mut FxHashSet<ObjectId>,
    backup: &mut Option<backup::Backup>,
) -> Result<Snapshot>
where
    Filter: FnMut(
        &Utf8Path,
        // More some day?
    ) -> Result<bool>,
{
    let action = match op {
        Op::Copy => "Copying snapshot",
        Op::Prune => "Repacking loose blobs from snapshot",
    };
    info!("{action} {}", snapshot_and_forest.id);
    let new_root = walk_tree(
        op,
        filter,
        Utf8Path::new(""),
        &snapshot_and_forest.snapshot.tree,
        &snapshot_and_forest.forest,
        reader,
        packed_blobs,
        backup,
    )
    .with_context(|| format!("In snapshot {}", snapshot_and_forest.id))?;

    let mut new_snapshot = snapshot_and_forest.snapshot.clone();
    new_snapshot.tree = new_root;
    Ok(new_snapshot)
}

#[expect(clippy::too_many_arguments)] // We know, sit down.
fn walk_tree<Filter>(
    op: Op,
    filter: &mut Filter,
    tree_path: &Utf8Path,
    tree_id: &ObjectId,
    forest: &tree::Forest,
    reader: &mut read::ChunkReader,
    packed_blobs: &mut FxHashSet<ObjectId>,
    backup: &mut Option<backup::Backup>,
) -> Result<ObjectId>
where
    Filter: FnMut(
        &Utf8Path,
        // More some day?
    ) -> Result<bool>,
{
    let tree: &tree::Tree = forest
        .get(tree_id)
        .ok_or_else(|| anyhow!("Missing tree {}", tree_id))
        .unwrap();

    // Cloning the tree node-by-node is wasteful if we didn't actually filter anything out
    // (which means this tree hasn't changed at all!).
    // But this is fast enough compared to all the IO that we don't need a separate
    // "we filtered nothing" path.
    let mut new_tree = tree::Tree::default();

    for (path, node) in tree {
        let mut node_path = tree_path.to_owned();
        node_path.push(path);
        if !filter(&node_path)? {
            info!("  {:>9} {node_path}", "skip");
            continue;
        }

        let new_node: tree::Node = match &node.contents {
            tree::NodeContents::File { chunks } => {
                let mut chunks_repacked = false;
                let verb = match op {
                    Op::Copy => "copied",
                    Op::Prune => "repacked",
                };

                for chunk in chunks {
                    if packed_blobs.insert(*chunk) {
                        repack_chunk(chunk, reader, backup)?;
                        chunks_repacked = true;
                    }
                }
                if chunks_repacked {
                    info!("  {verb:>9} {node_path}");
                } else {
                    info!("  {:>9} {node_path}", "deduped"); // Sorta; "unneeded"? Bleh.
                }
                // We're not changing any files, the node stays the same.
                node.clone()
            }
            tree::NodeContents::Symlink { .. } => {
                // Nothing to change or repack for symlinks.
                node.clone()
            }
            tree::NodeContents::Directory { subtree } => {
                let new_tree = walk_tree(
                    op,
                    filter,
                    &node_path,
                    subtree,
                    forest,
                    reader,
                    packed_blobs,
                    backup,
                )?;
                info!(
                    "  {:>9} {node_path}{}",
                    "finished",
                    std::path::MAIN_SEPARATOR
                );
                tree::Node {
                    contents: tree::NodeContents::Directory { subtree: new_tree },
                    metadata: node.metadata.clone(),
                }
            }
        };
        assert!(new_tree.insert(path.clone(), new_node).is_none());
    }

    // We might have a new tree, we might have the exact same tree.
    // Serialize and hash it to find out.
    // (Again, we could have a separate "we didn't filter anything" path,
    // but it doesn't seem worth it at the moment.)
    let (serialized, new_tree_id) = tree::serialize_and_hash(&new_tree)?;
    // If we don't have this tree, new or old, in the backup, add it.
    if packed_blobs.insert(new_tree_id) {
        if let Some(b) = backup {
            b.tree_tx.send(blob::Blob {
                contents: blob::Contents::Buffer(serialized),
                id: new_tree_id,
                kind: blob::Type::Tree,
            })?;
        }
    }
    Ok(new_tree_id)
}

fn repack_chunk<'a, 'b: 'a>(
    id: &'a ObjectId,
    reader: &mut read::ChunkReader<'b>,
    backup: &mut Option<backup::Backup>,
) -> Result<()> {
    if let Some(b) = backup {
        // TODO: Don't clone this? Make the Buffer RC? The blob cache Arc? Ugh, where GC
        let contents = blob::Contents::Buffer((*reader.read_blob(id)?).clone());
        b.chunk_tx.send(blob::Blob {
            contents,
            id: *id,
            kind: blob::Type::Chunk,
        })?;
    }
    Ok(())
}

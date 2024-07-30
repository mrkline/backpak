//! Shared utilities to repack blobs,
//! either loose ones in `backpak prune` or to another repo in `backpak copy`
use anyhow::{anyhow, ensure, Context, Result};
use camino::Utf8Path;
use rustc_hash::FxHashSet;
use tracing::*;

use crate::{backend, backup, blob, hashing::ObjectId, read, snapshot, tree};

pub struct SnapshotAndForest {
    pub id: ObjectId,
    pub snapshot: snapshot::Snapshot,
    pub forest: tree::Forest,
}

pub fn load_snapshots_and_forests(
    cached_backend: &backend::CachedBackend,
    tree_cache: &mut tree::Cache,
) -> Result<Vec<SnapshotAndForest>> {
    snapshot::load_chronologically(cached_backend)?
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

pub fn walk_snapshots(
    op: Op,
    snapshots_and_forests: &[SnapshotAndForest],
    reader: &mut read::BlobReader,
    packed_blobs: &mut FxHashSet<ObjectId>,
    backup: &mut Option<backup::Backup>,
) -> Result<()> {
    // Walk from newest to oldest snapshots so that we prioritize the locality of chunks
    // in newer snapshots. This is probably a horse a piece - you could argue that
    // older snapshots are more important - but all the blobs will get packed up regardless.
    for snapshot in snapshots_and_forests.iter().rev() {
        walk_snapshot(op, snapshot, reader, packed_blobs, backup)?
    }
    Ok(())
}

fn walk_snapshot(
    op: Op,
    snapshot_and_forest: &SnapshotAndForest,
    reader: &mut read::BlobReader,
    packed_blobs: &mut FxHashSet<ObjectId>,
    backup: &mut Option<backup::Backup>,
) -> Result<()> {
    let action = match op {
        Op::Copy => "Copying snapshot ",
        Op::Prune => "Repacking any loose blobs from snapshot ",
    };
    debug!("{action} {}", snapshot_and_forest.id);
    walk_tree(
        op,
        &snapshot_and_forest.snapshot.tree,
        &snapshot_and_forest.forest,
        reader,
        packed_blobs,
        backup,
    )
    .with_context(|| format!("In snapshot {}", snapshot_and_forest.id))
}

fn walk_tree(
    op: Op,
    tree_id: &ObjectId,
    forest: &tree::Forest,
    reader: &mut read::BlobReader,
    packed_blobs: &mut FxHashSet<ObjectId>,
    backup: &mut Option<backup::Backup>,
) -> Result<()> {
    let tree: &tree::Tree = forest
        .get(tree_id)
        .ok_or_else(|| anyhow!("Missing tree {}", tree_id))
        .unwrap();

    for (path, node) in tree {
        match &node.contents {
            tree::NodeContents::File { chunks } => {
                for chunk in chunks {
                    if packed_blobs.insert(*chunk) {
                        repack_chunk(op, chunk, path, reader, backup)?;
                    } else {
                        let verb = match op {
                            Op::Copy => "copied",
                            Op::Prune => "repacked",
                        };
                        trace!("Skipping chunk {chunk}; already {verb}");
                    }
                }
            }
            tree::NodeContents::Symlink { .. } => {
                // Nothing to repack for symlinks.
            }
            tree::NodeContents::Directory { subtree } => {
                walk_tree(op, subtree, forest, reader, packed_blobs, backup)?
            }
        }
    }

    if packed_blobs.insert(*tree_id) {
        repack_tree(op, tree_id, tree, backup)?;
    } else {
        trace!("Skipping tree {}; already packed", tree_id);
    }
    Ok(())
}

fn repack_chunk(
    op: Op,
    id: &ObjectId,
    path: &Utf8Path,
    reader: &mut read::BlobReader,
    backup: &mut Option<backup::Backup>,
) -> Result<()> {
    let verb = match op {
        Op::Copy => "Copying",
        Op::Prune => "Repacking",
    };
    trace!("{verb} chunk {id} from {path}");
    if let Some(b) = backup {
        let contents = blob::Contents::Buffer(reader.read_blob(id)?);
        b.chunk_tx.send(blob::Blob {
            contents,
            id: *id,
            kind: blob::Type::Chunk,
        })?;
    }
    Ok(())
}

fn repack_tree(
    op: Op,
    id: &ObjectId,
    tree: &tree::Tree,
    backup: &mut Option<backup::Backup>,
) -> Result<()> {
    let verb = match op {
        Op::Copy => "Copying",
        Op::Prune => "Repacking",
    };
    trace!("{verb} tree {}", id);

    let (reserialized, check_id) = tree::serialize_and_hash(tree)?;
    // Sanity check:
    ensure!(
        check_id == *id,
        "Tree {} has a different ID ({}) when reserialized",
        id,
        check_id
    );

    if let Some(b) = backup {
        let contents = blob::Contents::Buffer(reserialized);
        b.tree_tx.send(blob::Blob {
            contents,
            id: *id,
            kind: blob::Type::Tree,
        })?;
    }

    Ok(())
}

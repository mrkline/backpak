//! Shared utilities to repack blobs,
//! either loose ones in `backpak prune` or to another repo in `backpak copy`
use anyhow::{anyhow, ensure, Context, Result};
use camino::Utf8Path;
use rustc_hash::FxHashSet;
use tracing::*;

use crate::{backup, blob, hashing::ObjectId, read, snapshot, tree};

pub struct SnapshotAndForest {
    pub id: ObjectId,
    pub snapshot: snapshot::Snapshot,
    pub forest: tree::Forest,
}

pub fn load_forests(
    snapshots: Vec<(snapshot::Snapshot, ObjectId)>,
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

pub fn walk_snapshots<Filter>(
    op: Op,
    snapshots_and_forests: &[SnapshotAndForest],
    mut filter: Filter,
    reader: &mut read::ChunkReader,
    packed_blobs: &mut FxHashSet<ObjectId>,
    backup: &mut Option<backup::Backup>,
) -> Result<()>
where
    Filter: FnMut(
        &Utf8Path,
        // More some day?
    ) -> Result<bool>,
{
    for snapshot in snapshots_and_forests.iter() {
        walk_snapshot(op, snapshot, &mut filter, reader, packed_blobs, backup)?
    }
    Ok(())
}

fn walk_snapshot<Filter>(
    op: Op,
    snapshot_and_forest: &SnapshotAndForest,
    filter: &mut Filter,
    reader: &mut read::ChunkReader,
    packed_blobs: &mut FxHashSet<ObjectId>,
    backup: &mut Option<backup::Backup>,
) -> Result<()>
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
    walk_tree(
        op,
        filter,
        Utf8Path::new(""),
        &snapshot_and_forest.snapshot.tree,
        &snapshot_and_forest.forest,
        reader,
        packed_blobs,
        backup,
    )
    .with_context(|| format!("In snapshot {}", snapshot_and_forest.id))
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
) -> Result<()>
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

    for (path, node) in tree {
        let mut node_path = tree_path.to_owned();
        node_path.push(path);
        if !filter(&node_path)? {
            info!("  {:>9} {node_path}", "skip");
            continue;
        }

        match &node.contents {
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
            }
            tree::NodeContents::Symlink { .. } => {
                // Nothing to repack for symlinks.
            }
            tree::NodeContents::Directory { subtree } => {
                walk_tree(
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
            }
        }
    }

    if packed_blobs.insert(*tree_id) {
        repack_tree(tree_id, tree, backup)?;
    }
    Ok(())
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

fn repack_tree(
    id: &ObjectId,
    tree: &tree::Tree,
    backup: &mut Option<backup::Backup>,
) -> Result<()> {
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

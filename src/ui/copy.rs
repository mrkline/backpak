use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use log::*;
use rustc_hash::FxHashSet;

use crate::backend;
use crate::backup;
use crate::blob;
// use crate::file_util::nice_size;
use crate::hashing::ObjectId;
use crate::index;
use crate::read;
use crate::snapshot;
use crate::tree;

/// Copy snapshots from one repository to another.
#[derive(Debug, Parser)]
#[command(verbatim_doc_comment)]
pub struct Args {
    #[clap(short = 'n', long)]
    pub dry_run: bool,

    /// Destination repository
    #[clap(short, long, name = "PATH")]
    to: Utf8PathBuf,
    // TODO: Specify snapshots, or ALL
}

pub fn run(repository: &Utf8Path, args: Args) -> Result<()> {
    // Build the usual suspects.
    let (_, src_cached_backend) = backend::open(repository, backend::CacheBehavior::Normal)?;
    let src_index = index::build_master_index(&src_cached_backend)?;
    let src_blob_map = index::blob_to_pack_map(&src_index)?;

    let src_snapshots_and_forests = load_snapshots_and_forests(
        &src_cached_backend,
        // We can drop the tree cache immediately once we have all our forests.
        &mut tree::Cache::new(&src_index, &src_blob_map, &src_cached_backend),
    )?;
    // Walk from newest to oldest snapshots so that we prioritize the locality of chunks
    // in newer snapshots. This is probably a horse a piece - you could argue that
    // older snapshots are more important - but all the blobs will get packed up regardless.
    let src_snapshots_and_forests: Vec<_> = src_snapshots_and_forests.into_iter().rev().collect();

    // Get a reader to load the chunks we're copying.
    let mut reader = read::BlobReader::new(&src_cached_backend, &src_index, &src_blob_map);

    let (dst_backend_config, dst_cached_backend) =
        backend::open(&args.to, backend::CacheBehavior::Normal)?;
    let dst_index = index::build_master_index(&dst_cached_backend)?;

    // Track all the blobs already in the destination.
    let mut packed_blobs = index::blob_id_set(&dst_index)?;

    let backup::ResumableBackup {
        wip_index,
        cwd_packfiles,
    } = backup::find_resumable(&dst_cached_backend)?.unwrap_or_default();

    for manifest in wip_index.packs.values() {
        for entry in manifest {
            packed_blobs.insert(entry.id);
        }
    }

    let dst_backend_config = Arc::new(dst_backend_config);
    let dst_cached_backend = Arc::new(dst_cached_backend);
    let mut backup = (!args.dry_run).then(|| {
        backup::spawn_backup_threads(dst_backend_config, dst_cached_backend.clone(), wip_index)
    });

    // Finish the WIP resume business.
    if let Some(b) = &mut backup {
        backup::upload_cwd_packfiles(&mut b.upload_tx, &cwd_packfiles)?;
    }
    drop(cwd_packfiles);

    walk_snapshots(
        &src_snapshots_and_forests,
        &mut reader,
        &mut packed_blobs,
        &mut backup,
    )?;

    // Important: make sure all blobs and indexes are written BEFORE
    // we upload the snapshot.
    // It's meaningless unless everything else is there first!
    let _stats = backup.map(|b| b.join()).transpose()?;

    if !args.dry_run {
        // Upload the indexes
    }

    Ok(())
}

struct SnapshotAndForest {
    id: ObjectId,
    snapshot: snapshot::Snapshot,
    forest: tree::Forest,
}

fn load_snapshots_and_forests(
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

fn walk_snapshots(
    snapshots_and_forests: &[SnapshotAndForest],
    reader: &mut read::BlobReader,
    packed_blobs: &mut FxHashSet<ObjectId>,
    backup: &mut Option<backup::Backup>,
) -> Result<()> {
    // Walk from newest to oldest snapshots so that we prioritize the locality of chunks
    // in newer snapshots. This is probably a horse a piece - you could argue that
    // older snapshots are more important - but all the blobs will get packed up regardless.
    for snapshot in snapshots_and_forests.iter().rev() {
        walk_snapshot(snapshot, reader, packed_blobs, backup)?
    }
    Ok(())
}

fn walk_snapshot(
    snapshot_and_forest: &SnapshotAndForest,
    reader: &mut read::BlobReader,
    packed_blobs: &mut FxHashSet<ObjectId>,
    backup: &mut Option<backup::Backup>,
) -> Result<()> {
    debug!("Copying snapshot {}", snapshot_and_forest.id);
    walk_tree(
        &snapshot_and_forest.snapshot.tree,
        &snapshot_and_forest.forest,
        reader,
        packed_blobs,
        backup,
    )
    .with_context(|| format!("In snapshot {}", snapshot_and_forest.id))
}

fn walk_tree(
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
                        repack_chunk(chunk, path, reader, backup)?;
                    } else {
                        trace!("Skipping chunk {}; already there", chunk);
                    }
                }
            }
            tree::NodeContents::Symlink { .. } => {
                // Nothing to repack for symlinks.
            }
            tree::NodeContents::Directory { subtree } => {
                walk_tree(subtree, forest, reader, packed_blobs, backup)?
            }
        }
    }

    if packed_blobs.insert(*tree_id) {
        repack_tree(tree_id, tree, backup)?;
    } else {
        trace!("Skipping tree {}; already there", tree_id);
    }
    Ok(())
}

fn repack_chunk(
    id: &ObjectId,
    path: &Utf8Path,
    reader: &mut read::BlobReader,
    backup: &mut Option<backup::Backup>,
) -> Result<()> {
    trace!("Copying chunk {id} from {path}");
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
    id: &ObjectId,
    tree: &tree::Tree,
    backup: &mut Option<backup::Backup>,
) -> Result<()> {
    trace!("Copying tree {}", id);

    let (reserialized, check_id) = tree::serialize_and_hash(tree)?;
    // Sanity check:
    /*
    ensure!(
        check_id == *id,
        "Tree {} has a different ID ({}) when reserialized",
        id,
        check_id
    );
    */

    if let Some(b) = backup {
        let contents = blob::Contents::Buffer(reserialized);
        b.tree_tx.send(blob::Blob {
            contents,
            id: check_id,
            kind: blob::Type::Tree,
        })?;
    }

    Ok(())
}

use std::thread;

use anyhow::{anyhow, bail, ensure, Context, Result};
use camino::Utf8Path;
use clap::Parser;
use rustc_hash::FxHashSet;
use tracing::*;

use crate::{
    backend, backup, blob, filter,
    hashing::ObjectId,
    index::{self, Index},
    repack, snapshot, tree,
};

/// Copy a snapshot, filtering out given paths
#[derive(Debug, Parser)]
#[command(verbatim_doc_comment)]
pub struct Args {
    #[clap(short = 'n', long)]
    dry_run: bool,

    /// Preserve snapshot author, time, and tags from the target
    #[clap(short, long)]
    keep_metadata: bool,

    /// The author of the snapshot (otherwise the hostname is used)
    #[clap(short, long, name = "name")]
    author: Option<String>,

    /// Add a metadata tag to the snapshot (can be specified multiple times)
    #[clap(short = 't', long = "tag", name = "tag")]
    tags: Vec<String>,

    /// Skip anything whose path matches the given regular expression
    #[clap(short = 's', long = "skip", name = "regex", required = true)]
    skips: Vec<String>,

    /// The snapshot to filter
    target_snapshot: String,
}

pub fn run(repository: &Utf8Path, args: Args) -> Result<()> {
    if args.keep_metadata && (args.author.is_some() || !args.tags.is_empty()) {
        bail!("Give either --keep-metadata or new metadata with --author, --tags (see --help)")
    }

    // Build the usual suspects.
    let (backend_config, cached_backend) =
        backend::open(repository, backend::CacheBehavior::Normal)?;
    let index = index::build_master_index(&cached_backend)?;
    let blob_map = index::blob_to_pack_map(&index)?;

    let chrono_snapshots = snapshot::load_chronologically(&cached_backend)?;
    let (target, target_id) = snapshot::find(&chrono_snapshots, &args.target_snapshot)?;

    let snapshot_and_forest = repack::load_forest(
        target.clone(),
        *target_id,
        // We can drop the tree cache immediately once we have our forest.
        &mut tree::Cache::new(&index, &blob_map, &cached_backend),
    )?;

    // Track all the blobs we already have.
    let mut packed_blobs = index::blob_id_set(&index)?;

    // Let's not do anything resumable for now;
    // this should be a cheap operation that only creates new, (smaller) trees.

    let bmode = if args.dry_run {
        backup::Mode::DryRun
    } else {
        backup::Mode::LiveFire
    };

    let back_stats = backup::BackupStatistics::default();
    let mut new_snapshot = thread::scope(|s| -> Result<_> {
        let mut backup = backup::spawn_backup_threads(
            s,
            bmode,
            &backend_config,
            &cached_backend,
            Index::default(),
            &back_stats,
        );

        let mut filter = filter::skip_matching_paths(&args.skips)?;

        let new_snapshot = walk_snapshot(
            &snapshot_and_forest,
            &mut filter,
            &mut packed_blobs,
            &mut backup,
        )?;

        // Important: make sure all new trees and the index are written BEFORE
        // we upload the new snapshot.
        // It's meaningless unless everything else is there first!
        backup.join()?;

        Ok(new_snapshot)
    })?;

    if new_snapshot == *target {
        info!("Nothing filtered; no new snapshot");
    } else if !args.dry_run {
        if !args.keep_metadata {
            new_snapshot.author = match args.author {
                Some(a) => a,
                None => hostname::get()
                    .context("Couldn't get hostname")?
                    .to_string_lossy()
                    .to_string(),
            };
            new_snapshot.time = jiff::Zoned::now().round(jiff::Unit::Second)?;
            new_snapshot.tags = args.tags.into_iter().collect();
        }

        snapshot::upload(&new_snapshot, &cached_backend)?;
    }

    Ok(())
}

// Basically a clone of the repack module,
// but we don't need to read nor upload any blobs.

fn walk_snapshot<Filter>(
    snapshot_and_forest: &repack::SnapshotAndForest,
    filter: &mut Filter,
    packed_blobs: &mut FxHashSet<ObjectId>,
    backup: &mut backup::Backup,
) -> Result<snapshot::Snapshot>
where
    Filter: FnMut(
        &Utf8Path,
        // More some day?
    ) -> bool,
{
    debug!("filtering snapshot {}", snapshot_and_forest.id);
    let new_root = walk_tree(
        filter,
        Utf8Path::new(""),
        &snapshot_and_forest.snapshot.tree,
        &snapshot_and_forest.forest,
        packed_blobs,
        backup,
    )
    .with_context(|| format!("In snapshot {}", snapshot_and_forest.id))?;

    let mut new_snapshot = snapshot_and_forest.snapshot.clone();
    new_snapshot.tree = new_root;
    Ok(new_snapshot)
}

fn walk_tree<Filter>(
    filter: &mut Filter,
    tree_path: &Utf8Path,
    tree_id: &ObjectId,
    forest: &tree::Forest,
    packed_blobs: &mut FxHashSet<ObjectId>,
    backup: &mut backup::Backup,
) -> Result<ObjectId>
where
    Filter: FnMut(
        &Utf8Path,
        // More some day?
    ) -> bool,
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
        if !filter(&node_path) {
            debug!("  {:>8} {node_path}", "skipped");
            continue;
        }

        let new_node: tree::Node = match &node.contents {
            tree::NodeContents::File { chunks } => {
                // Chunks better not have changed and we'd better have them all.
                // We could skip this entirely, but while we're here...
                for chunk in chunks {
                    ensure!(
                        packed_blobs.contains(chunk),
                        "Missing chunk {chunk} from {node_path}"
                    );
                }
                debug!("  {:>8} {node_path}", "kept");
                // We're not changing any files, the node stays the same.
                node.clone()
            }
            tree::NodeContents::Symlink { .. } => {
                debug!("  {:>8} {node_path}", "kept"); // Keep consistent with above
                                                       // Nothing to change or repack for symlinks.
                node.clone()
            }
            tree::NodeContents::Directory { subtree } => {
                let new_tree =
                    walk_tree(filter, &node_path, subtree, forest, packed_blobs, backup)?;
                debug!(
                    "  {:>8} {node_path}{}",
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
        backup.tree_tx.send(blob::Blob {
            contents: blob::Contents::Buffer(serialized),
            id: new_tree_id,
            kind: blob::Type::Tree,
        })?;
    }
    Ok(new_tree_id)
}

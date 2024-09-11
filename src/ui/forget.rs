use anyhow::{bail, Result};
use clap::Parser;
use tracing::*;

use crate::backend;
use crate::hashing::ObjectId;
use crate::snapshot;

/// Forget snapshots
///
/// Data used by these snapshots is not immediately deleted,
/// but will be thrown out by the next `prune`.
#[derive(Debug, Parser)]
#[clap(verbatim_doc_comment)]
pub struct Args {
    #[clap(short = 'n', long)]
    dry_run: bool,

    /// The ID of a snapshot to forget or
    /// "duplicates" to forget duplicate snapshots
    #[clap(required = true, name = "SNAPSHOTS", verbatim_doc_comment)]
    to_forget: Vec<String>,
}

pub fn run(repository: &camino::Utf8Path, args: Args) -> Result<()> {
    unsafe {
        crate::prettify::prettify_serialize();
    }

    assert!(!args.to_forget.is_empty());

    let (_cfg, cached_backend) = backend::open(repository, backend::CacheBehavior::Normal)?;

    let snapshots = snapshot::load_chronologically(&cached_backend)?;
    let success = if args.to_forget == ["duplicates"] {
        forget_duplicate_snapshots(&cached_backend, &snapshots, args.dry_run)?
    } else {
        forget_snapshot_list(&cached_backend, &snapshots, &args)
    };

    if success {
        Ok(())
    } else {
        bail!("Couldn't forget snapshots!");
    }
}

fn forget_duplicate_snapshots(
    cached_backend: &backend::CachedBackend,
    snapshots: &[(snapshot::Snapshot, ObjectId)],
    dry_run: bool,
) -> Result<bool> {
    let mut success = true;
    let mut last_unique_snapshot_and_tree: Option<(ObjectId, ObjectId)> = None;

    for (snapshot, id) in snapshots.iter() {
        if last_unique_snapshot_and_tree.is_none() {
            last_unique_snapshot_and_tree = Some((*id, snapshot.tree));
            continue;
        }

        let (last_unique_snapshot, last_unique_tree) =
            last_unique_snapshot_and_tree.as_ref().unwrap();

        if snapshot.tree != *last_unique_tree {
            last_unique_snapshot_and_tree = Some((*id, snapshot.tree));
            continue;
        }

        // Hey, a duplicate tree!
        info!("Snapshot {} is a duplicate of {}", id, last_unique_snapshot);
        success &= forget_snapshot(cached_backend, id, dry_run);
    }
    Ok(success)
}

fn forget_snapshot_list(
    cached_backend: &backend::CachedBackend,
    snapshots: &[(snapshot::Snapshot, ObjectId)],
    args: &Args,
) -> bool {
    let mut success = true;

    for id_prefix in &args.to_forget {
        let (_snap, id) = match crate::snapshot::find(snapshots, id_prefix) {
            Ok(id) => id,
            Err(e) => {
                error!("{:?}", e);
                success = false;
                continue;
            }
        };

        success &= forget_snapshot(cached_backend, id, args.dry_run);
    }
    success
}

fn forget_snapshot(cached_backend: &backend::CachedBackend, id: &ObjectId, dry_run: bool) -> bool {
    if dry_run {
        info!("Would remove {id}");
        return true;
    } else {
        info!("Forgetting {id}");
    }

    match cached_backend.remove_snapshot(id) {
        Ok(()) => true,
        Err(e) => {
            error!("{:?}", e);
            false
        }
    }
}

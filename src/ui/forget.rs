use std::path::Path;

use anyhow::*;
use log::*;
use structopt::StructOpt;

use crate::backend;
use crate::hashing::ObjectId;
use crate::snapshot;

/// Forget snapshots
///
/// Data used by these snapshots is not immediately deleted,
/// but will be thrown out by the next prune.
#[derive(Debug, StructOpt)]
#[structopt(verbatim_doc_comment)]
pub struct Args {
    #[structopt(short = "n", long)]
    pub dry_run: bool,

    #[structopt(required = true)]
    to_forget: Vec<String>,
}

pub fn run(repository: &Path, args: Args) -> Result<()> {
    unsafe {
        crate::prettify::prettify_serialize();
    }

    assert!(!args.to_forget.is_empty());

    let cached_backend = backend::open(repository)?;

    let success = if args.to_forget == ["duplicates"] {
        forget_duplicate_snapshots(&cached_backend, args.dry_run)?
    } else {
        forget_snapshot_list(&cached_backend, &args)
    };

    if success {
        Ok(())
    } else {
        bail!("Couldn't forget snapshots!");
    }
}

fn forget_duplicate_snapshots(
    cached_backend: &backend::CachedBackend,
    dry_run: bool,
) -> Result<bool> {
    let snapshots = snapshot::load_chronologically(cached_backend)?;

    let mut success = true;
    let mut last_unique_snapshot_and_tree: Option<(ObjectId, ObjectId)> = None;

    for (snapshot, id) in &snapshots {
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
        debug!("Snapshot {} is a duplicate of {}", id, last_unique_snapshot);
        success &= forget_snapshot(cached_backend, id, dry_run);
    }
    Ok(success)
}

fn forget_snapshot_list(cached_backend: &backend::CachedBackend, args: &Args) -> bool {
    let mut success = true;

    for id_prefix in &args.to_forget {
        let id = match crate::snapshot::find(id_prefix, &cached_backend) {
            Ok(id) => id,
            Err(e) => {
                error!("{:?}", e);
                success = false;
                continue;
            }
        };

        success &= forget_snapshot(cached_backend, &id, args.dry_run);
    }
    success
}

fn forget_snapshot(cached_backend: &backend::CachedBackend, id: &ObjectId, dry_run: bool) -> bool {
    if dry_run {
        info!("Would remove {}", id);
        return true;
    }

    match cached_backend.remove_snapshot(&id) {
        Ok(()) => {
            info!("Removed snapshot {}", id);
            true
        }
        Err(e) => {
            error!("{:?}", e);
            false
        }
    }
}

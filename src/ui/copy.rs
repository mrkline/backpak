use std::sync::Arc;

use anyhow::{ensure, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;

use crate::backend;
use crate::backup;
use crate::index;
use crate::read;
use crate::repack;
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

    let src_snapshots_and_forests = repack::load_snapshots_and_forests(
        &src_cached_backend,
        // We can drop the tree cache immediately once we have all our forests.
        &mut tree::Cache::new(&src_index, &src_blob_map, &src_cached_backend),
    )?;

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

    repack::walk_snapshots(
        repack::Op::Copy,
        &src_snapshots_and_forests,
        &mut reader,
        &mut packed_blobs,
        &mut backup,
    )?;

    // Important: make sure all blobs and the index are written BEFORE
    // we upload the snapshots.
    // It's meaningless unless everything else is there first!
    let _stats = backup.map(|b| b.join()).transpose()?;

    if !args.dry_run {
        for sf in &src_snapshots_and_forests {
            let new_id = snapshot::upload(&sf.snapshot, &dst_cached_backend)?;
            ensure!(new_id == sf.id,
                "Snapshot {} has a different ID ({new_id}) when reserialized", sf.id
            );
        }
    }

    Ok(())
}

use std::sync::{atomic::Ordering, Arc};

use anyhow::Result;
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use console::Term;

use crate::backend;
use crate::backup;
use crate::file_util::nice_size;
use crate::filter;
use crate::index;
use crate::read;
use crate::repack;
use crate::snapshot;
use crate::tree;
use crate::ui::progress::{spinner, truncate_path, ProgressThread};

/// Copy snapshots from one repository to another.
#[derive(Debug, Parser)]
#[command(verbatim_doc_comment)]
pub struct Args {
    #[clap(short = 'n', long)]
    dry_run: bool,

    /// Don't print progress to stdout
    #[clap(short, long)]
    quiet: bool,

    /// Skip anything whose absolute path matches the given regular expression
    #[clap(short = 's', long = "skip", name = "regex")]
    skips: Vec<String>,

    /// Destination repository
    #[clap(short, long, name = "PATH")]
    to: Utf8PathBuf,

    #[command(flatten)]
    target: Target,
}

#[derive(Debug, Clone, clap::Args)]
#[group(required = true, multiple = false)]
pub struct Target {
    #[clap(long)]
    all: bool,

    snapshots: Vec<String>,
}

pub fn run(repository: &Utf8Path, args: Args) -> Result<()> {
    let target_snapshots = &args.target.snapshots;

    // Trust but verify
    assert!(args.target.all ^ !target_snapshots.is_empty());

    // Build the usual suspects.
    let (_, src_cached_backend) = backend::open(repository, backend::CacheBehavior::Normal)?;
    let src_index = index::build_master_index(&src_cached_backend)?;
    let src_blob_map = index::blob_to_pack_map(&src_index)?;

    let mut src_snapshots = snapshot::load_chronologically(&src_cached_backend)?;
    if !target_snapshots.is_empty() {
        let mut desired_snaps = Vec::with_capacity(target_snapshots.len());
        for desired_snap in target_snapshots {
            let (s, i) = snapshot::find(&src_snapshots, desired_snap)?;
            desired_snaps.push((s.clone(), *i));
        }
        // Take whatever the user asked for and make it chronological with no duplicates.
        desired_snaps.sort_by_key(|(snap, _)| snap.time.timestamp());
        desired_snaps.dedup_by(|(_, id1), (_, id2)| id1 == id2);
        src_snapshots = desired_snaps;
    }

    let src_snapshots_and_forests = repack::load_forests(
        src_snapshots,
        // We can drop the tree cache immediately once we have all our forests.
        &mut tree::Cache::new(&src_index, &src_blob_map, &src_cached_backend),
    )?;

    // Get a reader to load the chunks we're copying.
    let mut reader = read::ChunkReader::new(&src_cached_backend, &src_index, &src_blob_map);

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

    let bmode = if args.dry_run {
        backup::Mode::DryRun
    } else {
        backup::Mode::LiveFire
    };
    let dst_backend_config = Arc::new(dst_backend_config);
    let dst_cached_backend = Arc::new(dst_cached_backend);
    let mut backup = backup::spawn_backup_threads(
        bmode,
        dst_backend_config,
        dst_cached_backend.clone(),
        wip_index,
    );

    let walk_stats = Arc::new(repack::WalkStatistics::default());
    let progress_thread = (!args.quiet).then(|| {
        let s2 = backup.statistics.clone();
        let ws = walk_stats.clone();
        let t = Term::stdout();
        ProgressThread::spawn(move |i| print_progress(i, &t, &s2, &ws))
    });

    // Finish the WIP resume business.
    if !args.dry_run {
        backup::upload_cwd_packfiles(&mut backup.upload_tx, &cwd_packfiles)?;
    }
    drop(cwd_packfiles);

    let filter = filter::skip_matching_paths(&args.skips)?;

    let new_snapshots = repack::walk_snapshots(
        repack::Op::Copy,
        &src_snapshots_and_forests,
        filter,
        &mut reader,
        &mut packed_blobs,
        &mut backup,
        &walk_stats,
    )?;

    // Important: make sure all blobs and the index are written BEFORE
    // we upload the snapshots.
    // It's meaningless unless everything else is there first!
    let _stats = backup.join()?;
    progress_thread.map(|h| h.join()).transpose()?;

    if !args.dry_run {
        for snap in &new_snapshots {
            snapshot::upload(snap, &dst_cached_backend)?;
        }
    }

    Ok(())
}

fn print_progress(
    i: usize,
    term: &Term,
    bstats: &backup::BackupStats,
    wstats: &repack::WalkStatistics,
) -> Result<()> {
    if i > 0 {
        term.clear_last_lines(4)?;
    }
    let spin = spinner(i);
    let cb = nice_size(bstats.chunk_bytes.load(Ordering::Relaxed));
    let tb = nice_size(bstats.tree_bytes.load(Ordering::Relaxed));
    let rb = nice_size(wstats.reused_bytes.load(Ordering::Relaxed));
    let cz = nice_size(bstats.compressed_bytes.load(Ordering::Relaxed));
    let ub = nice_size(bstats.uploaded_bytes.load(Ordering::Relaxed));
    println!("{spin} P {cb} + {tb} | R {rb} | Z {cz} | U {ub}");

    let idxd = bstats.indexed_packs.load(Ordering::Relaxed);
    let ispin = if idxd % 2 != 0 { 'i' } else { 'I' };
    println!("{ispin} {idxd} packs indexed");

    let cs = wstats.current_snapshot.lock().unwrap().clone();
    println!("snap: {cs}");

    let cf: Utf8PathBuf = wstats.current_file.lock().unwrap().clone();
    let cf = truncate_path(&cf, term);
    println!("{cf}");
    Ok(())
}

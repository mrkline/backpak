use std::cell::RefCell;
use std::collections::BTreeSet;
use std::io;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use anyhow::{bail, ensure, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::Parser;
use console::Term;
use rustc_hash::FxHashSet;
use tracing::*;

use crate::backend;
use crate::backup::{self, *};
use crate::blob::{self, Blob};
use crate::chunk;
use crate::file_util::nice_size;
use crate::filter;
use crate::fs_tree;
use crate::hashing::{HashingWriter, ObjectId};
use crate::index;
use crate::snapshot::{self, Snapshot};
use crate::tree;
use crate::ui::progress::{truncate_path, print_backup_lines, ProgressThread};

/// Create a snapshot of the given files and directories.
#[derive(Debug, Parser)]
pub struct Args {
    /// Dereference symbolic links instead of just saving their target.
    #[clap(short = 'L', long)]
    dereference: bool,

    /// The author of the snapshot (otherwise the hostname is used)
    #[clap(short, long, name = "name")]
    author: Option<String>,

    /// Add a metadata tag to the snapshot (can be specified multiple times)
    #[clap(short = 't', long = "tag", name = "tag")]
    tags: Vec<String>,

    /// Skip anything whose absolute path matches the given regular expression
    #[clap(short = 's', long = "skip", name = "regex")]
    skips: Vec<String>,

    #[clap(short = 'n', long)]
    dry_run: bool,

    /// Don't print progress to stdout
    #[clap(short, long)]
    quiet: bool,

    /// The paths to back up
    ///
    /// These paths are canonicalized into absolute ones.
    /// Snapshots can be restored to either the same absolute paths,
    /// or to a given directory with `restore -o some/dir`
    #[clap(required = true, verbatim_doc_comment)]
    paths: Vec<Utf8PathBuf>,
}

pub fn run(repository: &Utf8Path, args: Args) -> Result<()> {
    // Let's canonicalize our paths (and make sure they're real!)
    // before we spin up a bunch of supporting infrastructure.
    let paths: BTreeSet<Utf8PathBuf> = args
        .paths
        .into_iter()
        .map(|p| {
            p.canonicalize_utf8()
                .with_context(|| format!("Couldn't canonicalize {p}"))
        })
        .collect::<Result<BTreeSet<Utf8PathBuf>>>()?;

    reject_matching_directories(&paths)?;

    let symlink_behavior = if args.dereference {
        tree::Symlink::Dereference
    } else {
        tree::Symlink::Read
    };

    // Do a quick scan of the paths to make sure we can read them and get
    // metadata before we get backends and indexes
    // and threads and all manner of craziness going.
    check_paths(symlink_behavior, &paths).context("Failed FS check prior to backup")?;

    let (backend_config, cached_backend) =
        backend::open(repository, backend::CacheBehavior::Normal)?;

    let index = index::build_master_index(&cached_backend)?;
    let blob_map = index::blob_to_pack_map(&index)?;

    info!("Finding a parent snapshot");
    let snapshots = snapshot::load_chronologically(&cached_backend)?;
    let parent = parent_snapshot(&paths, snapshots);
    let parent = parent.as_ref();

    trace!("Loading all trees from the parent snapshot");
    let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);
    let parent_forest = parent
        .map(|p| tree::forest_from_root(&p.tree, &mut tree_cache))
        .transpose()?
        .unwrap_or_default();
    drop(tree_cache);

    // Track all the blobs we've already backed up and use that set to deduplicate.
    let mut packed_blobs = index::blob_id_set(&index)?;

    let ResumableBackup {
        wip_index,
        cwd_packfiles,
    } = find_resumable(&cached_backend)?.unwrap_or_default();

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
    let backend_config = Arc::new(backend_config);
    let cached_backend = Arc::new(cached_backend);
    let mut backup = spawn_backup_threads(bmode, backend_config, cached_backend.clone(), wip_index);

    let walk_stats = Arc::new(WalkStatistics::default());
    let progress_thread = (!args.quiet).then(|| {
        let s2 = backup.statistics.clone();
        let ws = walk_stats.clone();
        let t = Term::stdout();
        ProgressThread::spawn(move |i| print_progress(i, &t, &s2, &ws))
    });

    // Finish the WIP resume business.
    if !args.dry_run {
        upload_cwd_packfiles(&mut backup.upload_tx, &cwd_packfiles)?;
    }
    drop(cwd_packfiles);

    info!(
        "Backing up {}",
        paths
            .iter()
            .map(|p| p.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let root = backup_tree(
        symlink_behavior,
        &paths,
        &args.skips,
        parent.map(|p| &p.tree),
        &parent_forest,
        &mut packed_blobs,
        &mut backup,
        &walk_stats,
    )?;
    drop(parent_forest);
    drop(packed_blobs);

    // Important: make sure all blobs and the index is written BEFORE
    // we upload the snapshot.
    // It's meaningless unless everything else is there first!
    let stats = backup.join()?;

    progress_thread.map(|h| h.join()).transpose()?;

    debug!("Root tree packed as {}", root);

    info!(
        "{} reused",
        nice_size(walk_stats.reused_bytes.load(Ordering::Relaxed))
    );
    let chunk_bytes = stats.chunk_bytes.load(Ordering::Relaxed);
    let tree_bytes = stats.tree_bytes.load(Ordering::Relaxed);

    let total_bytes = nice_size(chunk_bytes + tree_bytes);
    let chunk_bytes = nice_size(chunk_bytes);
    let tree_bytes = nice_size(tree_bytes);
    info!("{total_bytes} new data ({chunk_bytes} files, {tree_bytes} metadata)");

    let author = match args.author {
        Some(a) => a,
        None => hostname::get()
            .context("Couldn't get hostname")?
            .to_string_lossy()
            .to_string(),
    };

    let time = jiff::Zoned::now().round(jiff::Unit::Second)?;

    let snapshot = Snapshot {
        time,
        author,
        tags: args.tags.into_iter().collect(),
        paths,
        tree: root,
    };
    trace!("{snapshot:?}");

    let snap_id = if !args.dry_run {
        snapshot::upload(&snapshot, &cached_backend)?
    } else {
        let mut hasher = HashingWriter::new(io::sink());
        ciborium::into_writer(&snapshot, &mut hasher)?;
        let (id, _) = hasher.finalize();
        id
    };

    if !args.quiet {
        println!("Snaphsot {snap_id} done");
    }
    Ok(())
}

/// Spit out by our fs walk below
#[derive(Default)]
struct WalkStatistics {
    current_file: Mutex<Utf8PathBuf>,
    reused_bytes: AtomicU64,
}

fn print_progress(
    i: usize,
    term: &Term,
    bstats: &backup::BackupStats,
    wstats: &WalkStatistics,
) -> Result<()> {
    if i > 0 {
        term.clear_last_lines(3)?;
    }

    let rb = wstats.reused_bytes.load(Ordering::Relaxed);
    print_backup_lines(i, bstats, rb);

    let cf: Utf8PathBuf = wstats.current_file.lock().unwrap().clone();
    let cf = truncate_path(&cf, term);
    println!("{cf}");
    Ok(())
}

/// Trees (including the top-level one for each snapshot!) don't store their nodes' absolute paths.
/// This falls apart if given two "foo"s, so yell about that.
///
/// Unfortunate, but see comments in the [`Snapshot`] definition for a discussion of the tradeoffs.
fn reject_matching_directories(paths: &BTreeSet<Utf8PathBuf>) -> Result<()> {
    let mut dirnames: FxHashSet<&str> =
        FxHashSet::with_capacity_and_hasher(paths.len(), Default::default());
    for path in paths {
        let dirname = path.file_name().expect("empty path");
        if !dirnames.insert(dirname) {
            bail!(
                "Backups of directories with matching names ({dirname}/) isn't currently supported",
            );
        }
    }
    Ok(())
}

fn parent_snapshot(
    paths: &BTreeSet<Utf8PathBuf>,
    snapshots: Vec<(Snapshot, ObjectId)>,
) -> Option<Snapshot> {
    let parent = snapshots
        .into_iter()
        .rev()
        .find(|snap| snap.0.paths == *paths);
    match &parent {
        Some(p) => debug!("Using snapshot {} as a parent", p.1),
        None => debug!("No parent snapshot found based on absolute paths"),
    };
    parent.map(|(snap, _)| snap)
}

fn check_paths(symlink_behavior: tree::Symlink, paths: &BTreeSet<Utf8PathBuf>) -> Result<()> {
    info!("Walking {paths:?} to see what we've got...");
    let mut no_op_filter = |_: &Utf8Path| Ok(true);
    let mut no_op_visit =
        |_nope: &mut (),
         _path: &Utf8Path,
         _metadata: tree::NodeMetadata,
         _previous_node: Option<&tree::Node>,
         _entry: fs_tree::DirectoryEntry<()>| { Ok(()) };
    let mut no_op_finalize = |()| Ok(());
    fs_tree::walk_fs(
        symlink_behavior,
        paths,
        None,
        &tree::Forest::default(),
        &mut no_op_filter,
        &mut no_op_visit,
        &mut no_op_finalize,
    )
}

#[expect(clippy::too_many_arguments)] // Stop shame culture
fn backup_tree(
    symlink_behavior: tree::Symlink,
    paths: &BTreeSet<Utf8PathBuf>,
    skips: &[String],
    previous_tree: Option<&ObjectId>,
    previous_forest: &tree::Forest,
    packed_blobs: &mut FxHashSet<ObjectId>,
    backup: &mut Backup,
    walk_stats: &WalkStatistics,
) -> Result<ObjectId> {
    use fs_tree::DirectoryEntry;

    let mf = filter::skip_matching_paths(skips)?;
    let mut filter = move |path: &Utf8Path| {
        let res = mf(path);
        if !res {
            info!("{:>9} {}", "skip", path);
        }
        Ok(res)
    };

    // Both closures need to get at packed_blobs at some point...
    let packed_blobs = RefCell::new(packed_blobs);

    let mut visit = |tree: &mut tree::Tree,
                     path: &Utf8Path,
                     metadata: tree::NodeMetadata,
                     previous_node: Option<&tree::Node>,
                     entry: DirectoryEntry<ObjectId>|
     -> Result<()> {
        *walk_stats.current_file.lock().unwrap() = path.to_owned();
        let subnode = match entry {
            DirectoryEntry::Directory(subtree) => {
                /*
                trace!(
                    "{}{} packed as {}",
                    path,
                    std::path::MAIN_SEPARATOR,
                    subtree
                );
                */
                info!("{:>9} {}{}", "finished", path, std::path::MAIN_SEPARATOR);

                let t = tree::Node {
                    metadata,
                    contents: tree::NodeContents::Directory { subtree },
                };
                t
            }
            DirectoryEntry::Symlink { target } => {
                assert_eq!(symlink_behavior, tree::Symlink::Read);
                info!("{:>9} {}", "symlink", path);

                let t = tree::Node {
                    metadata,
                    contents: tree::NodeContents::Symlink { target },
                };
                t
            }
            DirectoryEntry::UnchangedFile => {
                info!("{:>9} {}", "unchanged", path);

                let rb = metadata.size().expect("files have sizes");
                walk_stats
                    .reused_bytes
                    .fetch_add(rb as u64, Ordering::Relaxed);
                let t = tree::Node {
                    metadata,
                    contents: previous_node.unwrap().contents.clone(),
                };
                t
            }
            DirectoryEntry::ChangedFile => {
                let chunks = chunk::chunk_file(path)?;

                let mut chunk_ids = Vec::new();
                let mut new_chunks = false;
                let mut total_chunks = 0usize;
                for chunk in chunks {
                    chunk_ids.push(chunk.id);

                    if packed_blobs.borrow_mut().insert(chunk.id) {
                        // The first time we get a new chunk, print "backup"
                        if !new_chunks {
                            info!("{:>9} {path}", "backup");
                        }
                        new_chunks = true;
                        backup
                            .chunk_tx
                            .send(chunk)
                            .context("backup -> chunk packer channel exited early")?;
                    } else {
                        walk_stats
                            .reused_bytes
                            .fetch_add(chunk.bytes().len() as u64, Ordering::Relaxed);
                    }
                    total_chunks += 1;
                }
                // We made it through the whole file without finding new data!
                if !new_chunks {
                    info!("{:>9} {path}", "deduped");
                }
                trace!("{path} was {total_chunks} chunks");

                let t = tree::Node {
                    metadata,
                    contents: tree::NodeContents::File { chunks: chunk_ids },
                };
                t
            }
        };
        ensure!(
            // NB: A tree's nodes are named by their relative path from the parent,
            //     not an absolute path. This is an obvious decision,
            //     since storing absolute paths at every level would break all useful comparisons
            //     *AND* waste a lot of data.
            //
            //     What's less obvious is that it ALSO APPLIES TO THE TOP-LEVEL TREE!
            //     Backing up /home/me and /etc will give a top-level tree of
            //     { "me" -> subtree, "etc" -> subtree }, which is why:
            //
            //     1. We store the absolute paths of what we backed up in the snapshot
            //     2. We get mad about top-level names matching - see reject_matching_directories()
            tree.insert(Utf8PathBuf::from(path.file_name().unwrap()), subnode)
                .is_none(),
            "Duplicate tree entries"
        );
        Ok(())
    };

    let mut finalize = |tree: tree::Tree| -> Result<ObjectId> {
        let (bytes, id) = tree::serialize_and_hash(&tree)?;

        if packed_blobs.borrow_mut().insert(id) {
            backup
                .tree_tx
                .send(Blob {
                    contents: blob::Contents::Buffer(bytes),
                    id,
                    kind: blob::Type::Tree,
                })
                .context("backup -> tree packer channel exited early")?;
        } else {
            trace!("tree {} already packed", id);
            walk_stats
                .reused_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        Ok(id)
    };

    fs_tree::walk_fs(
        symlink_behavior,
        paths,
        previous_tree,
        previous_forest,
        &mut filter,
        &mut visit,
        &mut finalize,
    )
}

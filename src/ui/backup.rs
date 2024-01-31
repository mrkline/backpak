use std::cell::RefCell;
use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::mpsc::SyncSender;
use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use chrono::prelude::*;
use clap::Parser;
use log::*;
use rustc_hash::FxHashSet;

use crate::backend;
use crate::blob::{self, Blob};
use crate::chunk;
use crate::fs_tree;
use crate::hashing::ObjectId;
use crate::index;
use crate::snapshot::{self, Snapshot};
use crate::tree;

/// Create a snapshot of the given files and directories.
#[derive(Debug, Parser)]
pub struct Args {
    /// The author of the snapshot (otherwise the hostname is used)
    #[clap(short, long, name = "name")]
    pub author: Option<String>,

    /// Add a metadata tag to the snapshot (can be specified multiple times)
    #[clap(short = 't', long = "tag", name = "tag")]
    pub tags: Vec<String>,

    /// The paths to back up
    ///
    /// These paths are canonicalized into absolute ones.
    /// Snapshots can be restored to either the same absolute paths,
    /// or to a given directory with `restore -o some/dir`
    #[clap(required = true, verbatim_doc_comment)]
    pub paths: Vec<Utf8PathBuf>,
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

    // Do a quick scan of the paths to make sure we can read them and get
    // metadata before we get backends and indexes
    // and threads and all manner of craziness going.
    check_paths(&paths).context("Failed FS check prior to backup")?;

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

    let mut packed_blobs = index::blob_set(&index)?;

    let (maybe_wip_index, maybe_cwd_packfiles) =
        find_resumable_backup(&cached_backend)?.unwrap_or_default();

    for manifest in maybe_wip_index.packs.values() {
        for entry in manifest {
            packed_blobs.insert(entry.id);
        }
    }

    let backend_config = Arc::new(backend_config);
    let cached_backend = Arc::new(cached_backend);
    let mut backup = crate::backup::spawn_backup_threads(
        backend_config,
        cached_backend.clone(),
        maybe_wip_index,
    );

    for p in maybe_cwd_packfiles {
        let name = format!("{p}.pack");
        let fd = std::fs::File::open(&name).with_context(|| format!("Couldn't open {name}"))?;
        backup
            .upload_tx
            .send((name, fd))
            .context("uploader channel exited early")?;
    }

    info!(
        "Backing up {}",
        paths
            .iter()
            .map(|p| p.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let root = backup_tree(
        &paths,
        parent.map(|p| &p.tree),
        &parent_forest,
        &mut packed_blobs,
        &mut backup.chunk_tx,
        &mut backup.tree_tx,
    )?;
    drop(parent_forest);
    drop(packed_blobs);

    // Important: make sure all blobs and indexes are written BEFORE
    // we upload the snapshot.
    // It's meaningless unless everything else is there first!
    backup.join()?;

    debug!("Root tree packed as {}", root);

    let author = match args.author {
        Some(a) => a,
        None => hostname::get()
            .context("Couldn't get hostname")?
            .to_string_lossy()
            .to_string(),
    };

    let now = Local::now();
    // I'm probably missing somthing here;
    // just trying to get the local timezone offset.
    // https://stackoverflow.com/a/59603899
    // .into() converts to UTC (+0)
    let time = now.with_timezone(now.offset());

    let snapshot = Snapshot {
        time,
        author,
        tags: args.tags.into_iter().collect(),
        paths,
        tree: root,
    };

    snapshot::upload(&snapshot, &cached_backend)
}

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

fn check_paths(paths: &BTreeSet<Utf8PathBuf>) -> Result<()> {
    debug!("Walking {paths:?} to check paths and if we can stat");
    let mut no_op_visit =
        |_nope: &mut (),
         _path: &Utf8Path,
         _metadata: tree::NodeMetadata,
         _previous_node: Option<&tree::Node>,
         _entry: fs_tree::DirectoryEntry<()>| { Ok(()) };
    let mut no_op_finalize = |()| Ok(());
    fs_tree::walk_fs(
        paths,
        None,
        &tree::Forest::default(),
        &mut no_op_visit,
        &mut no_op_finalize,
    )
}

fn find_resumable_backup(
    backend: &backend::CachedBackend,
) -> Result<Option<(index::Index, Vec<ObjectId>)>> {
    // NB: If the index is already finished and renamed from the WIP name,
    //     we won't find it here and will start all over.
    //     This is sad, but:
    //
    //     1. We can minimize the chance of that happening by keeping queue sizes down,
    //        which we do.
    //
    //     2. The alternative would be more complicated logic to name the file down the line
    //        (e.g., `upload()` might need a current name & a desired name.
    //
    // This seems worth fixing, though. Maybe have `index()` just return the desired ID,
    // wait for the whole backup thread graph to join, and then upload it right before
    // the snapshot?
    let wip = match index::read_wip()? {
        Some(i) => i,
        None => {
            trace!("No WIP index file found, no backup to resume");
            return Ok(None);
        }
    };
    info!("WIP index file found, resuming backup...");

    debug!("Looking for packfiles that haven't been uploaded...");
    // Since we currently bound the upload channel to size 0,
    // we'll only find at most 1, but that's neither here nor there...
    let cwd_packfiles = find_cwd_packfiles(&wip)?;

    let mut missing_packfiles: FxHashSet<ObjectId> = wip.packs.keys().copied().collect();
    for p in &cwd_packfiles {
        assert!(missing_packfiles.remove(p));
    }

    debug!("Checking backend for other packfiles in the index...");
    // (We want to make sure that everything the index contains is backed up,
    // or just has to be uploaded, so it's a valid starting point).
    let mut errs = false;
    for p in &missing_packfiles {
        if let Err(e) = backend.probe_pack(p) {
            error!("{e}");
            errs = true;
        } else {
            trace!("Found pack {p}");
        }
    }
    if errs {
        bail!("WIP index file references packfiles not backed up or in the working directory.");
    }
    Ok(Some((wip, cwd_packfiles)))
}

fn find_cwd_packfiles(index: &index::Index) -> Result<Vec<ObjectId>> {
    let mut packfiles = vec![];

    for entry in Utf8Path::new(".").read_dir_utf8()? {
        let entry = entry?;
        let name_tokens: Vec<_> = entry.file_name().split('.').collect();
        if name_tokens.len() != 2 || name_tokens[1] != "pack" || !entry.file_type()?.is_file() {
            continue;
        }
        if let Ok(id) = ObjectId::from_str(name_tokens[0]) {
            if index.packs.contains_key(&id) {
                trace!("Found {} in the WIP index", entry.file_name());
                packfiles.push(id);
            } else {
                warn!("Found {} but it isn't in the WIP index", entry.file_name());
            }
        }
    }

    Ok(packfiles)
}

fn backup_tree(
    paths: &BTreeSet<Utf8PathBuf>,
    previous_tree: Option<&ObjectId>,
    previous_forest: &tree::Forest,
    packed_blobs: &mut FxHashSet<ObjectId>,
    chunk_tx: &mut SyncSender<Blob>,
    tree_tx: &mut SyncSender<Blob>,
) -> Result<ObjectId> {
    use fs_tree::DirectoryEntry;

    // Both closures need to get at packed_blobs at some point...
    let packed_blobs = RefCell::new(packed_blobs);

    let mut visit = |tree: &mut tree::Tree,
                     path: &Utf8Path,
                     metadata: tree::NodeMetadata,
                     previous_node: Option<&tree::Node>,
                     entry: DirectoryEntry<ObjectId>|
     -> Result<()> {
        let subnode = match entry {
            DirectoryEntry::Directory(subtree) => {
                trace!(
                    "{}{} packed as {}",
                    path,
                    std::path::MAIN_SEPARATOR,
                    subtree
                );
                info!("{:>9} {}{}", "finished", path, std::path::MAIN_SEPARATOR);

                tree::Node {
                    metadata,
                    contents: tree::NodeContents::Directory { subtree },
                }
            }
            DirectoryEntry::Symlink { target } => {
                info!("{:>9} {}", "symlink", path);

                tree::Node {
                    metadata,
                    contents: tree::NodeContents::Symlink { target },
                }
            }
            DirectoryEntry::UnchangedFile => {
                info!("{:>9} {}", "unchanged", path);

                tree::Node {
                    metadata,
                    contents: previous_node.unwrap().contents.clone(),
                }
            }
            DirectoryEntry::ChangedFile => {
                let chunks = chunk::chunk_file(path)?;

                let mut chunk_ids = Vec::new();
                let num_chunks = chunks.len();
                for (i, chunk) in chunks.into_iter().enumerate() {
                    let i = i + 1; // chunk 1/5, not 0/5
                    chunk_ids.push(chunk.id);

                    if packed_blobs.borrow_mut().insert(chunk.id) {
                        if num_chunks <= 1 {
                            info!("{:>9} {}", "backup", path);
                        } else {
                            info!("{:>9} {} (chunk {}/{})", "backup", path, i, num_chunks);
                        }
                        chunk_tx
                            .send(chunk)
                            .context("backup -> chunk packer channel exited early")?;
                    } else {
                        if num_chunks <= 1 {
                            info!("{:>9} {}", "deduped", path);
                        } else {
                            info!("{:>9} {} (chunk {}/{})", "deduped", path, i, num_chunks);
                        }
                        trace!("chunk {} already packed", chunk.id);
                    }
                }

                tree::Node {
                    metadata,
                    contents: tree::NodeContents::File { chunks: chunk_ids },
                }
            }
        };
        ensure!(
            tree.insert(Utf8PathBuf::from(path.file_name().unwrap()), subnode)
                .is_none(),
            "Duplicate tree entries"
        );
        Ok(())
    };

    let mut finalize = |tree: tree::Tree| -> Result<ObjectId> {
        let (bytes, id) = tree::serialize_and_hash(&tree)?;

        if packed_blobs.borrow_mut().insert(id) {
            tree_tx
                .send(Blob {
                    contents: blob::Contents::Buffer(bytes),
                    id,
                    kind: blob::Type::Tree,
                })
                .context("backup -> tree packer channel exited early")?;
        } else {
            trace!("tree {} already packed", id);
        }
        Ok(id)
    };

    fs_tree::walk_fs(
        paths,
        previous_tree,
        previous_forest,
        &mut visit,
        &mut finalize,
    )
}

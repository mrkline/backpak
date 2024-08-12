use std::cell::RefCell;
use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use chrono::prelude::*;
use clap::Parser;
use regex::RegexSet;
use rustc_hash::FxHashSet;
use tracing::*;

use crate::backend;
use crate::backup::*;
use crate::blob::{self, Blob};
use crate::chunk;
use crate::file_util::nice_size;
use crate::fs_tree;
use crate::hashing::ObjectId;
use crate::index;
use crate::snapshot::{self, Snapshot};
use crate::tree;

/// Create a snapshot of the given files and directories.
#[derive(Debug, Parser)]
pub struct Args {
    /// Dereference symbolic links instead of just saving their target.
    #[clap(short = 'L', long)]
    pub dereference: bool,

    /// The author of the snapshot (otherwise the hostname is used)
    #[clap(short, long, name = "name")]
    pub author: Option<String>,

    /// Add a metadata tag to the snapshot (can be specified multiple times)
    #[clap(short = 't', long = "tag", name = "tag")]
    pub tags: Vec<String>,

    /// Skip anything whose absolute path matches the given regular expression
    #[clap(short = 's', long = "skip", name = "regex")]
    pub skips: Vec<String>,

    #[clap(short = 'n', long)]
    pub dry_run: bool,

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

    let backend_config = Arc::new(backend_config);
    let cached_backend = Arc::new(cached_backend);
    let mut backup = (!args.dry_run)
        .then(|| spawn_backup_threads(backend_config, cached_backend.clone(), wip_index));

    // Finish the WIP resume business.
    if let Some(b) = &mut backup {
        upload_cwd_packfiles(&mut b.upload_tx, &cwd_packfiles)?;
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

    let (root, bytes_reused) = backup_tree(
        symlink_behavior,
        &paths,
        &args.skips,
        parent.map(|p| &p.tree),
        &parent_forest,
        &mut packed_blobs,
        &mut backup,
    )?;
    drop(parent_forest);
    drop(packed_blobs);

    // Important: make sure all blobs and the index is written BEFORE
    // we upload the snapshot.
    // It's meaningless unless everything else is there first!
    let stats = backup.map(|b| b.join()).transpose()?;

    debug!("Root tree packed as {}", root);

    info!("{} reused", nice_size(bytes_reused));
    if let Some(s) = stats {
        let total_bytes = nice_size(s.chunk_bytes + s.tree_bytes);
        let chunk_bytes = nice_size(s.chunk_bytes);
        let tree_bytes = nice_size(s.tree_bytes);
        info!("{total_bytes} new data ({chunk_bytes} files, {tree_bytes} metadata)");
    }

    if !args.dry_run {
        let author = match args.author {
            Some(a) => a,
            None => hostname::get()
                .context("Couldn't get hostname")?
                .to_string_lossy()
                .to_string(),
        };

        // DateTime<Local> -> DateTime<FixedOffset>
        let time: DateTime<FixedOffset> = Local::now().into();

        let snapshot = Snapshot {
            time,
            author,
            tags: args.tags.into_iter().collect(),
            paths,
            tree: root,
        };

        snapshot::upload(&snapshot, &cached_backend)?;
    }
    Ok(())
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

fn check_paths(symlink_behavior: tree::Symlink, paths: &BTreeSet<Utf8PathBuf>) -> Result<()> {
    debug!("Walking {paths:?} to check paths and if we can stat");
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

fn backup_tree(
    symlink_behavior: tree::Symlink,
    paths: &BTreeSet<Utf8PathBuf>,
    skips: &[String],
    previous_tree: Option<&ObjectId>,
    previous_forest: &tree::Forest,
    packed_blobs: &mut FxHashSet<ObjectId>,
    backup: &mut Option<Backup>,
) -> Result<(ObjectId, u64)> {
    use fs_tree::DirectoryEntry;

    let skipset = RegexSet::new(skips).context("Skip rules are not valid regex")?;

    let mut filter = |path: &Utf8Path| {
        // We could lift the filter into fs_tree as a dedicated argument to walk_fs()
        // to avoid getting this file's metadata only to skip it,
        // or worse - walking a directory just to skip it,
        // but for now just make it part of this visiter.
        if skipset.is_match(path.as_str()) {
            info!("{:>9} {}", "skip", path);
            Ok(false)
        } else {
            Ok(true)
        }
    };

    // Both closures need to get at packed_blobs at some point...
    let packed_blobs = RefCell::new(packed_blobs);

    let mut visit = |(tree, bytes_reused): &mut (tree::Tree, u64),
                     path: &Utf8Path,
                     metadata: tree::NodeMetadata,
                     previous_node: Option<&tree::Node>,
                     entry: DirectoryEntry<(ObjectId, u64)>|
     -> Result<()> {
        let (subnode, subnode_bytes_reused) = match entry {
            DirectoryEntry::Directory((subtree, subtree_bytes_reused)) => {
                trace!(
                    "{}{} packed as {}",
                    path,
                    std::path::MAIN_SEPARATOR,
                    subtree
                );
                info!("{:>9} {}{}", "finished", path, std::path::MAIN_SEPARATOR);

                let t = tree::Node {
                    metadata,
                    contents: tree::NodeContents::Directory { subtree },
                };
                (t, subtree_bytes_reused)
            }
            DirectoryEntry::Symlink { target } => {
                assert_eq!(symlink_behavior, tree::Symlink::Read);
                info!("{:>9} {}", "symlink", path);

                let t = tree::Node {
                    metadata,
                    contents: tree::NodeContents::Symlink { target },
                };
                (t, 0)
            }
            DirectoryEntry::UnchangedFile => {
                info!("{:>9} {}", "unchanged", path);

                let reused_bytes = metadata.size().expect("files have sizes");
                let t = tree::Node {
                    metadata,
                    contents: previous_node.unwrap().contents.clone(),
                };
                (t, reused_bytes)
            }
            DirectoryEntry::ChangedFile => {
                let chunks = chunk::chunk_file(path)?;

                let mut chunk_ids = Vec::new();
                let mut reused_bytes = 0;
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
                        if let Some(b) = &backup {
                            b.chunk_tx
                                .send(chunk)
                                .context("backup -> chunk packer channel exited early")?;
                        }
                    } else {
                        reused_bytes += chunk.bytes().len() as u64;
                        if num_chunks <= 1 {
                            info!("{:>9} {}", "deduped", path);
                        } else {
                            info!("{:>9} {} (chunk {}/{})", "deduped", path, i, num_chunks);
                        }
                        trace!("chunk {} already packed", chunk.id);
                    }
                }

                let t = tree::Node {
                    metadata,
                    contents: tree::NodeContents::File { chunks: chunk_ids },
                };
                (t, reused_bytes)
            }
        };
        ensure!(
            tree.insert(Utf8PathBuf::from(path.file_name().unwrap()), subnode)
                .is_none(),
            "Duplicate tree entries"
        );
        *bytes_reused += subnode_bytes_reused;
        Ok(())
    };

    let mut finalize = |(tree, mut bytes_reused): (tree::Tree, u64)| -> Result<(ObjectId, u64)> {
        let (bytes, id) = tree::serialize_and_hash(&tree)?;

        if packed_blobs.borrow_mut().insert(id) {
            if let Some(b) = &backup {
                b.tree_tx
                    .send(Blob {
                        contents: blob::Contents::Buffer(bytes),
                        id,
                        kind: blob::Type::Tree,
                    })
                    .context("backup -> tree packer channel exited early")?;
            }
        } else {
            trace!("tree {} already packed", id);
            bytes_reused += bytes.len() as u64;
        }
        Ok((id, bytes_reused))
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

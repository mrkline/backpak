use std::cell::RefCell;
use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::mpsc::*;
use std::sync::Arc;

use anyhow::*;
use chrono::prelude::*;
use log::*;
use structopt::StructOpt;

use crate::backend;
use crate::blob::{self, Blob};
use crate::chunk;
use crate::fs_tree;
use crate::hashing::ObjectId;
use crate::index;
use crate::snapshot::{self, Snapshot};
use crate::tree;

/// Create a snapshot of the given files and directories.
#[derive(Debug, StructOpt)]
pub struct Args {
    /// The author of the snapshot (otherwise the hostname is used)
    #[structopt(short, long, name = "name", verbatim_doc_comment)]
    pub author: Option<String>,

    /// Add a metadata tag to the snapshot (can be specified multiple times)
    #[structopt(short = "t", long = "tag", name = "tag")]
    pub tags: Vec<String>,

    /// The paths to back up
    #[structopt(required = true)]
    pub paths: Vec<PathBuf>,
}

pub fn run(repository: &Path, args: Args) -> Result<()> {
    // Let's canonicalize our paths (and make sure they're real!)
    // before we spin up a bunch of supporting infrastructure.
    let paths: BTreeSet<PathBuf> = args
        .paths
        .into_iter()
        .map(|p| {
            p.canonicalize()
                .with_context(|| format!("Couldn't canonicalize {}", p.display()))
        })
        .collect::<Result<BTreeSet<PathBuf>>>()?;

    let cached_backend = backend::open(repository)?;

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
        .unwrap_or_else(tree::Forest::new);
    drop(tree_cache);

    // TODO: Load WIP index and upload any existing packs
    // before we start new ones.

    let mut backup =
        crate::backup::spawn_backup_threads(Arc::new(cached_backend), index::Index::default());

    info!(
        "Backing up {}",
        paths
            .iter()
            .map(|p| p.to_string_lossy())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let mut packed_blobs = index::blob_set(&index)?;

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

    snapshot::upload(&snapshot, backup.upload_tx)?;

    drop(backup.chunk_tx);
    drop(backup.tree_tx);

    backup.threads.join().unwrap()
}

fn parent_snapshot(
    paths: &BTreeSet<PathBuf>,
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

pub fn backup_tree(
    paths: &BTreeSet<PathBuf>,
    previous_tree: Option<&ObjectId>,
    previous_forest: &tree::Forest,
    packed_blobs: &mut HashSet<ObjectId>,
    chunk_tx: &mut Sender<Blob>,
    tree_tx: &mut Sender<Blob>,
) -> Result<ObjectId> {
    use fs_tree::DirectoryEntry;

    // Both closures need to get at packed_blobs at some point...
    let packed_blobs = RefCell::new(packed_blobs);

    let mut visit = |tree: &mut tree::Tree,
                     path: &Path,
                     metadata: tree::NodeMetadata,
                     previous_node: Option<&tree::Node>,
                     entry: DirectoryEntry<ObjectId>|
     -> Result<()> {
        let subnode = match entry {
            DirectoryEntry::Directory(subtree) => {
                trace!(
                    "{}{} packed as {}",
                    path.display(),
                    std::path::MAIN_SEPARATOR,
                    subtree
                );
                info!("finished {}{}", path.display(), std::path::MAIN_SEPARATOR);

                tree::Node {
                    metadata,
                    contents: tree::NodeContents::Directory { subtree },
                }
            }
            DirectoryEntry::Symlink { target } => {
                info!("{:>8} {}", "symlink", path.display());

                tree::Node {
                    metadata,
                    contents: tree::NodeContents::Symlink { target },
                }
            }
            DirectoryEntry::UnchangedFile => {
                info!("{:>8} {}", "skip", path.display());

                tree::Node {
                    metadata,
                    contents: previous_node.unwrap().contents.clone(),
                }
            }
            DirectoryEntry::ChangedFile => {
                let chunks = chunk::chunk_file(&path)?;

                let mut chunk_ids = Vec::new();
                for chunk in chunks {
                    chunk_ids.push(chunk.id);

                    if packed_blobs.borrow_mut().insert(chunk.id) {
                        chunk_tx
                            .send(chunk)
                            .context("backup -> chunk packer channel exited early")?;
                    } else {
                        trace!("chunk {} already packed", chunk.id);
                    }
                }
                info!("{:>8} {}", "backup", path.display());

                tree::Node {
                    metadata,
                    contents: tree::NodeContents::File { chunks: chunk_ids },
                }
            }
        };
        ensure!(
            tree.insert(PathBuf::from(path.file_name().unwrap()), subnode)
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

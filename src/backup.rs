use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc::*;
use std::sync::{Arc, Mutex};
use std::thread;

use anyhow::*;
use chrono::prelude::*;
use log::*;
use structopt::StructOpt;

use crate::backend;
use crate::chunk;
use crate::hashing::ObjectId;
use crate::index;
use crate::pack;
use crate::snapshot;
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

    info!("Opening {}", repository.display());
    let cached_backend = backend::open(repository)?;

    info!("Building index of backed-up blobs");
    let index = index::build_master_index(&cached_backend)?;

    // TODO: Load WIP index and upload any existing packs
    // before we start new ones.

    let blob_set = Arc::new(Mutex::new(index::blob_set(&index)?));

    let (mut chunk_tx, chunk_rx) = channel();
    let (mut tree_tx, tree_rx) = channel();
    let (chunk_pack_tx, pack_rx) = channel();
    let tree_pack_tx = chunk_pack_tx.clone();
    let (chunk_pack_upload_tx, upload_rx) = sync_channel(1);
    let tree_pack_upload_tx = chunk_pack_upload_tx.clone();
    let index_upload_tx = chunk_pack_upload_tx.clone();
    let snapshot_upload_tx = chunk_pack_upload_tx.clone();

    let mut cached_backend = backend::open(repository)?;

    let tree_set = blob_set.clone(); // We need an Arc clone for the tree packer

    let chunk_packer =
        thread::spawn(move || pack::pack(chunk_rx, chunk_pack_tx, chunk_pack_upload_tx, blob_set));
    let tree_packer =
        thread::spawn(move || pack::pack(tree_rx, tree_pack_tx, tree_pack_upload_tx, tree_set));
    let indexer = thread::spawn(move || index::index(pack_rx, index_upload_tx));
    let uploader = thread::spawn(move || upload(&mut cached_backend, upload_rx));

    let root = pack_tree(&paths, &mut chunk_tx, &mut tree_tx)?;

    let author = match args.author {
        Some(a) => a,
        None => hostname::get()
            .context("Couldn't get hostname")?
            .to_string_lossy()
            .to_string(),
    };

    let snapshot = snapshot::Snapshot {
        time: Local::now().into(),
        author,
        tags: args.tags.into_iter().collect(),
        paths,
        tree: root,
    };

    snapshot::upload(&snapshot, snapshot_upload_tx)?;

    drop(chunk_tx);
    drop(tree_tx);

    uploader.join().unwrap()?;
    chunk_packer.join().unwrap()?;
    tree_packer.join().unwrap()?;
    indexer.join().unwrap()?;
    Ok(())
}

fn pack_tree(
    paths: &BTreeSet<PathBuf>,
    chunk_tx: &mut Sender<pack::Blob>,
    tree_tx: &mut Sender<pack::Blob>,
) -> Result<ObjectId> {
    let mut nodes = tree::Tree::new();

    for path in paths {
        // TOCTOU? Is that better than opening the file and changing
        // its access time? Maybe, but we also might use the metadata
        // as criteria to skip the file once we build out more efficient
        // snapshotting.
        let metadata = tree::get_metadata(path)?;

        let node = if path.is_dir() {
            // Gather the dir entries in `path`, call pack_tree with them,
            // and add an entry to `nodes` for the subtree.
            let subpaths = fs::read_dir(path)?
                .map(|entry| entry.map(|e| e.path()))
                .collect::<io::Result<BTreeSet<PathBuf>>>()
                .with_context(|| format!("Failed iterating subdirectory {}", path.display()))?;

            let subtree: ObjectId = pack_tree(&subpaths, chunk_tx, tree_tx)
                .with_context(|| format!("Failed to pack subdirectory {}", path.display()))?;

            tree::Node {
                metadata,
                contents: tree::NodeContents::Directory { subtree },
            }
        } else {
            let chunks = chunk::chunk_file(&path)?;
            let length = chunks.iter().map(|c| c.len() as u64).sum();
            let mut chunk_ids = Vec::new();
            for chunk in chunks {
                chunk_ids.push(chunk.id);
                chunk_tx
                    .send(pack::Blob::Chunk(chunk))
                    .context("backup -> chunk packer channel exited early")?;
            }
            tree::Node {
                metadata,
                contents: tree::NodeContents::File {
                    chunks: chunk_ids,
                    length,
                },
            }
        };
        ensure!(
            nodes
                .insert(
                    PathBuf::from(path.file_name().expect("Given path ended in ..")),
                    node
                )
                .is_none(),
            "Duplicate tree entries"
        );
    }
    let (bytes, id) = tree::serialize_and_hash(&nodes)?;
    tree_tx
        .send(pack::Blob::Tree { bytes, id })
        .context("backup -> tree packer channel exited early")?;
    Ok(id)
}

fn upload(cached_backend: &mut backend::CachedBackend, rx: Receiver<(String, File)>) -> Result<()> {
    while let Ok((path, fh)) = rx.recv() {
        cached_backend.write(&path, fh)?;
    }
    Ok(())
}

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::mpsc::*;
use std::sync::{Arc, Mutex};
use std::thread;

use anyhow::*;
use chrono::prelude::*;
use log::*;
use structopt::StructOpt;

use crate::backend;
use crate::hashing::ObjectId;
use crate::index;
use crate::pack;
use crate::snapshot::{self, Snapshot};
use crate::tree;

mod walk;

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
    let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);
    let snapshots = snapshot::load_chronologically(&cached_backend)?;
    let parent = parent_snapshot(&paths, &snapshots);

    // TODO: Load WIP index and upload any existing packs
    // before we start new ones.

    let blob_set = Arc::new(Mutex::new(index::blob_set(&index)?));

    // ALL THE CONCURRENCY
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
    let indexer = thread::spawn(move || index::index(HashSet::new(), pack_rx, index_upload_tx));
    let uploader = thread::spawn(move || upload(&mut cached_backend, upload_rx));

    let root = walk::pack_tree(
        &paths,
        parent.map(|p| p.tree),
        &mut tree_cache,
        &mut chunk_tx,
        &mut tree_tx,
    )?;
    debug!("Root tree packed as {}", root);

    let author = match args.author {
        Some(a) => a,
        None => hostname::get()
            .context("Couldn't get hostname")?
            .to_string_lossy()
            .to_string(),
    };

    let snapshot = Snapshot {
        time: Local::now().into(),
        author,
        tags: args.tags.into_iter().collect(),
        paths,
        tree: root,
    };

    snapshot::upload(&snapshot, snapshot_upload_tx)?;

    drop(chunk_tx);
    drop(tree_tx);

    let mut errors: Vec<anyhow::Error> = Vec::new();
    let mut append_error = |result: Option<anyhow::Error>| {
        if let Some(e) = result {
            errors.push(e);
        }
    };

    append_error(uploader.join().unwrap().err());
    append_error(chunk_packer.join().unwrap().err());
    append_error(tree_packer.join().unwrap().err());
    append_error(indexer.join().unwrap().err());

    if errors.is_empty() {
        Ok(())
    } else {
        for e in errors {
            error!("{:?}", e);
        }
        bail!("backup failed");
    }
}

fn parent_snapshot<'a>(
    paths: &BTreeSet<PathBuf>,
    snapshots: &'a [(Snapshot, ObjectId)],
) -> Option<&'a Snapshot> {
    let parent = snapshots.iter().rev().find(|snap| snap.0.paths == *paths);
    match parent {
        Some(p) => debug!("Using snapshot {} as a parent", p.1),
        None => debug!("No parent snapshot found based on absolute paths"),
    };
    parent.map(|(snap, _)| snap)
}

fn upload(cached_backend: &mut backend::CachedBackend, rx: Receiver<(String, File)>) -> Result<()> {
    while let Ok((path, fh)) = rx.recv() {
        cached_backend.write(&path, fh)?;
    }
    Ok(())
}

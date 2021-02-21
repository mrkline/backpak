use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc::*;
use std::sync::{Arc, Mutex};

use anyhow::*;
use chrono::prelude::*;
use log::*;
use structopt::StructOpt;

use crate::backend;
use crate::hashing::ObjectId;
use crate::index;
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

    let blob_set = Arc::new(Mutex::new(index::blob_set(&index)?));

    let mut backup =
        crate::backup::spawn_backup_threads(Arc::new(cached_backend), blob_set, index::Index::default());

    let root = walk::pack_tree(
        &paths,
        parent.map(|p| &p.tree),
        &parent_forest,
        &mut backup.chunk_tx,
        &mut backup.tree_tx,
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

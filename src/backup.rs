use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::prelude::*;
use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::mpsc::*;
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
    #[structopt(short, long)]
    pub author: Option<String>,

    #[structopt(short = "t", long = "tag")]
    pub tags: Vec<String>,

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

    let (mut chunk_tx, chunk_rx) = channel();
    let (mut tree_tx, tree_rx) = channel();
    let (chunk_pack_tx, pack_rx) = channel();
    let tree_pack_tx = chunk_pack_tx.clone();
    let (chunk_pack_upload_tx, upload_rx) = sync_channel(1);
    let tree_pack_upload_tx = chunk_pack_upload_tx.clone();
    let index_upload_tx = chunk_pack_upload_tx.clone();
    let snapshot_upload_tx = chunk_pack_upload_tx.clone();

    let mut backend = backend::open(repository)?;

    // TODO: Get these paths out of config? Some constants in a shared module?
    let chunk_packer =
        thread::spawn(move || pack::pack(chunk_rx, chunk_pack_tx, chunk_pack_upload_tx));
    let tree_packer = thread::spawn(move || pack::pack(tree_rx, tree_pack_tx, tree_pack_upload_tx));
    let indexer = thread::spawn(move || index::index(pack_rx, index_upload_tx));
    let uploader = thread::spawn(move || upload(&mut *backend, upload_rx));

    // TODO: The ID of the tree root is what we reference in the snapshot.
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

    // TODO: Join errors together so that we don't just get errors from
    // the first one of these to fail.
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

fn upload(backend: &mut dyn backend::Backend, rx: Receiver<(String, File)>) -> Result<()> {
    while let Ok((path, mut fh)) = rx.recv() {
        fh.seek(SeekFrom::Start(0))?;
        let to = backend::destination(&path);
        backend.write(&mut fh, &to)?;
        debug!("Backed up {}. Removing temp copy", path);
        fs::remove_file(&path).with_context(|| format!("Couldn't remove {}", path))?
    }
    Ok(())
}

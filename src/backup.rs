use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::mpsc::*;
use std::thread;

use anyhow::*;
use log::*;
use structopt::StructOpt;

use crate::backend;
use crate::chunk;
use crate::index;
use crate::pack;
use crate::tree;

/// Create a new mod directory here (or wherever -C gave)
#[derive(Debug, StructOpt)]
pub struct Args {
    files: Vec<PathBuf>,
}

pub fn run(repository: &str, args: Args) -> Result<()> {
    let (chunk_tx, chunk_rx) = channel();
    let (tree_tx, tree_rx) = channel();
    let (chunk_pack_tx, pack_rx) = channel();
    let tree_pack_tx = chunk_pack_tx.clone();
    let (chunk_pack_upload_tx, upload_rx) = sync_channel(1);
    let tree_pack_upload_tx = chunk_pack_upload_tx.clone();
    let index_upload_tx = chunk_pack_upload_tx.clone();

    let mut backend = backend::open(repository)?;

    // TODO: Get these paths out of config? Some constants in a shared module?
    let chunk_packer = thread::spawn(move || {
        pack::pack(
            "temp-chunks.pack",
            chunk_rx,
            chunk_pack_tx,
            chunk_pack_upload_tx,
        )
    });
    let tree_packer = thread::spawn(move || {
        pack::pack(
            "temp-trees.pack",
            tree_rx,
            tree_pack_tx,
            tree_pack_upload_tx,
        )
    });
    let indexer = thread::spawn(move || index::index(pack_rx, index_upload_tx));
    let uploader = thread::spawn(move || upload(&mut *backend, upload_rx));

    let tree = pack_tree(&args.files, chunk_tx)?;
    tree_tx
        .send(pack::Blob::Tree(tree))
        .expect("backup -> tree packer channel exited early");
    drop(tree_tx);

    chunk_packer.join().unwrap()?;
    tree_packer.join().unwrap()?;
    indexer.join().unwrap()?;
    uploader.join().unwrap()?;
    Ok(())
}

fn pack_tree(paths: &[PathBuf], tx: Sender<pack::Blob>) -> Result<tree::Tree> {
    let mut nodes = tree::Tree::new();

    for path in paths {
        if path.is_dir() {
        } else {
            // TOCTOU? Is that better than opening the file and changing
            // its access time? Maybe, but we also might use the metadata
            // as criteria to skip the file once we build out more efficient
            // snapshotting.
            let metadata = tree::get_metadata(path)?;

            let mut contents = Vec::new();
            for chunk in chunk::chunk_file(&path)? {
                contents.push(chunk.id);
                tx.send(pack::Blob::Chunk(chunk))
                    .context("backup -> chunk packer channel exited early")?;
            }
            ensure!(
                nodes
                    .insert(
                        PathBuf::from(path.file_name().expect("Given path ended in ..")),
                        tree::Node {
                            metadata,
                            contents: tree::NodeContents::File { contents }
                        }
                    )
                    .is_none(),
                "Duplicate tree entries"
            );
        }
    }
    Ok(nodes)
}

fn upload(backend: &mut dyn backend::Backend, rx: Receiver<String>) -> Result<()> {
    while let Ok(path) = rx.recv() {
        let mut fh =
            File::open(&path).with_context(|| format!("Couldn't open {} for upload", path))?;
        let to = backend::destination(&path);
        backend.write(&mut fh, &to)?;
        debug!("Backed up {}. Removing temp copy", path);
        fs::remove_file(&path).with_context(|| format!("Couldn't remove {}", path))?
    }
    Ok(())
}

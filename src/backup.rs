use std::ffi::OsStr;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::mpsc::*;
use std::thread;

use anyhow::*;
use log::*;
use structopt::StructOpt;

use crate::backend;
use crate::chunk;
use crate::index;
use crate::pack;
use crate::tree::*;

/// Create a new mod directory here (or wherever -C gave)
#[derive(Debug, StructOpt)]
pub struct Args {
    files: Vec<PathBuf>,
}

pub fn run(repository: &str, args: Args) -> Result<()> {
    let (mut blob_tx, blob_rx) = channel();
    let (pack_tx, pack_rx) = channel();
    let (pack_upload_tx, upload_rx) = sync_channel(1);
    let index_upload_tx = pack_upload_tx.clone();

    let mut backend = backend::open(repository)?;

    let packer = thread::spawn(move || pack::pack(blob_rx, pack_tx, pack_upload_tx));
    let indexer = thread::spawn(move || index::index(pack_rx, index_upload_tx));
    let uploader = thread::spawn(move || upload(&mut *backend, upload_rx));

    let tree = pack_tree(&args.files, &mut blob_tx)?;
    blob_tx
        .send(pack::Blob::Tree(tree))
        .expect("backup -> packer channel exited early");
    drop(blob_tx);

    packer.join().unwrap()?;
    indexer.join().unwrap()?;
    uploader.join().unwrap()?;
    Ok(())
}

fn pack_tree(paths: &[PathBuf], tx: &mut Sender<pack::Blob>) -> Result<Tree> {
    let mut nodes = Vec::new();

    for path in paths {
        if path.is_dir() {
        } else {
            let mut content = Vec::new();
            for chunk in chunk::chunk_file(&path)? {
                content.push(chunk.id);
                tx.send(pack::Blob::Chunk(chunk))
                    .context("backup -> packer channel exited early")?;
            }
            nodes.push(Node {
                name: PathBuf::from(path.file_name().expect("Given path ended in ..")),
                node_type: NodeType::File { content },
            });
        }
    }
    Ok(Tree { nodes })
}

fn upload(backend: &mut dyn backend::Backend, rx: Receiver<String>) -> Result<()> {
    while let Ok(path) = rx.recv() {
        let mut fh =
            File::open(&path).with_context(|| format!("Couldn't open {} for upload", path))?;
        let to = destination(&path);
        backend.write(&mut fh, &to)?;
        debug!("Backed up {}. Removing temp copy", path);
        fs::remove_file(&path).with_context(|| format!("Couldn't remove {}", path))?
    }
    Ok(())
}

fn destination(src: &str) -> String {
    match Path::new(src).extension().and_then(OsStr::to_str) {
        Some("pack") => format!("packs/{}/{}", &src[0..2], src),
        Some("index") => format!("indexes/{}", src),
        _ => panic!("Unexpected extension on file to upload: {}", src),
    }
}

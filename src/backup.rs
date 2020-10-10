use std::path::PathBuf;
use std::sync::mpsc::{channel, Sender};
use std::thread;

use anyhow::Result;
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

    let backend = backend::open(repository)?;

    let packer = thread::spawn(move || pack::pack(blob_rx, pack_tx));
    let indexer = thread::spawn(move || index::index(pack_rx));

    let tree = pack_tree(&args.files, &mut blob_tx)?;
    blob_tx
        .send(pack::Blob::Tree(tree))
        .expect("Packer exited early");
    drop(blob_tx);

    packer.join().unwrap()?;
    indexer.join().unwrap()?;
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
                    .expect("Packer exited early");
            }
            nodes.push(Node {
                name: PathBuf::from(path.file_name().expect("Given path ended in ..")),
                node_type: NodeType::File { content },
            });
        }
    }
    Ok(Tree { nodes })
}

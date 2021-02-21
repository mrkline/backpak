//! The backup machinery, decoupled from what needs to be backed up.
//!
//! Various commands (backup, prune, etc.) can walk data, existing or new,
//! and send them to this machinery.

use std::collections::HashSet;
use std::fs::File;
use std::sync::mpsc::*;
use std::sync::{Arc, Mutex};
use std::thread;

use anyhow::*;
use log::*;

use crate::backend;
use crate::blob::Blob;
use crate::hashing::ObjectId;
use crate::index;
use crate::pack;
use crate::upload;

pub struct Backup {
    pub chunk_tx: Sender<Blob>,
    pub tree_tx: Sender<Blob>,
    pub upload_tx: SyncSender<(String, File)>,
    pub threads: thread::JoinHandle<Result<()>>,
}

impl Backup {
    /// Convenience function to join the threads
    /// assuming the channels haven't been moved out.
    pub fn join(self) -> Result<()> {
        drop(self.chunk_tx);
        drop(self.tree_tx);
        drop(self.upload_tx);
        self.threads.join().unwrap()
    }
}

pub fn spawn_backup_threads(
    cached_backend: Arc<backend::CachedBackend>,
    existing_blobs: Arc<Mutex<HashSet<ObjectId>>>,
    starting_index: index::Index,
) -> Backup {
    let (chunk_tx, chunk_rx) = channel();
    let (tree_tx, tree_rx) = channel();
    let (upload_tx, upload_rx) = sync_channel(1);
    let upload_tx2 = upload_tx.clone();

    let threads = thread::Builder::new()
        .name(String::from("backup master"))
        .spawn(move || {
            backup_master_thread(
                chunk_rx,
                tree_rx,
                upload_tx2,
                upload_rx,
                cached_backend,
                existing_blobs,
                starting_index,
            )
        })
        .unwrap();

    Backup {
        chunk_tx,
        tree_tx,
        upload_tx,
        threads,
    }
}

fn backup_master_thread(
    chunk_rx: Receiver<Blob>,
    tree_rx: Receiver<Blob>,
    upload_tx: SyncSender<(String, File)>,
    upload_rx: Receiver<(String, File)>,
    cached_backend: Arc<backend::CachedBackend>,
    existing_blobs: Arc<Mutex<HashSet<ObjectId>>>,
    starting_index: index::Index,
) -> Result<()> {
    // ALL THE CONCURRENCY
    let (chunk_pack_tx, pack_rx) = channel();
    let tree_pack_tx = chunk_pack_tx.clone();
    let chunk_pack_upload_tx = upload_tx;
    let tree_pack_upload_tx = chunk_pack_upload_tx.clone();
    let index_upload_tx = chunk_pack_upload_tx.clone();

    // We need an Arc clone for the tree packer
    let existing_blobs2 = existing_blobs.clone();

    let chunk_packer = thread::Builder::new()
        .name(String::from("chunk packer"))
        .spawn(move || {
            pack::pack(
                chunk_rx,
                chunk_pack_tx,
                chunk_pack_upload_tx,
                existing_blobs,
            )
        })
        .unwrap();

    let tree_packer = thread::Builder::new()
        .name(String::from("tree packer"))
        .spawn(move || pack::pack(tree_rx, tree_pack_tx, tree_pack_upload_tx, existing_blobs2))
        .unwrap();

    let indexer = thread::Builder::new()
        .name(String::from("indexer"))
        .spawn(move || index::index(starting_index, pack_rx, index_upload_tx))
        .unwrap();

    let uploader = thread::Builder::new()
        .name(String::from("uploader"))
        .spawn(move || upload::upload(&*cached_backend, upload_rx))
        .unwrap();

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

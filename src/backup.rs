//! Common backup machinery exposed as channel-chomping tasks
//!
//! Various commands (backup, prune, etc.) can walk data, existing or new,
//! and send them to this machinery.

use std::fs::File;
use std::sync::Arc;

use anyhow::*;
use log::*;
use tokio::sync::mpsc::{
    channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender,
};
use tokio::task::{spawn, JoinHandle};

use crate::backend;
use crate::blob::Blob;
use crate::index;
use crate::pack;
use crate::upload;

pub struct Backup {
    pub chunk_tx: UnboundedSender<Blob>,
    pub tree_tx: UnboundedSender<Blob>,
    pub upload_tx: Sender<(String, File)>,
    pub tasks: JoinHandle<Result<()>>,
}

impl Backup {
    /// Convenience function to join the tasks
    /// assuming the channels haven't been moved out.
    pub async fn join(self) -> Result<()> {
        drop(self.chunk_tx);
        drop(self.tree_tx);
        drop(self.upload_tx);
        self.tasks.await.unwrap()
    }
}

pub fn spawn_backup_tasks(
    cached_backend: Arc<backend::CachedBackend>,
    starting_index: index::Index,
) -> Backup {
    let (chunk_tx, chunk_rx) = unbounded_channel();
    let (tree_tx, tree_rx) = unbounded_channel();
    let (upload_tx, upload_rx) = channel(1);
    let upload_tx2 = upload_tx.clone();

    let tasks = spawn(backup_master_task(
        chunk_rx,
        tree_rx,
        upload_tx2,
        upload_rx,
        cached_backend,
        starting_index,
    ));

    Backup {
        chunk_tx,
        tree_tx,
        upload_tx,
        tasks,
    }
}

async fn backup_master_task(
    chunk_rx: UnboundedReceiver<Blob>,
    tree_rx: UnboundedReceiver<Blob>,
    upload_tx: Sender<(String, File)>,
    upload_rx: Receiver<(String, File)>,
    cached_backend: Arc<backend::CachedBackend>,
    starting_index: index::Index,
) -> Result<()> {
    // ALL THE CONCURRENCY
    let (chunk_pack_tx, pack_rx) = unbounded_channel();
    let tree_pack_tx = chunk_pack_tx.clone();
    let chunk_pack_upload_tx = upload_tx;
    let tree_pack_upload_tx = chunk_pack_upload_tx.clone();
    let index_upload_tx = chunk_pack_upload_tx.clone();

    let chunk_packer = spawn(pack::pack(chunk_rx, chunk_pack_tx, chunk_pack_upload_tx));

    let tree_packer = spawn(pack::pack(tree_rx, tree_pack_tx, tree_pack_upload_tx));

    let indexer = spawn(index::index(starting_index, pack_rx, index_upload_tx));

    let uploader = spawn(async move { upload::upload(&*cached_backend, upload_rx).await });

    let mut errors: Vec<anyhow::Error> = Vec::new();
    let mut append_error = |result: Option<anyhow::Error>| {
        if let Some(e) = result {
            errors.push(e);
        }
    };

    append_error(uploader.await.unwrap().err());
    append_error(chunk_packer.await.unwrap().err());
    append_error(tree_packer.await.unwrap().err());
    append_error(indexer.await.unwrap().err());

    if errors.is_empty() {
        Ok(())
    } else {
        for e in errors {
            error!("{:?}", e);
        }
        bail!("backup failed");
    }
}

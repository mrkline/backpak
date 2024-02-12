//! Common backup machinery exposed as channel-chomping threads
//!
//! As a crappy diagram, [`spawn_backup_threads()`] spins up:
//!
//! ```text
//!     chunk_tx --blobs--> chunk packer --files------->---------
//!                                     \                        \
//!                                    ---manifests--> indexer ---> uploader
//!                                   /                          /
//!     tree_tx --blobs--> tree packer---files--------->---------
//! ```
//!
//! What's going on?
//!
//! - While both are stored as a [`Blob`], it's very convenient to store chunks
//!   (of files we're backing up) in separate packs from trees (dir structure & metadata).
//!   Many operations (ls, diff) only need to look at the trees, and having trees
//!   in the same packs gives us great locality.
//!   See [`tree::Cache`](crate::tree::Cache) - whenever we pull down a pack of trees,
//!   we read them all and insert them into the cache.
//!
//! - Each packer takes [`Blob`]s and inserts them into pack files,
//!   compressed streams of blobs with a [`PackManifest`](crate::pack::PackManifest)
//!   at the end for quick indexing. Pack files are filled until they reach a certain size.
//!
//! - When each pack file is finished, its hash
//!   (i.e., its [`ObjectId`](crate::hashing::ObjectId)!) and manifest are sent
//!   to the indexer. Each backup creates a single index file that contains
//!   an [`Index`](crate::index::Index) which maps pack IDs to their manifests.
//!   (We can also pass a starting index containing previously existing packs.
//!   This isn't necessary for a normal backup, since
//!   [`build_master_index()`](crate::index::build_master_index) merges all
//!   backed-up indexes, but it's useful for pruning or resuming an interrupted
//!   backup session.)
//!
//! - Each of these threads ultimately generate files which need to be... backed up!
//!   That's the job of the uploader thread, which receives each in turn
//!   (still open, to avoid filesystem races and perf hits from closing and reopening)
//!   and uploads them to the current [`CachedBackend`](crate::backend::CachedBackend).
//!
//! That's it! To back up a snapshot, the [backup command](crate::ui::backup)
//! walks the parts of the filesystem we want to back up, sending chunks of files
//! to the file packer and trees to the tree packer. To prune the backup store,
//! the [prune command](crate::ui::prune) builds lists of packs that are
//! used/unused/partially used, starts a new index pre-populated with the fully-used
//! packs (passing it to the indexer as a starting point), then feeds blobs from
//! the partially-used packs to the packers for compaction. And so on, and so forth.

use std::fs::{self, File};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::thread;

use anyhow::{bail, Context, Result};
use log::*;

use crate::backend;
use crate::blob::Blob;
use crate::index;
use crate::pack;
use crate::upload;

pub struct Backup {
    pub chunk_tx: SyncSender<Blob>,
    pub tree_tx: SyncSender<Blob>,
    pub upload_tx: SyncSender<(String, File)>,
    threads: thread::JoinHandle<Result<()>>,
}

impl Backup {
    /// Convenience function to join the threads
    /// assuming the channels haven't been moved out.
    pub fn join(self) -> Result<()> {
        drop(self.chunk_tx);
        drop(self.tree_tx);
        drop(self.upload_tx);
        self.threads.join().unwrap()?;

        // If everything exited cleanly, we uploaded the new index.
        // We can axe the WIP one, which we kept around until now to make sure we're resumable.
        match fs::remove_file(index::WIP_NAME) {
            // Well, unless there was zero new data,
            // in which case we didn't create a new index.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Ok(())
            }
            otherwise => otherwise
        }.with_context(|| format!("Couldn't remove {}", index::WIP_NAME))?;
        Ok(())
    }
}

pub fn spawn_backup_threads(
    backend_config: Arc<backend::Config>,
    cached_backend: Arc<backend::CachedBackend>,
    starting_index: index::Index,
) -> Backup {
    // Channels are all handoffs holding no elements - this simplifies reasoning about:
    // - When data is flowing through the system
    // - When some tasks are waiting on others
    // - In turn, how well we've broken up all our work into different threads.
    //
    // We can revisit this if profiling shows us spending a lot of time sleeping/waking
    // that could be eased by adding some slack in the channels.

    let (chunk_tx, chunk_rx) = sync_channel(0);
    let (tree_tx, tree_rx) = sync_channel(0);
    let (upload_tx, upload_rx) = sync_channel(0);
    let upload_tx2 = upload_tx.clone();

    let threads = thread::Builder::new()
        .name(String::from("backup master"))
        .spawn(move || {
            backup_master_thread(
                chunk_rx,
                tree_rx,
                upload_tx2,
                upload_rx,
                backend_config,
                cached_backend,
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
    backend_config: Arc<backend::Config>,
    cached_backend: Arc<backend::CachedBackend>,
    starting_index: index::Index,
) -> Result<()> {
    // ALL THE CONCURRENCY

    // We shouldn't be swamped with a bunch of indexes at once since packing is the slow part,
    // and we only have two packers (chunks and trees) feeding this.
    let (chunk_index_tx, index_rx) = sync_channel(0);
    let tree_index_tx = chunk_index_tx.clone();
    let chunk_pack_upload_tx = upload_tx;
    let tree_pack_upload_tx = chunk_pack_upload_tx.clone();
    let index_upload_tx = chunk_pack_upload_tx.clone();
    let pack_size = backend_config.pack_size;

    let chunk_packer = thread::Builder::new()
        .name(String::from("chunk packer"))
        .spawn(move || pack::pack(pack_size, chunk_rx, chunk_index_tx, chunk_pack_upload_tx))
        .unwrap();

    let tree_packer = thread::Builder::new()
        .name(String::from("tree packer"))
        .spawn(move || pack::pack(pack_size, tree_rx, tree_index_tx, tree_pack_upload_tx))
        .unwrap();

    let indexer = thread::Builder::new()
        .name(String::from("indexer"))
        .spawn(move || index::index(starting_index, index_rx, index_upload_tx))
        .unwrap();

    let uploader = thread::Builder::new()
        .name(String::from("uploader"))
        .spawn(move || upload::upload(&cached_backend, upload_rx))
        .unwrap();

    let mut errors: Vec<anyhow::Error> = Vec::new();
    let mut append_error = |thread: &'static str, result: Option<anyhow::Error>| {
        if let Some(e) = result {
            errors.push(e.context(thread));
        }
    };

    append_error("Packing chunks failed", chunk_packer.join().unwrap().err());
    append_error("Packing trees failed", tree_packer.join().unwrap().err());
    append_error("Indexing failed", indexer.join().unwrap().err());
    append_error("Uploading failed", uploader.join().unwrap().err());

    if errors.is_empty() {
        Ok(())
    } else {
        for e in errors {
            error!("{:?}", e);
        }
        bail!("backup failed");
    }
}

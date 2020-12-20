use std::collections::BTreeSet;
use std::path::Path;
use std::sync::mpsc::*;
use std::thread;

use anyhow::*;
use log::*;
use rayon::prelude::*;

use crate::backend;
use crate::hashing::ObjectId;
use crate::index;
use crate::pack;
use crate::upload;

pub fn run(repository: &Path) -> Result<()> {
    let mut cached_backend = backend::open(repository)?;

    let superseded = cached_backend
        .backend
        .list_indexes()?
        .iter()
        .map(|index_file| backend::id_from_path(index_file))
        .collect::<Result<BTreeSet<ObjectId>>>()?;

    let (pack_tx, pack_rx) = channel();
    let (upload_tx, upload_rx) = sync_channel(1);

    info!("Reading all packs to build a new index");
    cached_backend
        .backend
        .list_packs()?
        .par_iter()
        .try_for_each_with::<_, _, Result<()>>(pack_tx, |pack_tx, pack_file| {
            let id = backend::id_from_path(pack_file)?;
            let manifest = pack::load_manifest(&id, &cached_backend)?;
            let metadata = pack::PackMetadata { id, manifest };
            pack_tx
                .send(metadata)
                .context("Pack thread closed unexpectedly")?;
            Ok(())
        })?;

    let replacing = superseded.clone();
    let indexer = thread::spawn(move || index::index(replacing, pack_rx, upload_tx));

    upload::upload(&mut cached_backend, upload_rx)?;

    indexer.join().unwrap()?;

    info!("Uploaded a new index; removing previous ones");
    for old_index in superseded {
        debug!("Removing {}", old_index);
        cached_backend.remove_index(&old_index)?;
    }

    Ok(())
}

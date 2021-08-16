use std::collections::BTreeSet;
use std::path::Path;

use anyhow::*;
use log::*;
use rayon::prelude::*;
use tokio::sync::mpsc::{channel, unbounded_channel};
use tokio::task::spawn;

use crate::backend;
use crate::hashing::ObjectId;
use crate::index;
use crate::pack;
use crate::upload;

pub async fn run(repository: &Path) -> Result<()> {
    let cached_backend = backend::open(repository)?;

    let superseded = cached_backend
        .list_indexes()
        .await?
        .iter()
        .map(backend::id_from_path)
        .collect::<Result<BTreeSet<ObjectId>>>()?;

    let (pack_tx, pack_rx) = unbounded_channel();
    let (upload_tx, upload_rx) = channel(1);

    info!("Reading all packs to build a new index");

    cached_backend
        .list_packs()
        .await?
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

    let replacing = index::Index {
        supersedes: superseded.clone(),
        ..Default::default()
    };
    let indexer = spawn(index::index(replacing, pack_rx, upload_tx));

    upload::upload(&cached_backend, upload_rx).await?;

    ensure!(indexer.await.unwrap()?, "No new index built");

    info!("Uploaded a new index; removing previous ones");
    for old_index in superseded {
        debug!("Removing {}", old_index);
        cached_backend.remove_index(&old_index).await?;
    }

    Ok(())
}

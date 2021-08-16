//! [uploads](upload) files ([packs](crate::pack), [indexes](crate::index),
//! [snapshots](crate::snapshot)) to a [backend]

use std::fs::File;

use crate::backend;
use anyhow::Result;
use tokio::sync::mpsc::Receiver;

pub async fn upload(
    cached_backend: &backend::CachedBackend,
    mut rx: Receiver<(String, File)>,
) -> Result<()> {
    while let Some((path, fh)) = rx.recv().await {
        cached_backend.write(&path, fh).await?;
    }
    Ok(())
}

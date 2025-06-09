//! [uploads](upload) files ([packs](crate::pack), [indexes](crate::index),
//! [snapshots](crate::snapshot)) to a [backend]

use std::fs::File;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc::Receiver;

use crate::backend;

pub enum Mode {
    DryRun,
    LiveFire,
}

pub async fn upload(
    mode: Mode,
    cached_backend: Arc<backend::CachedBackend>,
    mut rx: Receiver<(String, File)>,
) -> Result<()> {
    while let Some((path, fh)) = rx.recv().await {
        match mode {
            Mode::LiveFire => cached_backend.write(&path, fh)?,
            Mode::DryRun => {
                // Just axe it, it isn't going anywhere.
                drop(fh);
                std::fs::remove_file(path)?;
            }
        };
    }
    Ok(())
}

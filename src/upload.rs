//! [uploads](upload) files ([packs](crate::pack), [indexes](crate::index),
//! [snapshots](crate::snapshot)) to a [backend]

use std::fs::File;
use std::sync::mpsc::Receiver;

use anyhow::Result;

use crate::backend;

pub enum Mode {
    DryRun,
    LiveFire,
}

pub fn upload(
    mode: Mode,
    cached_backend: &backend::CachedBackend,
    rx: Receiver<(String, File)>,
) -> Result<()> {
    while let Ok((path, fh)) = rx.recv() {
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

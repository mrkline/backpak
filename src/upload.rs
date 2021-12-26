//! [uploads](upload) files ([packs](crate::pack), [indexes](crate::index),
//! [snapshots](crate::snapshot)) to a [backend]

use std::fs::File;
use std::sync::mpsc::Receiver;

use crate::backend;
use anyhow::Result;

pub fn upload(cached_backend: &backend::CachedBackend, rx: Receiver<(String, File)>) -> Result<()> {
    while let Ok((path, fh)) = rx.recv() {
        cached_backend.write(&path, fh)?;
    }
    Ok(())
}

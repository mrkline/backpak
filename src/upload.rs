//! [uploads](upload) files ([packs](crate::pack), [indexes](crate::index),
//! [snapshots](crate::snapshot)) to a [backend]

use std::fs::File;
use std::sync::{atomic::AtomicU64, mpsc::Receiver};

use anyhow::Result;

use crate::backend;

pub fn upload(
    cached_backend: &backend::CachedBackend,
    rx: Receiver<(String, File)>,
    upload_byte_count: &AtomicU64,
) -> Result<()> {
    while let Ok((path, fh)) = rx.recv() {
        cached_backend.write(&path, fh, upload_byte_count)?;
    }
    Ok(())
}

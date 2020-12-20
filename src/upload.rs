use std::fs::File;
use std::sync::mpsc::Receiver;

use crate::backend;
use anyhow::*;

pub fn upload(
    cached_backend: &mut backend::CachedBackend,
    rx: Receiver<(String, File)>,
) -> Result<()> {
    while let Ok((path, fh)) = rx.recv() {
        cached_backend.write(&path, fh)?;
    }
    Ok(())
}

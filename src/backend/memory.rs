use super::*;

use std::io::{self, Cursor};
use std::sync::Mutex;

use anyhow::Result;
use rustc_hash::FxHashMap;

/// A backend that stores everything as path-addressed buffers.
///
/// Great for testing
pub struct MemoryBackend {
    files: Mutex<FxHashMap<String, Vec<u8>>>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self {
            files: Mutex::new(FxHashMap::default()),
        }
    }

    // Cursor is also seek - expose that to `CachedBackend`
    pub fn read_cursor(&self, from: &str) -> Result<Cursor<Vec<u8>>> {
        let buf: Vec<u8> = self
            .files
            .lock()
            .unwrap()
            .get(from)
            .ok_or_else(|| anyhow!("No file {}", from))?
            .clone();
        Ok(Cursor::new(buf))
    }
}

impl Backend for MemoryBackend {
    fn read(&self, from: &str) -> Result<Box<dyn Read + Send + 'static>> {
        Ok(Box::new(self.read_cursor(from)?))
    }

    fn write(&self, _len: u64, from: &mut (dyn Read + Send), to: &str) -> Result<()> {
        let mut vec = Vec::new();
        io::copy(from, &mut vec)?;
        self.files.lock().unwrap().insert(to.to_owned(), vec);
        Ok(())
    }

    fn remove(&self, which: &str) -> Result<()> {
        self.files.lock().unwrap().remove(which);
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let paths: Vec<String> = self
            .files
            .lock()
            .unwrap()
            .keys()
            .filter(|f| f.starts_with(prefix))
            .cloned()
            .collect();
        Ok(paths)
    }
}

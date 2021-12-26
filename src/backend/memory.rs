use super::*;

use std::io;
use std::sync::Mutex;

use rustc_hash::FxHashMap;

/// A backend that stores everything as path-addressed buffers.
///
/// Great for testing
pub struct MemoryBackend {
    files: Mutex<FxHashMap<String, Vec<u8>>>,
}

impl Backend for MemoryBackend {
    fn read<'a>(&'a self, from: &str) -> Result<Box<dyn Read + Send + 'a>> {
        let buf: Vec<u8> = self
            .files
            .lock()
            .unwrap()
            .get(from)
            .ok_or_else(|| anyhow!("No file {}", from))?
            .clone();
        Ok(Box::new(io::Cursor::new(buf)))
    }

    fn write(&self, from: &mut dyn Read, to: &str) -> Result<()> {
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

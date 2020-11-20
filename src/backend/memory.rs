use super::*;

use std::collections::HashMap;
use std::io;

/// A backend that stores everything as path-addressed buffers.
///
/// Great for testing
pub struct MemoryBackend {
    files: HashMap<String, Vec<u8>>,
}

impl Backend for MemoryBackend {
    fn read<'a>(&'a self, from: &str) -> Result<Box<dyn Read + Send + 'a>> {
        let buf = self
            .files
            .get(from)
            .ok_or_else(|| anyhow!("No file {}", from))?;
        Ok(Box::new(io::Cursor::new(buf.as_slice())))
    }

    fn write(&mut self, from: &mut dyn Read, to: &str) -> Result<()> {
        let mut vec = Vec::new();
        io::copy(from, &mut vec)?;
        self.files.insert(to.to_owned(), vec);
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let paths: Vec<String> = self
            .files
            .keys()
            .filter(|f| f.starts_with(prefix))
            .cloned()
            .collect();
        Ok(paths)
    }
}

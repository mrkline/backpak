use super::*;

use tokio::sync::Mutex;

use async_trait::async_trait;
use rustc_hash::FxHashMap;

/// A backend that stores everything as path-addressed buffers.
///
/// Great for testing
pub struct MemoryBackend {
    files: Mutex<FxHashMap<String, Vec<u8>>>,
}

#[async_trait]
impl Backend for MemoryBackend {
    async fn read<'a>(&'a self, from: &str) -> Result<Box<dyn Read + Send + 'a>> {
        let buf: Vec<u8> = self
            .files
            .lock()
            .await
            .get(from)
            .ok_or_else(|| anyhow!("No file {}", from))?
            .clone();
        Ok(Box::new(std::io::Cursor::new(buf)))
    }

    async fn write(&self, from: &mut (dyn AsyncRead + Unpin + Send), to: &str) -> Result<()> {
        let mut vec = Vec::new();
        tokio::io::copy(from, &mut vec).await?;
        self.files.lock().await.insert(to.to_owned(), vec);
        Ok(())
    }

    async fn remove(&self, which: &str) -> Result<()> {
        self.files.lock().await.remove(which);
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let paths: Vec<String> = self
            .files
            .lock()
            .await
            .keys()
            .filter(|f| f.starts_with(prefix))
            .cloned()
            .collect();
        Ok(paths)
    }
}

use super::*;

/// A backend that stores everything as path-addressed buffers.
///
/// Great for testing
pub struct BackendFilter {
    pub filter: String,
    pub unfilter: String,
    pub raw: Box<dyn super::Backend + Send + Sync>
}

impl Backend for BackendFilter {
    fn read<'a>(&'a self, from: &str) -> Result<Box<dyn Read + Send + 'a>> {
        todo!("Run unfilter in a shell and pipe the raw backend through it.");
    }

    fn write(&self, from: &mut dyn Read, to: &str) -> Result<()> {
        todo!("Run filter in a shell and pipe it to the raw backend.");
    }

    fn remove(&self, which: &str) -> Result<()> {
        self.raw.remove(which)
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.raw.list(prefix)
    }
}

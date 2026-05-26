use super::*;
use crate::semaphored;

use std::sync::atomic::AtomicU32;

pub struct Semaphored<B> {
    inner: B,
    count: AtomicU32,
}

impl<B: Backend> Semaphored<B> {
    pub fn new(inner: B, concurrency: u32) -> Self {
        Self {
            inner,
            count: AtomicU32::new(concurrency),
        }
    }
}

impl<B: Backend> Backend for Semaphored<B> {
    fn read(&self, from: &str) -> Result<Box<dyn Read + Send + 'static>> {
        let _sem = semaphored::dec(&self.count);
        self.inner.read(from)
    }

    fn write(&self, len: u64, from: &mut (dyn Read + Send), to: &str) -> Result<()> {
        let _sem = semaphored::dec(&self.count);
        self.inner.write(len, from, to)
    }

    fn remove(&self, which: &str) -> Result<()> {
        let _sem = semaphored::dec(&self.count);
        self.inner.remove(which)
    }

    fn list(&self, prefix: &str) -> Result<Vec<(String, u64)>> {
        let _sem = semaphored::dec(&self.count);
        self.inner.list(prefix)
    }
}

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

struct SemaphoredRead<'a> {
    // Fields drop in order, so the connection closes before we post the semaphore.
    inner: Box<dyn Read + Send + 'a>,
    _sem: semaphored::SemaphoreGuard<'a>,
}

impl Read for SemaphoredRead<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

impl<B: Backend> Backend for Semaphored<B> {
    fn read(&self, from: &str) -> Result<Box<dyn Read + Send + '_>> {
        let _sem = semaphored::dec(&self.count);
        let inner = self.inner.read(from)?;
        Ok(Box::new(SemaphoredRead { inner, _sem }))
    }

    fn write(&self, len: u64, from: &mut dyn SeekableRead, to: &str) -> Result<()> {
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

#[cfg(test)]
mod test {
    use super::*;

    use std::io::Cursor;
    use std::sync::atomic::Ordering;

    #[test]
    fn read_holds_permit() -> Result<()> {
        let s = Semaphored::new(memory::MemoryBackend::new(), 1);
        s.write(3, &mut Cursor::new([1u8, 2, 3]), "foo")?;
        assert_eq!(s.count.load(Ordering::Relaxed), 1);

        let r = s.read("foo")?;
        assert_eq!(s.count.load(Ordering::Relaxed), 0);
        drop(r);
        assert_eq!(s.count.load(Ordering::Relaxed), 1);
        Ok(())
    }
}

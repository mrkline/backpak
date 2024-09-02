//! Go full Java-brain and synchronize methods of a [`Backend`].
//!
//! At least it's not a mutex, but it is a counting semaphore.
//! For some backends (especially going to the Internet)
//! we want to limit the number of concurrent actions (i.e., connections)!

use super::*;

use std::sync::atomic::{AtomicU32, Ordering};

use atomic_wait::*; // <3 Mara

/// lmao how does the stdlib not have counting semaphores
struct SemaphoreGuard<'a> {
    count: &'a AtomicU32,
}

fn dec(count: &AtomicU32) -> SemaphoreGuard {
    // Sanity check:
    // https://www.remlab.net/op/futex-misc.shtml

    // We could slightly optimize this by starting with a relaxed load
    // (that'd keep the CAS from missing the first time if prev is wrong),
    // but it would be completely absurd to worry about a single CAS when we're
    // about to travel the Internet. This is definitely not the critical 3 percent.
    let mut prev = 1;
    loop {
        match count.compare_exchange_weak(prev, prev - 1, Ordering::Acquire, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => {
                if actual == 0 {
                    // I sleep
                    wait(count, 0);
                    // We're back, so at least one thread posted the semaphore
                    // (by dropping the guard) and we've got a positive count again.
                    // Assume only one thread did - if several beat us here, no worries,
                    // the CAS will fail and we'll go again.
                    prev = 1;
                } else {
                    // Boring case: the current count is positive (so we don't have to nap).
                    // Update prev and try the CAS again.
                    prev = actual;
                }
            }
        }
    }

    SemaphoreGuard { count }
}

impl Drop for SemaphoreGuard<'_> {
    fn drop(&mut self) {
        if self.count.fetch_add(1, Ordering::Release) == 0 {
            // real shit
            wake_one(self.count);
        }
    }
}

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
        let _sem = dec(&self.count);
        self.inner.read(from)
    }

    fn write(&self, len: u64, from: &mut (dyn Read + Send), to: &str) -> Result<()> {
        let _sem = dec(&self.count);
        self.inner.write(len, from, to)
    }

    fn remove(&self, which: &str) -> Result<()> {
        let _sem = dec(&self.count);
        self.inner.remove(which)
    }

    fn list(&self, prefix: &str) -> Result<Vec<(String, u64)>> {
        let _sem = dec(&self.count);
        self.inner.list(prefix)
    }
}

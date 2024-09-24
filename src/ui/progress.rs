use std::{
    io::{self, Read, Write},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread::{self, park_timeout, JoinHandle},
    time::{Duration, Instant},
};

use anyhow::Result;

// Used for printing progress as we go
pub struct AtomicCountRead<'a, R> {
    inner: R,
    count: &'a AtomicU64,
}

impl<'a, R: Read> AtomicCountRead<'a, R> {
    pub fn new(inner: R, count: &'a AtomicU64) -> Self {
        Self { inner, count }
    }

    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: Read> Read for AtomicCountRead<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let num_read = self.inner.read(buf)?;
        self.count.fetch_add(num_read as u64, Ordering::Relaxed);
        Ok(num_read)
    }
}

pub struct AtomicCountWrite<'a, W> {
    inner: W,
    count: &'a AtomicU64,
}

impl<'a, W: Write> AtomicCountWrite<'a, W> {
    pub fn new(inner: W, count: &'a AtomicU64) -> Self {
        Self { inner, count }
    }

    pub fn get_ref(&self) -> &W {
        &self.inner
    }

    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for AtomicCountWrite<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let num_written = self.inner.write(buf)?;
        self.count.fetch_add(num_written as u64, Ordering::Relaxed);
        Ok(num_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

pub struct ProgressThread {
    handle: JoinHandle<Result<()>>,
    done_flag: Arc<AtomicBool>,
}

impl ProgressThread {
    pub fn spawn<F: FnMut(usize) -> Result<()> + Send + 'static>(
        rate: Duration,
        f: F,
    ) -> ProgressThread {
        let done_flag = Arc::new(AtomicBool::new(false));
        let df = done_flag.clone();
        let handle = thread::Builder::new()
            .name(String::from("progress-cli"))
            .spawn(move || periodically(rate, &df, f))
            .unwrap();
        Self { handle, done_flag }
    }

    pub fn join(self) -> Result<()> {
        self.done_flag.store(true, Ordering::SeqCst);
        self.handle.join().unwrap()
    }
}

/// Do a thing (presumably draw some progress UI) at the given rate until the exit flag is set.
pub fn periodically<F: FnMut(usize) -> Result<()>>(
    rate: Duration,
    exit: &AtomicBool,
    mut f: F,
) -> Result<()> {
    let mut last = false;
    let mut i = 0;

    loop {
        let start = Instant::now();
        let next = start + rate;

        f(i)?;
        if last {
            return Ok(());
        }

        // Could we simplify this with a futex_wait on exit?
        // Fork Mara's atomic-wait to fix the cpp-brain on Mac?
        loop {
            if exit.load(Ordering::Acquire) {
                // Make sure we print one last round with the final stats.
                last = true;
                break;
            }
            let now = Instant::now();
            if now >= next {
                break;
            } else {
                // Meanwhile, callers can unpark this guy.
                park_timeout(next - now);
            }
        }
        i += 1;
    }
}

pub fn spinner(i: usize) -> char {
    match i % 4 {
        0 => '|',
        1 => '/',
        2 => '-',
        3 => '\\',
        _ => unsafe { std::hint::unreachable_unchecked() },
    }
}

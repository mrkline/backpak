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
use camino::Utf8Path;
use console::Term;
use unicode_segmentation::UnicodeSegmentation;

use crate::backup;
use crate::file_util::nice_size;

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
    pub fn spawn<F: FnMut(usize) -> Result<()> + Send + 'static>(f: F) -> ProgressThread {
        let rate = Duration::from_millis(100);
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

/// Two of em:
///
/// Spinner | Packed | Reused | Compressed | Uploaded,
/// Index
pub fn print_backup_lines(i: usize, bstats: &backup::BackupStats, reused_bytes: u64) {
    let spin = spinner(i);
    let cb = nice_size(bstats.chunk_bytes.load(Ordering::Relaxed));
    let tb = nice_size(bstats.tree_bytes.load(Ordering::Relaxed));
    let rb = nice_size(reused_bytes);
    let cz = nice_size(bstats.compressed_bytes.load(Ordering::Relaxed));
    let ub = nice_size(bstats.uploaded_bytes.load(Ordering::Relaxed));
    println!("{spin} P {cb} + {tb} | R {rb} | Z {cz} | U {ub}");

    let idxd = bstats.indexed_packs.load(Ordering::Relaxed);
    let ispin = if idxd % 2 != 0 { 'i' } else { 'I' };
    println!("{ispin} {idxd} packs indexed");
}

pub fn truncate_path(p: &Utf8Path, term: &Term) -> impl std::fmt::Display {
    // Arbitrary truncation; do something smarter?
    let w = term.size_checked().unwrap_or((0, 80)).1 as usize; // (h, w) wut
    let syms: Vec<_> = p.as_str().graphemes(true).collect();
    if syms.len() > w {
        let back = p.file_name().unwrap();
        if back.len() >= (w - 3) {
            format!("...{back}")
        } else {
            let backsyms = back.graphemes(true).count();
            let front = &syms[0..(w - backsyms - 3)];
            format!("{}...{}", front.join(""), back)
        }
    } else {
        format!("{}", p.as_str())
    }
}

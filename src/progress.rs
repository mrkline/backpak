use std::{
    io::{self, Read, Write},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use camino::Utf8Path;
use console::Term;
use tokio::{sync::Notify, task};
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

pub struct ProgressTask {
    handle: task::JoinHandle<()>,
    done: Arc<Notify>,
}

impl ProgressTask {
    pub fn spawn<F>(f: F) -> Self
    where
        F: FnMut(usize) -> Result<()> + Send + 'static,
    {
        let rate = Duration::from_millis(100);
        let done = Arc::new(Notify::new());
        let df = done.clone();
        let handle = tokio::spawn(periodically(rate, df, f)).unwrap();
        Self { handle, done }
    }

    pub async fn join(self) {
        self.done.notify_one();
        self.handle.await.expect("Couldn't join progress task")
    }
}

/// Do a thing (presumably draw some progress UI) at the given rate until the exit flag is set.
pub async fn peiodically<F: FnMut(usize) -> Result<()>>(
    rate: Duration,
    exit: Notify,
    mut f: F,
) -> Result<()> {
    let mut last = false;
    let mut i = 0;
    let exit = exit.notified();
    tokio::pin!(exit);

    loop {
        let start = Instant::now();

        f(i)?;
        if last {
            return Ok(());
        }
        let next = start + rate;
        let now = Instant::now();

        let sleep = tokio::time::sleep(next - now);
        tokio::select! {
            _ = sleep => {},
            _ = &mut exit => {
                // Make sure we print one last round with the final stats.
                last = true;
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
pub fn print_backup_lines(
    i: usize,
    bstats: &backup::BackupStatistics,
    reused_bytes: u64,
    uploaded_bytes: u64,
) {
    let spin = spinner(i);
    let cb = nice_size(bstats.chunk_bytes.load(Ordering::Relaxed));
    let tb = nice_size(bstats.tree_bytes.load(Ordering::Relaxed));
    let rb = nice_size(reused_bytes);
    let cz = nice_size(bstats.compressed_bytes.load(Ordering::Relaxed));
    let ub = nice_size(uploaded_bytes);
    println!("{spin} P {cb} + {tb} | R {rb} | Z {cz} | U {ub}");

    let idxd = bstats.indexed_packs.load(Ordering::Relaxed);
    let ispin = if idxd % 2 != 0 { 'i' } else { 'I' };
    println!("{ispin} {idxd} packs indexed");
}

pub fn print_download_line(downloaded_bytes: u64) {
    let db = nice_size(downloaded_bytes);
    // Flip every 500K.
    // Better symbols? Trying to commit to ASCII art only.
    let dspin = if downloaded_bytes % 1000000 > 500000 {
        'L'
    } else {
        'D'
    };
    println!("{dspin} {db} downloaded");
}

pub fn truncate_path(p: &Utf8Path, term: &Term) -> String {
    // Arbitrary truncation; do something smarter?
    let w = term.size().1 as usize; // (h, w) wut
    // How absurd. But do it so we don't panic on underflow
    if w <= 3 {
        return ".".repeat(w);
    }
    let syms: Vec<_> = p.as_str().graphemes(true).collect();
    if syms.len() > w {
        // If the filename itself is more than the line length (minus "..."),
        // trim just the filename and print it.
        let back: Vec<_> = p.file_name().unwrap().graphemes(true).collect();
        if back.len() >= (w - 3) {
            format!("...{}", back[back.len() - w + 3..].concat())
        }
        // Otherwise try to get both the front and back of the file path.
        else {
            let front = &syms[..(w - back.len() - 3)];
            format!("{}...{}", front.concat(), back.concat())
        }
    } else {
        p.to_string()
    }
}

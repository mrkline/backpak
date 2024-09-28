use anyhow::Result;
use console::Term;

use crate::{
    backend::CachedBackend,
    backup,
    progress::{self, print_backup_lines, print_download_line, truncate_path},
};

use super::*;

pub struct ProgressThread {
    inner: progress::ProgressThread,
}

impl ProgressThread {
    pub fn spawn(
        src: Arc<CachedBackend>,
        dest: Arc<CachedBackend>,
        bs: Arc<backup::BackupStats>,
        ws: Arc<WalkStatistics>,
    ) -> Self {
        let t = Term::stdout();
        let inner = progress::ProgressThread::spawn(move |i| {
            print_progress(i, &t, &bs, &ws, &src.bytes_downloaded, &dest.bytes_uploaded)
        });
        Self { inner }
    }

    pub fn join(self) -> Result<()> {
        self.inner.join()
    }
}

fn print_progress(
    i: usize,
    term: &Term,
    bstats: &backup::BackupStats,
    wstats: &WalkStatistics,
    down: &AtomicU64,
    up: &AtomicU64,
) -> Result<()> {
    if i > 0 {
        term.clear_last_lines(5)?;
    }

    let rb = wstats.reused_bytes.load(Ordering::Relaxed);
    let ub = up.load(Ordering::Relaxed);
    print_backup_lines(i, bstats, rb, ub);

    print_download_line(down.load(Ordering::Relaxed));

    let cs = wstats.current_snapshot.lock().unwrap().clone();
    println!("Snapshot: {cs}");

    let cf: Utf8PathBuf = wstats.current_file.lock().unwrap().clone();
    let cf = truncate_path(&cf, term);
    println!("{cf}");
    Ok(())
}

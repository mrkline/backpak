use anyhow::Result;
use console::Term;

use crate::{
    backup,
    progress::{self, print_backup_lines, truncate_path},
};

use super::*;

pub struct ProgressThread {
    inner: progress::ProgressThread,
}

impl ProgressThread {
    pub fn spawn(bs: Arc<backup::BackupStats>, ws: Arc<WalkStatistics>) -> Self {
        let t = Term::stdout();
        let inner = progress::ProgressThread::spawn(move |i| print_progress(i, &t, &bs, &ws));
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
) -> Result<()> {
    if i > 0 {
        term.clear_last_lines(4)?;
    }

    let rb = wstats.reused_bytes.load(Ordering::Relaxed);
    print_backup_lines(i, bstats, rb);

    let cs = wstats.current_snapshot.lock().unwrap().clone();
    println!("snap: {cs}");

    let cf: Utf8PathBuf = wstats.current_file.lock().unwrap().clone();
    let cf = truncate_path(&cf, term);
    println!("{cf}");
    Ok(())
}

use std::sync::Arc;

use anyhow::Result;
use console::Term;

use crate::{
    backup,
    progress::{self, print_backup_lines, print_download_line, truncate_path},
};

use super::*;

pub struct ProgressThread {
    inner: progress::ProgressTask,
}

impl ProgressThread {
    pub fn spawn(
        bs: Arc<backup::BackupStatistics>,
        ws: Arc<WalkStatistics>,
        down: Arc<AtomicU64>,
        up: Arc<AtomicU64>,
    ) -> Self {
        let inner =
            progress::ProgressTask::spawn(|i| print_progress(i, &Term::stdout(), bs, ws, down, up));
        Self { inner }
    }

    pub async fn join(self) {
        self.inner.join().await
    }
}

fn print_progress(
    i: usize,
    term: &Term,
    bstats: Arc<backup::BackupStatistics>,
    wstats: Arc<WalkStatistics>,
    down: Arc<AtomicU64>,
    up: Arc<AtomicU64>,
) -> Result<()> {
    if i > 0 {
        term.clear_last_lines(5)?;
    }

    let rb = wstats.reused_bytes.load(Ordering::Relaxed);
    let ub = up.load(Ordering::Relaxed);
    print_backup_lines(i, bstats, rb, ub);

    print_download_line(down.load(Ordering::Relaxed));

    let cs = wstats.current_snapshot.borrow();
    println!("Snapshot: {cs}");

    let cf = wstats.current_file.borrow();
    let cf = truncate_path(&cf, term);
    println!("{cf}");
    Ok(())
}

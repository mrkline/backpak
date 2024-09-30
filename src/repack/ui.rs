use std::thread::Scope;

use anyhow::Result;
use console::Term;

use crate::{
    backup,
    progress::{self, print_backup_lines, print_download_line, truncate_path},
};

use super::*;

pub struct ProgressThread<'scope> {
    inner: progress::ProgressThread<'scope>,
}

impl<'scope> ProgressThread<'scope> {
    pub fn spawn<'env>(
        s: &'scope Scope<'scope, 'env>,
        bs: &'env backup::BackupStatistics,
        ws: &'env WalkStatistics,
        down: &'env AtomicU64,
        up: &'env AtomicU64,
    ) -> Self {
        let inner = progress::ProgressThread::spawn(s, |i| {
            print_progress(i, &Term::stdout(), bs, ws, down, up)
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
    bstats: &backup::BackupStatistics,
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

    let cs = wstats.current_snapshot.borrow();
    println!("Snapshot: {cs}");

    let cf = wstats.current_file.borrow();
    let cf = truncate_path(&cf, term);
    println!("{cf}");
    Ok(())
}

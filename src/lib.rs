//! Some big dumb backup system.
//!
//! See the [`backup`] module for an overview and a crappy block diagram.
use anyhow::Error;
use rustc_hash::FxHashSet;
use std::sync::{LazyLock, Mutex};
use tracing::error;

pub mod backend;
pub mod backup;
pub mod blob;
pub mod chunk;
pub mod concurrently;
pub mod config;
pub mod counters;
pub mod diff;
pub mod file_util;
pub mod filter;
pub mod fs_tree;
pub mod hashing;
pub mod index;
pub mod ls;
pub mod pack;
pub mod prettify;
pub mod progress;
pub mod rcu;
pub mod read;
pub mod repack;
pub mod snapshot;
pub mod tree;
pub mod upload;

// CLI stuff:
pub mod ui;

// Something awful:
//
// Maintain global state of all child proceses so we can kill them when a fatal error occurs.
//
// Consider running `backpak -r foo check` where `foo` is a repo filtered by GPG.
// If we fail to enter the right password into the agent (or cancel it),
// we'd like to immediately quit, but instead the user gets prompted again, and again,
// times the concurrency limit set by the backend. This is a shitty user experience.
//
// We have no way to abort the other threads from the initial guy who failed -
// they're blocking on read calls to the kids' stdout pipes.
// And even if we `exit(1)` here, the children live on and will keep harassing the user...
// unless we murder them!
//
// This is a terrible hack that makes me wonder if we should go async,
// *just* so we have some sort of uniform task cancellation (i.e., by dropping futures).

static CHILDREN: LazyLock<Mutex<FxHashSet<u32>>> =
    LazyLock::new(|| Mutex::new(FxHashSet::default()));

pub struct ChildGuard {
    id: u32,
}

impl ChildGuard {
    pub fn new(id: u32) -> Self {
        CHILDREN.lock().unwrap().insert(id);
        Self { id }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        CHILDREN.lock().unwrap().remove(&self.id);
    }
}

pub fn fatal(e: Error) -> ! {
    use rustix::process::{self, Pid, Signal};

    error!("{e:?}");
    for i in CHILDREN.lock().unwrap().iter() {
        let p = unsafe { Pid::from_raw_unchecked(*i as i32) };
        let _ = process::kill_process(p, Signal::TERM);
    }
    std::process::exit(1);
}

use std::sync::{Mutex, atomic::AtomicU32};
use std::thread;

use anyhow::Result;

use super::fatal;
use super::semaphored;

fn atomic_cpus() -> AtomicU32 {
    AtomicU32::new(num_cpus::get_physical() as u32)
}

/// Run each of the given functions in its own thread.
/// If one fails, kill the program instead of waiting around.
///
/// Useful for when the functions can be long-running or involve user interaction,
/// so we don't want to wait on failure.
///
/// Should we just throw in the towel and use async so we have cancellation?
pub fn concurrently<F, I>(funs: I)
where
    F: FnOnce() -> Result<()> + Send,
    I: Iterator<Item = F>,
{
    let conc = atomic_cpus();
    thread::scope(|s| {
        for f in funs {
            // Use a semaphore to limit *spawning* threads,
            // but release them when the thread exits.
            let sem = semaphored::dec(&conc);
            s.spawn(|| {
                let _sem = sem;
                if let Err(e) = f() {
                    fatal(e);
                }
            });
        }
    })
}

pub fn named_concurrently<S, F, I>(funs: I)
where
    S: Into<String>,
    F: FnOnce() -> Result<()> + Send,
    I: Iterator<Item = (S, F)>,
{
    let conc = atomic_cpus();
    thread::scope(|s| {
        for (n, f) in funs {
            let sem = semaphored::dec(&conc);
            thread::Builder::new()
                .name(n.into())
                .spawn_scoped(s, || {
                    let _sem = sem;
                    if let Err(e) = f() {
                        fatal(e);
                    }
                })
                .unwrap();
        }
    })
}

pub fn map_concurrently<T, F, I>(funs: I) -> Vec<T>
where
    T: Send,
    F: FnOnce() -> Result<T> + Send,
    I: Iterator<Item = F>,
{
    let conc = atomic_cpus();
    let results = Mutex::new(vec![]);
    thread::scope(|s| {
        for f in funs {
            let sem = semaphored::dec(&conc);
            s.spawn(|| {
                let _sem = sem;
                match f() {
                    Ok(v) => results.lock().unwrap().push(v),
                    Err(e) => fatal(e),
                }
            });
        }
    });
    let mut results = results.into_inner().unwrap();
    results.shrink_to_fit();
    results
}

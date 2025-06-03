use std::sync::Mutex;
use std::thread;

use anyhow::Result;

use super::fatal;

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
    thread::scope(|s| {
        for f in funs {
            s.spawn(|| {
                if let Err(e) = f() {
                    fatal(e);
                }
            });
        }
    })
}

pub fn map_concurrently<T, F, I>(funs: I) -> Vec<T>
where
    T: Send,
    F: FnOnce() -> Result<T> + Send,
    I: Iterator<Item = F>,
{
    let results = Mutex::new(vec![]);
    thread::scope(|s| {
        for f in funs {
            s.spawn(|| match f() {
                Ok(v) => results.lock().unwrap().push(v),
                Err(e) => fatal(e),
            });
        }
    });
    let mut results = results.into_inner().unwrap();
    results.shrink_to_fit();
    results
}

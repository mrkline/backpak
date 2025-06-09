use std::sync::Mutex;

use anyhow::Result;
use tokio::task::JoinSet;

/// Run each of the given functions in its own thread.
/// If one fails, kill the program instead of waiting around.
///
/// Useful for when the functions can be long-running or involve user interaction,
/// so we don't want to wait on failure.
///
/// Should we just throw in the towel and use async so we have cancellation?
pub async fn concurrently<F, I>(funs: I) -> Result<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
    I: Iterator<Item = F>,
{
    let mut ts = JoinSet::new();
    for f in funs {
        ts.spawn(f);
    }

    while let Some(res) = ts.join_next().await {
        res.unwrap()?;
    }
    Ok(())
}

pub async fn map_concurrently<T, F, I>(funs: I) -> Result<Vec<T>>
where
    T: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
    I: Iterator<Item = F>,
{
    let results = Mutex::new(vec![]);

    let mut ts = JoinSet::new();
    for f in funs {
        ts.spawn(f);
    }

    while let Some(res) = ts.join_next().await {
        results.lock().unwrap().push(res.unwrap()?);
    }

    let mut results = results.into_inner().unwrap();
    results.shrink_to_fit();
    Ok(results)
}

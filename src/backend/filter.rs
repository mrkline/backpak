use super::*;

use std::{
    io,
    process::{Child, Command, Stdio},
    thread,
};

use anyhow::{Result, ensure};

/// A backend that filters another backend through a pair of shell commands,
/// `filter` and `unfilter`.
pub struct BackendFilter {
    pub filter: String,
    pub unfilter: String,
    pub raw: Box<dyn super::Backend + Send + Sync>,
}

struct UnfilterRead {
    from: String,
    unfilter: String,
    copy_thread: Option<thread::JoinHandle<Result<()>>>,
    child: Child,
}

// DANGER WILL ROBINSIN:
//
// This might suck more than I initially thought - since the panic doens't happen until
// after the read is dropped, callers might think it succeeded.
// If we're not careful to pass this by value and drop it,
// it breaks everything, especially our `safe_copy_to_file()` hopes.

// It would be nice to have some join(self) to gracefully catch errors,
// but then Backend::read() couldn't return a generic Read trait object,
// we'd need some JoinableRead...
impl Drop for UnfilterRead {
    fn drop(&mut self) {
        // Lacking that, await the unfilter process in the destructor
        // and panic if it failed :/
        self.copy_thread
            .take()
            .unwrap()
            .join()
            .unwrap()
            .unwrap_or_else(|e| {
                panic!(
                    "Piping {} through {} failed: {:#?}",
                    self.from, self.unfilter, e
                )
            });
        if !self.child.wait().unwrap().success() {
            panic!("{} < {} failed", self.unfilter, self.from)
        }
    }
}

impl Read for UnfilterRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.child.stdout.as_mut().unwrap().read(buf)
    }
}

impl Backend for BackendFilter {
    fn read(&self, from: &str) -> Result<Box<dyn Read + Send + 'static>> {
        debug!("{} < {from}", self.unfilter);

        let mut inner_read = self.raw.read(from)?;

        let mut uf = Command::new("sh")
            .arg("-c")
            .arg(&self.unfilter)
            .stdout(Stdio::piped())
            .stdin(Stdio::piped())
            .spawn()
            .with_context(|| format!("Couldn't run {}", self.unfilter))?;

        let mut to_unfilter = uf.stdin.take().unwrap();

        let copy_thread = thread::Builder::new()
            .name("unfilter-copy".to_string())
            .spawn(move || -> anyhow::Result<()> {
                io::copy(&mut inner_read, &mut to_unfilter)?;
                Ok(())
            })
            .unwrap(); // Panic if we can't spawn a thread

        Ok(Box::new(UnfilterRead {
            from: from.to_string(),
            unfilter: self.unfilter.clone(),
            copy_thread: Some(copy_thread),
            child: uf,
        }))
    }

    fn write(&self, _len: u64, from: &mut (dyn Read + Send), to: &str) -> Result<()> {
        debug!("{} > {to}", self.filter);

        let mut f = Command::new("sh")
            .arg("-c")
            .arg(&self.filter)
            .stdout(Stdio::piped())
            .stdin(Stdio::piped())
            .spawn()
            .with_context(|| format!("Couldn't run {}", self.filter))?;

        let mut to_filter = f.stdin.take().unwrap();
        let mut from_filter = f.stdout.take().unwrap();

        // NB: Some backends (particularly cloud storage like B2)
        // need to know the exact size of the file!
        // With an arbitrary filter, we don't know how big that will be until it exits.
        // This sadly means we can't filter and upload in parallel.
        // Until we can think of something smarter, write to a tempfile.
        let mut filtered = tempfile::tempfile_in(".")?;

        thread::scope(|s| -> anyhow::Result<()> {
            // Create a thread to copy to the filter process.
            let copy_to = thread::Builder::new()
                .name("filter-copy".to_string())
                .spawn_scoped(s, move || -> anyhow::Result<()> {
                    io::copy(from, &mut to_filter)?;
                    // It's important to move to_filter in so it gets dropped here.
                    // Otherwise the pipe file descriptor stays open and we hang.
                    Ok(())
                })
                .unwrap(); // Panic if we can't spawn a thread.

            // Meanwhile, in this thread, copy output to our tempfile.
            io::copy(&mut from_filter, &mut filtered)?;

            // Unwrap the result of the join (i.e., that the child didn't panic)
            // and check that copying to the filter didn't fail.
            copy_to.join().unwrap()?;
            Ok(())
        })
        .with_context(|| format!("Piping {to} through {} failed", self.filter))?;

        ensure!(
            f.wait().unwrap().success(),
            format!("{} > {to} failed", self.filter)
        );

        // Meanwhile, in this thread, copy to the underlying backend.
        let len = filtered.stream_position()?;
        filtered.seek(io::SeekFrom::Start(0))?;
        self.raw.write(len, &mut filtered, to)?;

        Ok(())
    }

    fn remove(&self, which: &str) -> Result<()> {
        self.raw.remove(which)
    }

    fn list(&self, prefix: &str) -> Result<Vec<(String, u64)>> {
        self.raw.list(prefix)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::io::Cursor;

    #[test]
    fn smoke() -> Result<()> {
        let f = BackendFilter {
            filter: "cat".to_string(),
            unfilter: "cat".to_string(),
            raw: Box::new(crate::backend::memory::MemoryBackend::new()),
        };

        let epitaph = "Everything was beautiful and nothing hurt";
        f.write(epitaph.len() as u64, &mut Cursor::new(epitaph), "epitaph")?;

        let mut so_it_goes = String::new();
        f.read("epitaph")?.read_to_string(&mut so_it_goes)?;
        assert_eq!(so_it_goes, "Everything was beautiful and nothing hurt");
        Ok(())
    }
}

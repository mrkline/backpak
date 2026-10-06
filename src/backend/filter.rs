use super::*;

use std::{
    io,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    thread,
};

use anyhow::{Result, ensure};

/// Pipes reads and writes of a backend through a pair of shell commands,
/// `filter` and `unfilter`.
pub struct Filters {
    pub filter: String,
    pub unfilter: String,
}

struct UnfilterRead<'a> {
    from: &'a str,
    unfilter: &'a str,
    copy_thread: Option<thread::ScopedJoinHandle<'a, io::Result<()>>>,
    child: Child,
    bytes_unfiltered: &'a AtomicU64,
    _cg: crate::ChildGuard,
}

impl Drop for UnfilterRead<'_> {
    fn drop(&mut self) {
        let _ = self.child.kill();
    }
}

impl Read for UnfilterRead<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Try to read some from the filter program.
        // Stdout shouldn't be None until we wait below.
        let res = self
            .child
            .stdout
            .as_mut()
            .expect("UnfilteredRead::read() called after it returned 0")
            .read(buf)?;
        self.bytes_unfiltered
            .fetch_add(res as u64, Ordering::Relaxed);

        // If the last bytes were read, we have some cleanup to do.
        if res == 0 {
            // See if the process exited successfully;
            // otherwise we'll have an incomplete file.
            let j = self.child.wait()?;
            if !j.success() {
                let j = match j.code() {
                    Some(c) => format!("failed with code {c}"),
                    None => "was killed".to_owned(),
                };
                return Err(io::Error::other(format!(
                    "{} < {} {j}",
                    self.unfilter, self.from
                )));
            }

            // Make sure we actually fed all the bytes into the filter program.
            // Check this before we return EOF so that callers like the cache
            // don't keep a truncated file.
            let copy_thread = self.copy_thread.take().unwrap();
            copy_thread.join().expect("unfilter-copy thread aborted")?;
        }

        Ok(res)
    }
}

impl Filters {
    /// Reads `from` the raw backend through the unfilter program,
    /// and gives the unfiltered bytes to `f`.
    pub fn read<T>(
        &self,
        raw: &dyn Backend,
        from: &str,
        bytes_downloaded: &AtomicU64,
        bytes_unfiltered: &AtomicU64,
        f: impl FnOnce(&mut dyn Read) -> Result<T>,
    ) -> Result<T> {
        debug!("{} < {from}", self.unfilter);

        let mut inner_read = progress::AtomicCountRead::new(raw.read(from)?, bytes_downloaded);

        let mut uf = Command::new("sh")
            .arg("-c")
            .arg(&self.unfilter)
            .stdout(Stdio::piped())
            .stdin(Stdio::piped())
            .spawn()
            .with_context(|| format!("Couldn't run {}", self.unfilter))?;

        let mut to_unfilter = uf.stdin.take().unwrap();

        thread::scope(|s| {
            let copy_thread = thread::Builder::new()
                .name("unfilter-copy".to_string())
                // It's important to move to_unfilter in so it gets dropped here.
                // Otherwise the pipe file descriptor stays open and we hang.
                .spawn_scoped(s, move || {
                    io::copy(&mut inner_read, &mut to_unfilter).map(|_| ())
                })
                .unwrap(); // Panic if we can't spawn a thread

            let ufid = uf.id();
            // If f returns before EOF, dropping this kills the unfilter program.
            // The copy thread then fails on the closed pipe, and the scope can join it.
            let mut unfiltered = UnfilterRead {
                from,
                unfilter: &self.unfilter,
                copy_thread: Some(copy_thread),
                child: uf,
                bytes_unfiltered,
                _cg: crate::ChildGuard::new(ufid),
            };
            f(&mut unfiltered)
        })
    }

    /// Writes `from` through the filter program to `to` in the raw backend.
    pub fn write(
        &self,
        raw: &dyn Backend,
        from: &mut dyn SeekableRead,
        to: &str,
        bytes_filtered: &AtomicU64,
        bytes_uploaded: &AtomicU64,
    ) -> Result<()> {
        debug!("{} > {to}", self.filter);

        let mut f = Command::new("sh")
            .arg("-c")
            .arg(&self.filter)
            .stdout(Stdio::piped())
            .stdin(Stdio::piped())
            .spawn()
            .with_context(|| format!("Couldn't run {}", self.filter))?;
        let _cg = crate::ChildGuard::new(f.id());

        let to_filter = f.stdin.take().unwrap();
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
                    let mut to_filter = progress::AtomicCountWrite::new(to_filter, bytes_filtered);
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
        let mut counter = progress::AtomicCountRead::new(filtered, bytes_uploaded);
        raw.write(len, &mut counter, to)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::io::Cursor;

    #[test]
    fn smoke() -> Result<()> {
        // Add a byte when filtering and remove it when unfiltering
        // so that the raw and the filtered byte counts differ.
        let f = Filters {
            filter: "printf x; cat".to_string(),
            unfilter: "tail -c +2".to_string(),
        };
        let raw = crate::backend::memory::MemoryBackend::new();
        let filtered = AtomicU64::new(0);
        let uploaded = AtomicU64::new(0);
        let downloaded = AtomicU64::new(0);
        let unfiltered = AtomicU64::new(0);

        let epitaph = "Everything was beautiful and nothing hurt";
        f.write(
            &raw,
            &mut Cursor::new(epitaph),
            "epitaph",
            &filtered,
            &uploaded,
        )?;

        let mut so_it_goes = String::new();
        f.read(&raw, "epitaph", &downloaded, &unfiltered, |r| {
            Ok(r.read_to_string(&mut so_it_goes)?)
        })?;
        assert_eq!(so_it_goes, "Everything was beautiful and nothing hurt");

        let len = epitaph.len() as u64;
        assert_eq!(filtered.load(Ordering::Relaxed), len);
        assert_eq!(uploaded.load(Ordering::Relaxed), len + 1);
        assert_eq!(downloaded.load(Ordering::Relaxed), len + 1);
        assert_eq!(unfiltered.load(Ordering::Relaxed), len);
        Ok(())
    }
}

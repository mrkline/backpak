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
    copy_thread: Option<thread::JoinHandle<io::Result<()>>>,
    child: Child,
}

impl Read for UnfilterRead {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Try to read some from the filter program.
        // Stdout shouldn't be None until we wait below.
        let res = self
            .child
            .stdout
            .as_mut()
            .expect("UnfilteredRead::read() called after it returned 0")
            .read(buf)?;

        // If the last bytes were read, we have some cleanup to do.
        if res == 0 {
            // Make sure we actually fed all the bytes into the filter program.
            let copy_thread = self.copy_thread.take().unwrap();
            assert!(copy_thread.is_finished());
            copy_thread.join().expect("unfilter-copy thread aborted")?;

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
        }

        Ok(res)
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
            // It's important to move to_unfilter in so it gets dropped here.
            // Otherwise the pipe file descriptor stays open and we hang.
            .spawn(move || io::copy(&mut inner_read, &mut to_unfilter).map(|_| ()))
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

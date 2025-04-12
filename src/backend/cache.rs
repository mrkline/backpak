use std::{
    fs::{self, File},
    io::prelude::*,
    sync::Mutex,
};

use anyhow::{Context, Result, anyhow, bail};
use byte_unit::Byte;
use camino::{Utf8Path, Utf8PathBuf};
use rusqlite::Connection;

use crate::config;
use crate::counters::{Op, bump};
use crate::file_util;

/// Local cache for any and all backends.
///
/// Originally this was going to just be a directory of files,
/// but we want to enforce some max cache size,
/// and multiple processes all trying to stat the same directory while inserting
/// and deleting files sounds like a pain in the ass, to put it mildly.
/// Retrying whenever the directory changes under our feet sounds similarly unpleasant.
/// What if we had some mechanism that was atomic, consistent, isolated, and durable?
///
/// Sad addendum: Originally I tried putting the file contents in the DB as well,
/// but as it turns out, forcing every byte through a locked database connection
/// is a recipe for slow sadness. Keep the actual file contents in... files.
pub struct Cache {
    pub directory: Utf8PathBuf,
    conn: Mutex<Connection>,
}

// 1G. Make this configurable with global settings (~/.config/backpak?)
pub const DEFAULT_SIZE: Byte = Byte::GIBIBYTE;

impl Cache {
    /// Create a cache given the database connection - let users handle the creation
    /// to make it easy to pass in `Connection::open_in_memory()`, etc.
    pub fn new(dir: &Utf8Path, cache_size: Byte) -> Result<Self> {
        let mut conn = Connection::open(dir.join("cache_metadata.sqlite"))?;

        let t = conn.transaction()?;
        let ver: i32 = t.query_row("PRAGMA user_version", (), |r| r.get(0))?;
        if ver < 1 {
            t.execute(
                "CREATE TABLE cache (
                    name TEXT NOT NULL PRIMARY KEY,
                    time INTEGER NOT NULL,
                    size INTEGER NOT NULL
                ) STRICT, WITHOUT ROWID",
                (),
            )?;
            // Make concurrent processes work off the same settings
            // (for now, just total size).
            t.execute(
                "CREATE TABLE settings (
                    key TEXT NOT NULL PRIMARY KEY,
                    value NOT NULL
                )",
                (),
            )?;
        }
        t.execute("PRAGMA user_version=1", ())?;
        t.commit()?;

        let jm: String = conn.query_row("PRAGMA journal_mode=wal", (), |r| r.get(0))?;
        // The journal mode could be memory if this is an in-memory DB for unit tests.
        assert!(jm == "wal" || jm == "memory", "sqlite: Couldn't set WAL");

        // Last guy wins.
        conn.execute(
            "REPLACE INTO settings(key, value) VALUES ('size', ?1)",
            [cache_size.as_u64()],
        )?;

        Ok(Self {
            directory: dir.to_owned(),
            conn: Mutex::new(conn),
        })
    }

    pub fn try_read(&self, name: &str) -> Result<Option<File>> {
        match File::open(self.directory.join(name)) {
            Ok(fd) => {
                self.bump_row(name, fd.metadata()?.len())?;
                Ok(Some(fd))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // If it's not here, bump off any stale records.
                self.conn
                    .lock()
                    .unwrap()
                    .execute("DELETE FROM cache WHERE name == ?1", [name])?;
                Ok(None)
            }
            Err(e) => bail!(e),
        }
    }

    /// Insert the given contents into the cache with the given name.
    /// Returns the file in the cache
    /// (since reads that just inserted will want to read the contents immediately).
    pub fn insert<R: Read>(&self, name: &str, contents: R) -> Result<File> {
        let to = self.directory.join(name);
        let cached = file_util::safe_copy_to_file(contents, &to)?;
        self.bump_row(name, cached.metadata()?.len())?;
        Ok(cached)
    }

    pub fn insert_file(&self, name: &str, f: File) -> Result<File> {
        let to = self.directory.join(name);
        let cached = file_util::move_opened(name, f, to)?;
        self.bump_row(name, cached.metadata()?.len())?;
        Ok(cached)
    }

    fn bump_row(&self, name: &str, size: u64) -> Result<()> {
        self.conn.lock().unwrap().execute(
            "REPLACE INTO cache(name, time, size) VALUES (?1, ?2, ?3)",
            (name, now_nanos(), size),
        )?;
        Ok(())
    }

    pub fn evict(&self, name: &str) -> Result<()> {
        self.delete_if_exists(name)?;
        let rows = self
            .conn
            .lock()
            .unwrap()
            .execute("DELETE FROM cache WHERE name == ?1", [name])?;
        assert!(rows <= 1, "Duplicate cache entries evicted");
        Ok(())
    }

    fn delete_if_exists(&self, name: &str) -> Result<()> {
        match fs::remove_file(self.directory.join(name)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => bail!(e),
        }
    }

    pub fn prune(&self) -> Result<()> {
        // We want this all to be atomic.
        let mut c = self.conn.lock().unwrap();
        let transaction = c.transaction()?;

        // Get the max cache size.
        let max_size: i64 =
            transaction.query_row("SELECT value FROM settings WHERE key = 'size'", (), |r| {
                r.get(0)
            })?;

        // Find least-recently used entries that exceed our cache size.
        let mut statement =
            transaction.prepare("SELECT name, time, size FROM cache ORDER BY time DESC")?;
        let mut times_and_sizes = statement.query(())?;

        let mut acc = 0i64;
        let mut oldest_that_fits = None;
        while acc < max_size {
            match times_and_sizes.next()? {
                Some(row) => {
                    let t: i64 = row.get(1)?;
                    let s: i64 = row.get(2)?;
                    oldest_that_fits = Some(t);
                    acc += s;
                }
                None => {
                    // The whole cache fits. We're done.
                    return Ok(());
                }
            }
        }
        // Delete files older than this
        while let Some(row) = times_and_sizes.next()? {
            let name: String = row.get(0)?;
            bump(Op::BackendCacheSpill);
            self.delete_if_exists(&name)?;
        }
        drop(times_and_sizes);
        drop(statement);

        // Delete those least-recently used entries that are too big.
        if let Some(o) = oldest_that_fits {
            transaction.execute("DELETE FROM cache WHERE time < ?1", [o])?;
            transaction.commit()?;
        } else {
            bail!("Absurd: zero-size cache");
        }

        Ok(())
    }
}

fn now_nanos() -> i64 {
    jiff::Timestamp::now().as_nanosecond() as i64
}

pub fn setup(conf: &config::Configuration) -> Result<Cache> {
    let mut cachedir: Utf8PathBuf = home::home_dir()
        .ok_or_else(|| anyhow!("Can't find home directory"))?
        .try_into()
        .context("Home directory isn't UTF-8")?;
    cachedir.extend([".cache", "backpak"]);
    fs::create_dir_all(&cachedir).with_context(|| format!("Couldn't create {cachedir}"))?;
    Cache::new(&cachedir, conf.cache_size)
}

#[cfg(test)]
mod test {
    use super::*;

    use tempfile::tempdir;

    #[test]
    fn smoke() -> Result<()> {
        let td = tempdir()?;
        let mut cache = Cache::new(Utf8Path::from_path(td.path()).unwrap(), DEFAULT_SIZE)?;

        // We can put something in and read it out.
        cache.insert("foo", &mut [1, 2, 3, 4].as_slice())?;
        let mut back = vec![];
        cache.try_read("foo")?.unwrap().read_to_end(&mut back)?;
        assert_eq!(&[1, 2, 3, 4], back.as_slice());

        // Things that don't exist aren't there.
        assert!(cache.try_read("bar")?.is_none());

        cache.insert("baz", &mut [1, 2, 3].as_slice())?;

        // With the default yuge size, we can fit seven bytes.
        cache.prune()?;
        let names_left = |c: &mut Cache| {
            c.conn
                .lock()
                .unwrap()
                .prepare("SELECT name FROM cache ORDER BY time DESC")
                .unwrap()
                .query_map((), |row| row.get(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<String>>>()
                .unwrap()
        };
        assert_eq!(names_left(&mut cache), ["baz", "foo"]);

        // But let's change that.
        let new_size = |c: &mut Cache, s| {
            c.conn
                .lock()
                .unwrap()
                .execute("REPLACE INTO settings(key, value) VALUES ('size', ?1)", [s])
                .unwrap()
        };
        new_size(&mut cache, 3);
        cache.prune()?;
        assert_eq!(names_left(&mut cache), ["baz"]);

        // Absurd: less than one entry (keep at least one entry).
        new_size(&mut cache, 1);
        cache.prune()?;
        assert_eq!(names_left(&mut cache), ["baz"]);

        // Evict that guy.
        cache.evict("baz")?;
        assert!(names_left(&mut cache).is_empty());

        // Absurd: zero-size cache should complain.
        new_size(&mut cache, 0);
        assert!(cache.prune().is_err());

        Ok(())
    }
}

use anyhow::{bail, Result};
use rusqlite::Connection;

/// Local cache for any and all backends.
///
/// Originally this was going to just be a directory of files,
/// but we want to enforce some max cache size,
/// and multiple processes all trying to stat the same directory while inserting
/// and deleting files sounds like a pain in the ass, to put it mildly.
/// Retrying whenever the directory changes under our feet sounds similarly unpleasant.
/// What if we had some mechanism that was atomic, consistent, isolated, and durable?
pub struct Cache {
    conn: Connection,
}

// 1G. Make this configurable with global settings (~/.config/backpak?)
const CACHE_SIZE: i64 = 1024 * 1024 * 1024;

impl Cache {
    /// Create a cache given the database connection - let users handle the creation
    /// to make it easy to pass in `Connection::open_in_memory()`, etc.
    pub fn new(mut conn: Connection) -> Result<Self> {
        let t = conn.transaction()?;
        let ver: i32 = t.query_row("PRAGMA user_version", (), |r| r.get(0))?;
        if ver < 1 {
            t.execute(
                "CREATE TABLE cache (
                    name TEXT NOT NULL PRIMARY KEY,
                    time INTEGER NOT NULL,
                    data BLOB NOT NULL
                ) STRICT",
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
            [CACHE_SIZE],
        )?;

        Ok(Self { conn })
    }

    pub fn try_read(&self, name: &str) -> Result<Option<Vec<u8>>> {
        let blobs = self
            .conn
            .prepare("SELECT data FROM cache where name = ?1")?
            .query_map([name], |r| r.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert!(blobs.len() <= 1);
        Ok(blobs.into_iter().next())
    }

    pub fn insert(&self, name: &str, contents: &[u8]) -> Result<()> {
        self.conn.execute(
            "REPLACE INTO cache(name, time, data) VALUES (?1, ?2, ?3)",
            (name, now_nanos(), contents),
        )?;
        Ok(())
    }

    pub fn evict(&self, name: &str) -> Result<()> {
        let rows = self
            .conn
            .execute("DELETE FROM cache WHERE name == ?1", [name])?;
        assert!(rows <= 1, "Duplicate cache entries evicted");
        Ok(())
    }

    pub fn prune(&mut self) -> Result<()> {
        // We want this all to be atomic.
        let transaction = self.conn.transaction()?;

        // Get the max cache size.
        let max_size: i64 =
            transaction.query_row("SELECT value FROM settings WHERE key = 'size'", (), |r| {
                r.get(0)
            })?;

        // Find least-recently used entries that exceed our cache size.
        let mut statement =
            transaction.prepare("SELECT time, length(data) FROM cache ORDER BY time DESC")?;
        let mut times_and_sizes = statement.query(())?;

        let mut acc = 0i64;
        let mut oldest_that_fits = None;
        while acc < max_size {
            match times_and_sizes.next()? {
                Some(row) => {
                    let t: i64 = row.get(0)?;
                    let s: i64 = row.get(1)?;
                    oldest_that_fits = Some(t);
                    acc += s;
                }
                None => {
                    // The whole cache fits. We're done.
                    return Ok(());
                }
            }
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
    chrono::Utc::now().timestamp_nanos_opt().unwrap()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn smoke() -> Result<()> {
        let c = Connection::open_in_memory()?;
        let mut cache = Cache::new(c)?;

        // We can put something in and read it out.
        let buf = [1, 2, 3, 4];
        cache.insert("foo", &buf)?;
        assert_eq!(buf.as_slice(), &cache.try_read("foo")?.unwrap());

        // Things that don't exist aren't there.
        assert_eq!(None, cache.try_read("bar")?);

        cache.insert("baz", &[1, 2, 3])?;

        // With the default yuge size, we can fit seven bytes.
        cache.prune()?;
        let names_left = |c: &mut Cache| {
            c.conn
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

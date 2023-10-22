use anyhow::Result;
use rusqlite::Connection;

/// Local cache for any and all backends.
///
/// Originally this was going to just be a directory of files,
/// but we want to enforce some max cache size,
/// and multiple processes all trying to stat the same directory while inserting
/// and deleting files sounds like a pain in the ass, to put it mildly.
/// Retrying whenever the directory changes under our feet sounds similarly unpleasant.
/// What if we had some mechanism that was atomic, consistent, isolated, and durable?
struct Cache {
    conn: Connection,
}

// 1G. Make this configurable with global settings (~/.config/backpak?)
const CACHE_SIZE: i64 = 1024 * 1024 * 1024;

impl Cache {
    /// Create a cache given the database connection - let users handle the creation
    /// to make it easy to pass in `Connection::open_in_memory()`, etc.
    fn new(mut conn: Connection) -> Result<Self> {
        let tx = conn.transaction()?;
        let ver: i32 = tx.query_row("PRAGMA user_version", (), |r| r.get(0))?;
        if ver < 1 {
            tx.execute(
                "CREATE TABLE cache (
                    name TEXT NOT NULL PRIMARY KEY,
                    time INTEGER NOT NULL,
                    data BLOB NOT NULL
                ) STRICT",
                (),
            )?;
            // Make concurrent processes work off the same settings
            // (for now, just total size).
            tx.execute(
                "CREATE TABLE settings (
                    key TEXT NOT NULL PRIMARY KEY,
                    value NOT NULL
                )",
                (),
            )?;
        }
        tx.execute("PRAGMA user_version=1", ())?;
        tx.commit()?;

        let jm: String = conn.query_row("PRAGMA journal_mode=wal", (), |r| r.get(0))?;
        assert_eq!(jm, "wal");

        // Last guy wins.
        conn.execute(
            "REPLACE INTO settings(key, value) VALUES ('size', ?1)",
            [CACHE_SIZE],
        )?;

        Ok(Self { conn })
    }

    fn try_read(&self, name: &str) -> Result<Option<Vec<u8>>> {
        let blobs = self
            .conn
            .prepare("SELECT data FROM cache where name = ?1")?
            .query_map([name], |r| r.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        assert!(blobs.len() <= 1);
        Ok(blobs.into_iter().next())
    }

    fn insert(&self, name: &str, contents: &[u8]) -> Result<()> {
        self.conn.execute(
            "REPLACE INTO cache(name, time, data) VALUES (?1, ?2, ?3)",
            (name, now_nanos(), contents),
        )?;
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
        let c = Connection::open("test.sqlite")?;
        let cache = Cache::new(c)?;
        let buf = [1, 2, 3, 4];
        cache.insert("foo", &buf)?;
        assert_eq!(buf.as_slice(), &cache.try_read("foo")?.unwrap());
        assert_eq!(None, cache.try_read("bar")?);
        Ok(())
    }
}

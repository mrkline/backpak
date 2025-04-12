use super::*;

use std::fs;

use anyhow::Result;
use b2::Session;
use backpak_b2 as b2;
use byte_unit::Byte;

pub struct BackblazeBackend {
    pub session: Session,
}

pub fn initialize(
    repository: &camino::Utf8Path,
    pack_size: Byte,
    filter: Option<(String, String)>,
    key_id: String,
    application_key: String,
    bucket: String,
    concurrent_connections: u32,
) -> Result<()> {
    let c = super::Config {
        pack_size,
        kind: super::Kind::Backblaze {
            key_id,
            application_key,
            bucket,
            concurrent_connections,
        },
        filter,
    };
    let fh = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(repository)
        .with_context(|| format!("Couldn't create {repository}"))?;

    super::write_config(fh, c)?;
    Ok(())
}

impl BackblazeBackend {
    pub fn open(key_id: &str, application_key: &str, bucket: &str) -> Result<Self> {
        let session = Session::new(key_id, application_key, bucket)?;
        Ok(Self { session })
    }
}

// We could make the timeout configurable, make this generic across different errors, etc.
fn retry<T, F: FnMut() -> b2::Result<T>>(mut f: F) -> b2::Result<T> {
    loop {
        match f() {
            Ok(k) => return Ok(k),
            Err(e) => {
                // Assume IO errors are issues with our machine that won't resolve quickly,
                // and everything else to be server-side, temporary sadness.
                if matches!(e, b2::Error::Io(_)) {
                    return Err(e);
                } else {
                    warn!("{e}, retrying");
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        }
    }
}

impl Backend for BackblazeBackend {
    fn read(&self, from: &str) -> Result<Box<dyn Read + Send + 'static>> {
        let r = retry(|| self.session.get(from))?;
        Ok(Box::new(r))
    }

    fn write(&self, len: u64, from: &mut (dyn Read + Send), to: &str) -> Result<()> {
        retry(|| self.session.put(to, len, from))?;
        Ok(())
    }

    fn remove(&self, which: &str) -> Result<()> {
        retry(|| self.session.delete(which))?;
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<(String, u64)>> {
        let l = retry(|| self.session.list(Some(prefix)))?;
        Ok(l)
    }
}

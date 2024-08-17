use super::*;

use std::fs;

use anyhow::{ensure, Result};
use b2::Session;
use byte_unit::Byte;

pub struct BackblazeBackend {
    pub session: Session,
}

pub fn initialize(
    repository: &camino::Utf8Path,
    pack_size: Byte,
    key_id: String,
    application_key: String,
    bucket: String,
    filter: Option<String>,
    unfilter: Option<String>,
) -> Result<()> {
    ensure!(
        filter.is_some() == unfilter.is_some(),
        "{repository} config should set `filter` and `unfilter` or neither."
    );
    let c = super::Config {
        pack_size,
        kind: super::Kind::Backblaze {
            key_id,
            application_key,
            bucket,
        },
        filter,
        unfilter,
    };
    let mut fh = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(repository)
        .with_context(|| format!("Couldn't create {repository}"))?;

    fh.write_all(toml::to_string(&c).unwrap().as_bytes())?;
    Ok(())
}

impl BackblazeBackend {
    pub fn open(key_id: &str, application_key: &str, bucket: &str) -> Result<Self> {
        let session = Session::new(key_id, application_key, bucket)?;
        Ok(Self { session })
    }
}

// Sad hackage. Make less sad once minreq (or our flavor thereof)
// supports uploading and downloading as Read.

impl Backend for BackblazeBackend {
    fn read(&self, from: &str) -> Result<Box<dyn Read + Send + 'static>> {
        let r = self.session.get(from)?;
        Ok(Box::new(r))
    }

    fn write(&self, len: u64, from: &mut (dyn Read + Send), to: &str) -> Result<()> {
        self.session.put(to, len, from)?;
        Ok(())
    }

    fn remove(&self, which: &str) -> Result<()> {
        self.session.delete(which)?;
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<(String, u64)>> {
        let l = self.session.list(Some(prefix))?;
        Ok(l)
    }
}

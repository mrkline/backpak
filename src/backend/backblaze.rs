use super::*;

use std::fs;

use anyhow::Result;
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
    filter: Option<(String, String)>,
) -> Result<()> {
    let c = super::Config {
        pack_size,
        kind: super::Kind::Backblaze {
            key_id,
            application_key,
            bucket,
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

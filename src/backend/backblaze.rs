use super::*;

use std::io::Cursor;

use b2::Session;

pub struct BackblazeBackend {
    pub session: Session,
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
        let buf = self.session.get(from)?;
        Ok(Box::new(Cursor::new(buf)))
    }

    fn write(&self, from: &mut (dyn Read + Send), to: &str) -> Result<()> {
        let mut buf = vec![];
        from.read_to_end(&mut buf)?;
        self.session.put(to, &buf)?;
        Ok(())
    }

    fn remove(&self, which: &str) -> Result<()> {
        self.session.delete(which)?;
        Ok(())
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let l = self.session.list(Some(prefix))?;
        Ok(l)
    }
}

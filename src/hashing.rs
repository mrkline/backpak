use std::fmt;
use std::io;
use std::io::prelude::*;

use sha2::{digest::generic_array::GenericArray, Digest, Sha224};

/// The hash (a SHA224) used to identify all objects in our system.
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct ObjectId {
    digest: GenericArray<u8, <Sha224 as Digest>::OutputSize>,
}

impl ObjectId {
    pub fn new(bytes: &[u8]) -> Self {
        Self {
            digest: Sha224::digest(bytes),
        }
    }

    fn from_digest(digest: GenericArray<u8, <Sha224 as Digest>::OutputSize>) -> Self {
        Self { digest }
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.digest.fmt(f)
    }
}

impl fmt::LowerHex for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.digest.fmt(f)
    }
}

impl fmt::UpperHex for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.digest.fmt(f)
    }
}

impl serde::Serialize for ObjectId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.digest.as_slice())
    }
}

impl<'de> serde::Deserialize<'de> for ObjectId {
    fn deserialize<D>(deserializer: D) -> Result<ObjectId, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: Vec<u8> = serde_bytes::deserialize(deserializer)?;
        Ok(ObjectId::new(&bytes))
    }
}

pub struct HashingWriter<W> {
    inner: W,
    hasher: Sha224,
}

#[allow(dead_code)]
impl<W: Write> HashingWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha224::new(),
        }
    }

    pub fn inner(&self) -> &W {
        &self.inner
    }

    pub fn finalize(self) -> (ObjectId, W) {
        (ObjectId::from_digest(self.hasher.finalize()), self.inner)
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let write_result = self.inner.write(buf);
        if let Ok(count) = write_result {
            self.hasher.update(&buf[..count]);
        }
        write_result
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

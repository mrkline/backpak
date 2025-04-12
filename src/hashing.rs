//! Tools for hashing everything we care about into a unique [`ObjectId`]

use std::fmt;
use std::io;
use std::io::prelude::*;

use anyhow::{Context, Result, ensure};
use data_encoding::BASE32_DNSSEC as BASE32HEX;
use sha2::{Digest, Sha224, digest::Output};

type Sha224Digest = Output<Sha224>;

/// The hash (a SHA224) used to identify all objects in our system.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ObjectId {
    digest: Sha224Digest,
}

impl ObjectId {
    /// Calculates an ID from the given bytes
    pub fn hash(bytes: &[u8]) -> Self {
        Self {
            digest: Sha224::digest(bytes),
        }
    }

    fn from_digest(digest: Sha224Digest) -> Self {
        Self { digest }
    }

    /// Gets a git-like shortened version of the hash that's unique enough
    /// for most UI uses.
    pub fn short_name(&self) -> String {
        let mut full = format!("{}", self);
        let _rest = full.split_off(8);
        full
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{ digest: {} }}", BASE32HEX.encode(&self.digest))
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", BASE32HEX.encode(&self.digest))
    }
}

impl std::str::FromStr for ObjectId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = BASE32HEX
            .decode(s.as_bytes())
            .with_context(|| format!("Couldn't decode {s} as base32"))?;

        ensure!(
            bytes.len() == <Sha224 as Digest>::output_size(),
            "Expected SHA224 base32hex"
        );
        Ok(ObjectId::from_digest(*Sha224Digest::from_slice(&bytes)))
    }
}

impl serde::Serialize for ObjectId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // HORRENDOUS HACK
        // When saving our actual metadata to disk/cloud, we want to serialize
        // hashes as their raw bytes, but when printing objects in the `cat`
        // subcommand, we want hex.
        //
        // `serde_hex` won't work because that _always_ makes bytes hex, which
        // isn't what we want.
        //
        // `std::any::Any` won't work because `S` would have to be `'static`,
        // and we can't put additional bounds on the Serialize trait's `serialize()`.
        //
        // `serde_state` might work, but would need more plumbing.
        //
        // So hang your head in shame and use a global variable.
        // (Obvious but worth saying: set it at the start and don't mess with it after.)
        if crate::prettify::should_prettify() {
            serializer.serialize_str(&BASE32HEX.encode(self.digest.as_slice()))
        } else {
            serializer.serialize_bytes(self.digest.as_slice())
        }
    }
}

impl<'de> serde::Deserialize<'de> for ObjectId {
    fn deserialize<D>(deserializer: D) -> Result<ObjectId, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: Vec<u8> = serde_bytes::deserialize(deserializer)?;
        Ok(ObjectId::from_digest(*Sha224Digest::from_slice(&bytes)))
    }
}

pub struct HashingReader<R> {
    inner: R,
    hasher: Sha224,
}

impl<R: Read> HashingReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: Sha224::new(),
        }
    }

    pub fn finalize(self) -> (ObjectId, R) {
        (ObjectId::from_digest(self.hasher.finalize()), self.inner)
    }
}

impl<R: Read> Read for HashingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let count = self.inner.read(buf)?;
        self.hasher.update(&buf[..count]);
        Ok(count)
    }
}

pub struct HashingWriter<W> {
    inner: W,
    hasher: Sha224,
}

impl<W: Write> HashingWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            hasher: Sha224::new(),
        }
    }

    pub fn finalize(self) -> (ObjectId, W) {
        (ObjectId::from_digest(self.hasher.finalize()), self.inner)
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let count = self.inner.write(buf)?;
        self.hasher.update(&buf[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const DEVELOPERS: &[u8] = b"Developers, developers, developers, developers!".as_slice();

    const EXPECTED: &[u8] =
        &hex_literal::hex!("354e63924f01c3b921222ab4d5b4a77ef67d04bedf437eef66d2e0d6");

    #[test]
    fn smoke() {
        let id = ObjectId::hash(DEVELOPERS);
        assert_eq!(id.digest.as_slice(), EXPECTED);
    }

    #[test]
    fn reader() -> Result<()> {
        let mut r = HashingReader::new(DEVELOPERS);
        io::copy(&mut r, &mut io::sink())?;
        assert_eq!(r.finalize().0.digest.as_slice(), EXPECTED);
        Ok(())
    }

    #[test]
    fn writer() -> Result<()> {
        let mut w = HashingWriter::new(io::sink());
        w.write_all(DEVELOPERS)?;
        assert_eq!(w.finalize().0.digest.as_slice(), EXPECTED);
        Ok(())
    }
}

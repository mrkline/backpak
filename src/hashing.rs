use std::io;
use std::io::prelude::*;

use sha2::{digest::generic_array::GenericArray, Digest, Sha224};

pub type Sha224Sum = GenericArray<u8, <Sha224 as Digest>::OutputSize>;

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

    pub fn finalize(self) -> (Sha224Sum, W) {
        (self.hasher.finalize(), self.inner)
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

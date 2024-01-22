//! Defines [`Blob`], our fundamental unit of backup.

use serde_derive::{Deserialize, Serialize};

use crate::chunk::FileSpan;
use crate::hashing::ObjectId;

/// A chunk of a file or a tree to place in a pack.
///
/// Our fundamental unit of backup.
#[derive(Debug, Clone)]
pub struct Blob {
    /// The bytes to back up
    pub contents: Contents,
    /// The ID of said bytes
    pub id: ObjectId,
    /// Is the blob a chunk or a tree?
    pub kind: Type,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Type {
    /// A chunk of a file.
    Chunk,
    /// File and directory metadata
    Tree,
}

/// Either part of a loaded file or [`Vec`] buffer.
///
/// Formerly this was some `Box<AsRef<u8> + Send + Sync>`,
/// but let's cut down on the indirection where there's only a few choices.
///
/// We could _almost_ elminate [`Blob::kind`] and make this
///
///     pub enum Contents {
///         Tree(Vec<u8>),
///         Chunk(FileSpan),
///     }
///
/// since chunks are almost always / `FileSpan`s and trees are almost always `Buffer`s. Almost...
/// Except for the fact that chunks read from an existing pack file (e.g., when repacking)
/// are also `Buffer`s.
#[derive(Debug, Clone)]
pub enum Contents {
    Buffer(Vec<u8>),
    Span(FileSpan),
}

impl Blob {
    /// Convenience method to get at the blob's contents as a byte slice
    pub fn bytes(&self) -> &[u8] {
        match &self.contents {
            Contents::Buffer(v) => v,
            Contents::Span(s) => s.as_ref(),
        }
    }
}

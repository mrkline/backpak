//! Uniquely ID and store directories and their metadata

use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;

use anyhow::{anyhow, ensure, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use chrono::prelude::*;
use log::*;
use rustc_hash::{FxHashMap, FxHashSet};
use serde_derive::{Deserialize, Serialize};

use crate::backend;
use crate::counters;
use crate::hashing::ObjectId;
use crate::index;
use crate::pack;
use crate::prettify;

/// The contents of a directory entry (file, directory, symlink)
///
/// Files have chunks, and a directory has a subtree representing
/// everything in that subdirectory.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum NodeContents {
    File { chunks: Vec<ObjectId> },
    Directory { subtree: ObjectId },
    Symlink { target: Utf8PathBuf },
}

impl NodeContents {
    // Convenience methods for when we know the type already.

    #[inline]
    pub fn chunks(&self) -> &[ObjectId] {
        match self {
            NodeContents::File { chunks } => chunks,
            _ => panic!("Expected a file"),
        }
    }

    #[inline]
    pub fn subtree(&self) -> &ObjectId {
        match self {
            NodeContents::Directory { subtree } => subtree,
            _ => panic!("Expected a directory"),
        }
    }

    #[inline]
    pub fn target(&self) -> &Utf8Path {
        match self {
            NodeContents::Symlink { target } => target,
            _ => panic!("Expected a symlink"),
        }
    }
}

/// Backup-relevant metadata taken from a `stat()` call on a Posix system.
#[derive(Debug, Serialize, Deserialize)]
pub struct PosixMetadata {
    pub mode: u32,
    pub size: u64,
    pub user_id: u32,
    pub group_id: u32,
    #[serde(with = "prettify::date_time")]
    pub access_time: DateTime<Utc>,
    #[serde(with = "prettify::date_time")]
    pub modify_time: DateTime<Utc>,
    #[serde(with = "prettify::date_time")]
    pub change_time: DateTime<Utc>,
}

// We don't want to make or break metadata equivalence with access time -
// just looking at a file means its metadata probably wouldn't match.

impl PartialEq for PosixMetadata {
    fn eq(&self, o: &Self) -> bool {
        self.mode == o.mode &&
            self.size == o.size &&
            self.user_id == o.user_id &&
            self.group_id == o.group_id &&
            // Skip access time! And change time (we can't set that!)
            self.modify_time == o.modify_time
    }
}

impl Eq for PosixMetadata {}

/// Backup-relevant metadata taken from a `GetFileInformationByHandle()` call
/// on Windows.
#[derive(Debug, Serialize, Deserialize)]
pub struct WindowsMetadata {
    pub attributes: u32,
    pub size: u64,
    #[serde(with = "prettify::date_time_option")]
    pub creation_time: Option<DateTime<Utc>>,
    #[serde(with = "prettify::date_time_option")]
    pub access_time: Option<DateTime<Utc>>,
    #[serde(with = "prettify::date_time_option")]
    pub write_time: Option<DateTime<Utc>>,
}

impl PartialEq for WindowsMetadata {
    fn eq(&self, o: &Self) -> bool {
        self.attributes == o.attributes &&
            self.size == o.size &&
            self.creation_time == o.creation_time &&
            // Skip access time!
            self.write_time == o.write_time
    }
}

impl Eq for WindowsMetadata {}

/// A file or directory's metadata - Windows or Posix.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum NodeMetadata {
    Posix(PosixMetadata),
    Windows(WindowsMetadata),
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum NodeType {
    File,
    Directory,
    Symlink,
    // TODO: Unknown?
}

impl NodeMetadata {
    pub fn kind(&self) -> NodeType {
        match self {
            NodeMetadata::Posix(p) => {
                // man inode
                let type_bits = p.mode & 0o170000;

                if type_bits == 0o0120000 {
                    NodeType::Symlink
                } else if type_bits == 0o0040000 {
                    NodeType::Directory
                } else {
                    NodeType::File
                }
            }
            NodeMetadata::Windows(w) => {
                // https://docs.microsoft.com/en-us/windows/win32/fileio/file-attribute-constants
                if (w.attributes & 0x400) != 0 {
                    NodeType::Symlink
                } else if (w.attributes & 0x10) != 0 {
                    NodeType::Directory
                } else {
                    NodeType::File
                }
            }
        }
    }

    /// File size (value isn't meaningful for directories)
    pub fn size(&self) -> u64 {
        match self {
            NodeMetadata::Posix(p) => p.size,
            NodeMetadata::Windows(w) => w.size,
        }
    }

    pub fn modification_time(&self) -> Option<DateTime<Utc>> {
        match self {
            NodeMetadata::Posix(p) => Some(p.modify_time),
            NodeMetadata::Windows(w) => w.write_time,
        }
    }

    pub fn access_time(&self) -> Option<DateTime<Utc>> {
        match self {
            NodeMetadata::Posix(p) => Some(p.access_time),
            NodeMetadata::Windows(w) => w.access_time,
        }
    }
}

#[cfg(unix)]
pub fn get_metadata(path: &Utf8Path) -> Result<NodeMetadata> {
    use std::os::unix::fs::MetadataExt;

    let meta = fs::symlink_metadata(path).with_context(|| format!("Couldn't stat {path}"))?;
    let mode = meta.mode();
    let size = meta.size();
    let user_id = meta.uid();
    let group_id = meta.gid();
    let access_time = chrono::Utc
        .timestamp_opt(meta.atime(), meta.atime_nsec() as u32)
        .unwrap();
    let modify_time = chrono::Utc
        .timestamp_opt(meta.mtime(), meta.mtime_nsec() as u32)
        .unwrap();
    let change_time = chrono::Utc
        .timestamp_opt(meta.ctime(), meta.ctime_nsec() as u32)
        .unwrap();

    Ok(NodeMetadata::Posix(PosixMetadata {
        mode,
        size,
        user_id,
        group_id,
        access_time,
        modify_time,
        change_time,
    }))
}

#[cfg(windows)]
pub fn get_metadata(path: &Utf8Path) -> Result<NodeMetadata> {
    use std::os::windows::fs::MetadataExt;

    let meta = fs::symlink_metadata(path).with_context(|| format!("Couldn't stat {path}"))?;
    let attributes = meta.file_attributes();
    let size = meta.file_size();

    let creation_time = windows_timestamp(meta.creation_time());
    let access_time = windows_timestamp(meta.last_access_time());
    let write_time = windows_timestamp(meta.last_write_time());

    Ok(NodeMetadata::Windows(WindowsMetadata {
        attributes,
        size,
        creation_time,
        access_time,
        write_time,
    }))
}

#[cfg(windows)]
fn windows_timestamp(ts: u64) -> Option<DateTime<Utc>> {
    // Windows returns 100ns intervals since January 1, 1601
    const TICKS_PER_SECOND: u64 = 1_000_000_000 / 100;

    if ts == 0 {
        None
    } else {
        let seconds = ts / TICKS_PER_SECOND;
        let nanos = (ts % TICKS_PER_SECOND) * 100;

        Some(
            Utc.with_ymd_and_hms(1601, 1, 1, 0, 0, 0).unwrap()
                + chrono::Duration::seconds(seconds as i64)
                + chrono::Duration::nanoseconds(nanos as i64),
        )
    }
}

/// A single file or directory and its metadata
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Node {
    #[serde(flatten)]
    pub contents: NodeContents,
    pub metadata: NodeMetadata,
}

impl Node {
    pub fn kind(&self) -> NodeType {
        // Assert that the contents matches the metadata while we're at it.
        // We can be confident this won't be flipped by corruption since
        // we verify a Tree's hash when deserializing it,
        // AND we're confident that we don't screw it up when generating trees
        // in ui/backup.rs
        match &self.contents {
            NodeContents::File { .. } => {
                assert_eq!(self.metadata.kind(), NodeType::File);
                NodeType::File
            }
            NodeContents::Directory { .. } => {
                assert_eq!(self.metadata.kind(), NodeType::Directory);
                NodeType::Directory
            }
            NodeContents::Symlink { .. } => {
                assert_eq!(self.metadata.kind(), NodeType::Symlink);
                NodeType::Symlink
            }
        }
    }
}

/// A tree represents a single directory of files (with contents),
/// directories (with subtrees), and their metadata, addressed by entry name.
pub type Tree = BTreeMap<Utf8PathBuf, Node>;

/// Serialize the tree into its on-disk CBOR representation and return its
/// ID (hash)
pub fn serialize_and_hash(tree: &Tree) -> Result<(Vec<u8>, ObjectId)> {
    let mut tree_cbor = Vec::new();
    ciborium::into_writer(tree, &mut tree_cbor)?;
    let id = ObjectId::hash(&tree_cbor);
    Ok((tree_cbor, id))
}

/// A collection of trees (which can reference each other as subtrees),
/// used to represent a directory hierarchy.
///
/// We use a FxHashMap because we never serialize a whole forest as a single object,
/// so we'll take constant-time lookup over deterministic order.
/// We use an `Arc<Tree>` so that a Forest can be used as a tree cache,
/// doling out references to its trees.
/// We use Arc and not Rc so that functions can operate in parallel on all
/// trees in the forest.
pub type Forest = FxHashMap<ObjectId, Arc<Tree>>;

/// A read-through cache of trees that extracts them from packs on-demand
pub struct Cache<'a> {
    /// The master index, used to look up a pack's manifest from its ID
    index: &'a index::Index,

    /// Finds the pack that contains a given blob
    blob_to_pack_map: &'a index::BlobMap,

    /// Gets packs on-demand from the backend.
    pack_cache: &'a backend::CachedBackend,

    /// Our actual tree cache.
    tree_cache: Forest,
}

impl<'a> Cache<'a> {
    pub fn new(
        index: &'a index::Index,
        blob_to_pack_map: &'a index::BlobMap,
        pack_cache: &'a backend::CachedBackend,
    ) -> Self {
        Self {
            index,
            blob_to_pack_map,
            pack_cache,
            tree_cache: Forest::default(),
        }
    }

    /// Reads the given tree from the cache,
    /// fishing it out of its packfile if required.
    pub fn read(&mut self, id: &ObjectId) -> Result<Arc<Tree>> {
        if let Some(t) = self.tree_cache.get(id) {
            trace!("Found tree {id} in-cache");
            counters::bump(counters::Op::TreeCacheHit);
            return Ok(t.clone());
        } else {
            counters::bump(counters::Op::TreeCacheMiss);
        }

        let pack_id = self
            .blob_to_pack_map
            .get(id)
            .ok_or_else(|| anyhow!("No pack contains tree {}", id))?;

        debug!("Reading pack {pack_id} into tree cache to get tree {id}");
        let mut pack_containing_tree = self.pack_cache.read_pack(pack_id)?;
        let manifest = self
            .index
            .packs
            .get(pack_id)
            .expect("Pack ID in blob -> pack map but not the index");

        pack::append_to_forest(&mut pack_containing_tree, manifest, &mut self.tree_cache)?;

        self.tree_cache
            .get(id)
            .ok_or_else(|| anyhow!("Tree {} missing from pack {}", id, pack_id))
            .cloned()
    }
}

/// Reads the given tree and all its subtrees from the given tree cache.
pub fn forest_from_root(root: &ObjectId, cache: &mut Cache) -> Result<Forest> {
    trace!("Assembling tree from root {}", root);
    let mut forest = Forest::default();
    let mut stack_set = FxHashSet::default();
    append_tree(root, &mut forest, cache, &mut stack_set)?;
    Ok(forest)
}

fn append_tree(
    tree_id: &ObjectId,
    forest: &mut Forest,
    cache: &mut Cache,
    stack_set: &mut FxHashSet<ObjectId>,
) -> Result<()> {
    ensure!(
        stack_set.insert(*tree_id),
        "Cycle detected! Tree {} loops up",
        tree_id
    );

    let tree = cache.read(tree_id)?;
    forest.insert(*tree_id, tree.clone());
    for val in tree.values().map(|v| &v.contents) {
        match val {
            NodeContents::Directory { subtree } => {
                append_tree(subtree, forest, cache, stack_set)?;
            }
            NodeContents::File { .. } | NodeContents::Symlink { .. } => {}
        };
    }

    assert!(stack_set.remove(tree_id));
    Ok(())
}

/// Collect the set of chunks for the files in the given forest
pub fn chunks_in_forest(forest: &Forest) -> FxHashSet<&ObjectId> {
    forest
        .values()
        .map(|t| chunks_in_tree(t))
        .reduce(|mut a, b| {
            a.extend(b);
            a
        })
        .unwrap_or_default()
}

/// Collect the set of chunks for the files the given tree
pub fn chunks_in_tree(tree: &Tree) -> FxHashSet<&ObjectId> {
    tree.values()
        .map(chunks_in_node)
        .fold(FxHashSet::default(), |mut set, node_chunks| {
            for chunk in node_chunks {
                set.insert(chunk);
            }
            set
        })
}

/// Return the slice of chunks in a file node,
/// or an empty slice if `node` is a directory or symlink
pub fn chunks_in_node(node: &Node) -> &[ObjectId] {
    match &node.contents {
        NodeContents::File { chunks, .. } => chunks,
        NodeContents::Directory { .. } | NodeContents::Symlink { .. } => &[],
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    /// Pack manifest and ID remains stable from build to build.
    fn stability() -> Result<()> {
        init();

        let mut tree = BTreeMap::new();
        tree.insert(
            Utf8PathBuf::from("UnixFileNode"),
            Node {
                contents: NodeContents::File {
                    chunks: vec![
                        ObjectId::hash(b"first chunk"),
                        ObjectId::hash(b"second chunk"),
                        ObjectId::hash(b"third chunk"),
                    ],
                },
                metadata: NodeMetadata::Posix(PosixMetadata {
                    mode: 0o644,
                    size: 42,
                    user_id: 1234,
                    group_id: 5678,
                    access_time: DateTime::parse_from_rfc3339("2020-10-30T06:30:25.157873535Z")
                        .unwrap()
                        .into(),
                    modify_time: DateTime::parse_from_rfc3339("2020-10-30T06:30:25.034542588Z")
                        .unwrap()
                        .into(),
                    change_time: DateTime::parse_from_rfc3339("2020-10-30T06:30:25.034542588Z")
                        .unwrap()
                        .into(),
                }),
            },
        );
        tree.insert(
            Utf8PathBuf::from("WindowsDirNode"),
            Node {
                contents: NodeContents::Directory {
                    subtree: ObjectId::hash(b"some subdirectory"),
                },
                metadata: NodeMetadata::Windows(WindowsMetadata {
                    attributes: 0xdeadbeef,
                    size: 42,
                    creation_time: None,
                    access_time: Some(
                        DateTime::parse_from_rfc3339("2020-10-29T09:11:05.701157660Z")
                            .unwrap()
                            .into(),
                    ),
                    write_time: Some(
                        DateTime::parse_from_rfc3339("2020-10-24T01:22:27.624697907Z")
                            .unwrap()
                            .into(),
                    ),
                }),
            },
        );

        let (serialized_tree, id) = serialize_and_hash(&tree)?;

        /*
        use std::io::Write;
        let mut fh = std::fs::File::create("tests/references/tree.stability")?;
        fh.write_all(&serialized_tree)?;
        */

        // ID remains stable
        assert_eq!(
            format!("{}", id),
            "3rt1hk2crrhsadv2u1jhatigf7jvi21qmnnnqhl8vh34a"
        );
        // Contents remain stable
        // (We could just use the ID and length,
        // but having some example CBOR in the repo seems helpful.)
        let from_example = fs::read("tests/references/tree.stability")?;
        assert_eq!(serialized_tree, from_example);
        Ok(())
    }
}

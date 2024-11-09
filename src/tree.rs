//! Uniquely ID and store directories and their metadata

use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;

use anyhow::{anyhow, ensure, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use jiff::Timestamp;
use rustc_hash::{FxHashMap, FxHashSet};
use serde_derive::{Deserialize, Serialize};
use tracing::*;

use crate::backend;
use crate::counters;
use crate::hashing::ObjectId;
use crate::index;
use crate::pack;
use crate::prettify;

/// How should we handle symbolic links?
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Symlink {
    /// Read them as symbolic links.
    Read,
    /// Follow symbolic links to their destination.
    Dereference,
}

/// The contents of a directory entry (file, directory, symlink)
///
/// Files have chunks, and a directory has a subtree representing
/// everything in that subdirectory.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
// If we give the field in each variant a unique name then flatten this
// into the node, we'll get:
//     "FileName": { "chunks": [...], "metadata": {...}},
//     "DirName": { "tree": <ID>, "metadata": {...}},
//     "SymlinkName": { "symlink": <PATH>, "metadata": {...}},
#[serde(untagged)]
pub enum NodeContents {
    File {
        #[serde(rename = "chunks")]
        chunks: Vec<ObjectId>,
    },
    Directory {
        #[serde(rename = "tree")]
        subtree: ObjectId,
    },
    Symlink {
        #[serde(rename = "symlink")]
        target: Utf8PathBuf,
    },
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PosixMetadata {
    pub mode: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(rename = "uid")]
    pub user_id: u32,
    #[serde(rename = "gid")]
    pub group_id: u32,
    #[serde(rename = "atime", with = "prettify::instant")]
    pub access_time: Timestamp,
    #[serde(rename = "mtime", with = "prettify::instant")]
    pub modify_time: Timestamp,
    // No change time - it's when the metadata changes, and since we can't set that
    // when restoring a file, nor compare it meaningfully between snapshots,
    // just leave it off.
}

/// Backup-relevant metadata taken from a `GetFileInformationByHandle()` call
/// on Windows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowsMetadata {
    pub attributes: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub size: Option<u64>,
    // Unlike POSIX, all three of these can be set:
    // https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-setfiletime
    // so recording them is helpful.
    #[serde(with = "prettify::instant_option")]
    pub creation_time: Option<Timestamp>,
    #[serde(with = "prettify::instant_option")]
    pub access_time: Option<Timestamp>,
    #[serde(with = "prettify::instant_option")]
    pub write_time: Option<Timestamp>,
}

/// A file or directory's metadata - Windows or Posix.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum NodeMetadata {
    Posix(PosixMetadata),
    Windows(WindowsMetadata),
}

/// For printing metadata diffs. See the `backpak diff` helptext
pub fn meta_diff_char(l: &NodeMetadata, r: &NodeMetadata) -> Option<char> {
    if l == r {
        return None;
    }
    use NodeMetadata::*;
    let c = match (l, r) {
        (Posix(lp), Posix(rp)) => {
            if lp.user_id != rp.user_id || lp.group_id != rp.group_id {
                'O'
            } else if lp.mode != rp.mode {
                'P'
            } else if lp.modify_time != rp.modify_time {
                'T'
            } else if lp.access_time != rp.access_time {
                'A'
            } else {
                'M'
            }
        }
        // TODO: Compare Windows
        (Windows(_), Windows(_)) => 'M',
        // TODO: Compare across  kinds
        (Posix(_), Windows(_)) => 'M',
        (Windows(_), Posix(_)) => 'M',
    };
    Some(c)
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum NodeType {
    File,
    Directory,
    Symlink,
    Unsupported(u32),
}

// Make these fail so we bail on weird file types?

fn posix_kind(mode: u32) -> NodeType {
    // man inode
    let type_bits = mode & 0o170000;

    match type_bits {
        0o0120000 => NodeType::Symlink,
        0o0040000 => NodeType::Directory,
        0o0100000 => NodeType::File,
        wut => NodeType::Unsupported(wut),
    }
}

fn windows_kind(attributes: u32) -> NodeType {
    // https://docs.microsoft.com/en-us/windows/win32/fileio/file-attribute-constants
    if (attributes & 0x400) != 0 {
        NodeType::Symlink
    } else if (attributes & 0x10) != 0 {
        NodeType::Directory
    } else {
        NodeType::File
    }
}

impl NodeMetadata {
    pub fn kind(&self) -> NodeType {
        match self {
            NodeMetadata::Posix(p) => posix_kind(p.mode),
            NodeMetadata::Windows(w) => windows_kind(w.attributes),
        }
    }

    /// File size (None for Directories and Symlinks)
    ///
    /// Both OSes *do* provide the size for symlinks,
    /// but we can just as easily look at the length of the string.
    /// File size is much more useful - files can be massive, and we don't know how big chunks are.
    pub fn size(&self) -> Option<u64> {
        match self {
            NodeMetadata::Posix(p) => p.size,
            NodeMetadata::Windows(w) => w.size,
        }
    }

    pub fn modification_time(&self) -> Option<Timestamp> {
        match self {
            NodeMetadata::Posix(p) => Some(p.modify_time),
            NodeMetadata::Windows(w) => w.write_time,
        }
    }

    pub fn access_time(&self) -> Option<Timestamp> {
        match self {
            NodeMetadata::Posix(p) => Some(p.access_time),
            NodeMetadata::Windows(w) => w.access_time,
        }
    }
}

#[cfg(unix)]
pub fn get_metadata(symlink_behavior: Symlink, path: &Utf8Path) -> Result<NodeMetadata> {
    use std::os::unix::fs::MetadataExt;

    let meta = match symlink_behavior {
        Symlink::Read => fs::symlink_metadata(path),
        Symlink::Dereference => fs::metadata(path),
    }
    .with_context(|| format!("Couldn't stat {path}"))?;
    let mode = meta.mode();
    let size = (posix_kind(mode) == NodeType::File).then(|| meta.size());
    let user_id = meta.uid();
    let group_id = meta.gid();
    let access_time = Timestamp::new(meta.atime(), meta.atime_nsec() as i32).unwrap();
    let modify_time = Timestamp::new(meta.mtime(), meta.mtime_nsec() as i32).unwrap();

    Ok(NodeMetadata::Posix(PosixMetadata {
        mode,
        size,
        user_id,
        group_id,
        access_time,
        modify_time,
    }))
}

#[cfg(windows)]
pub fn get_metadata(symlink_behavior: Symlink, path: &Utf8Path) -> Result<NodeMetadata> {
    use std::os::windows::fs::MetadataExt;

    let meta = match symlink_behavior {
        Symlink::Read => fs::symlink_metadata(path),
        Symlink::Dereference => fs::metadata(path),
    }
    .with_context(|| format!("Couldn't stat {path}"))?;
    let attributes = meta.file_attributes();
    let size = (windows_kind(attributes) == NodeType::File).then(|| meta.file_size());
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
fn windows_timestamp(ts: u64) -> Option<Timestamp> {
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
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
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
///
/// On cache misses, it uses a blob -> pack map to figure out which pack
/// that tree is in, then caches the entire pack (of trees).
/// This works well since:
/// 1. Trees in the same hierarchy (forest) are usually in the same pack.
/// 2. Packs are compressed, which means we can't just seek to the one tree we want.
///    We might as well deserialize while we decompress.
pub struct Cache<'a> {
    /// The master index, used to look up a pack's manifest from its ID
    index: &'a index::Index,

    /// Finds the pack that contains a given blob
    blob_to_pack_map: &'a index::BlobMap<'a>,

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
    /// reading a new packfile if required.
    pub fn read(&mut self, id: &ObjectId) -> Result<Arc<Tree>> {
        if let Some(t) = self.tree_cache.get(id) {
            counters::bump(counters::Op::TreeCacheHit);
            return Ok(t.clone());
        }

        counters::bump(counters::Op::TreeCacheMiss);

        let pack_id = self
            .blob_to_pack_map
            .get(id)
            .ok_or_else(|| anyhow!("No pack contains tree {}", id))?;

        trace!("Cache miss; reading pack {pack_id}");
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
    trace!("Assembling forest from root {}", root);
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

/// Collect the set of chunks for the files in the given tree
pub fn chunks_in_tree(tree: &Tree) -> FxHashSet<ObjectId> {
    tree.values()
        .map(chunks_in_node)
        .fold(FxHashSet::default(), |mut set, node_chunks| {
            for chunk in node_chunks {
                set.insert(*chunk);
            }
            set
        })
}

/// Return the slice of chunks in a file node,
/// or an empty slice if `node` is a directory or symlink
fn chunks_in_node(node: &Node) -> &[ObjectId] {
    match &node.contents {
        NodeContents::File { chunks, .. } => chunks,
        NodeContents::Directory { .. } | NodeContents::Symlink { .. } => &[],
    }
}

#[derive(Default)]
pub struct FileSize {
    pub introduced: u64,
    pub reused: u64,
}

#[derive(Default)]
pub struct ForestSizes {
    pub tree_bytes: u64,
    pub chunk_bytes: u64,
    pub introduced: u64,
    pub reused: u64,
    // &'a Utf8Path would be more ideal, but tying ourselves to the lifetime of the forest
    // we get the path names from is a pretty big PITA.
    pub per_file: Vec<(Utf8PathBuf, FileSize)>,
}

// Useful for totals in `usage`. Does _not_ concatenate per-file vecs since that would get yuge.
impl std::ops::AddAssign for ForestSizes {
    fn add_assign(&mut self, o: Self) {
        self.tree_bytes += o.tree_bytes;
        self.chunk_bytes += o.chunk_bytes;
        self.introduced += o.introduced;
        self.reused += o.reused;
        self.per_file.clear();
    }
}

/// Gets size breakdowns of the given forest, appending to `visited_blobs` as it... visits blobs.
///
/// This is useful for walking the repo snapshot by snapshot, showing when data is introduced,
/// when it is reused, and how much of each kind.
pub fn forest_sizes(
    root: &ObjectId,
    forest: &Forest,
    size_map: &FxHashMap<&ObjectId, u32>,
    visited_blobs: &mut FxHashSet<ObjectId>,
) -> Result<ForestSizes> {
    let mut s = ForestSizes::default();
    tree_size(
        Utf8Path::new(""),
        root,
        forest,
        size_map,
        visited_blobs,
        &mut s,
    )?;
    assert_eq!(s.tree_bytes + s.chunk_bytes, s.introduced + s.reused);
    Ok(s)
}

/// Get the size of blobs in the given tree (including said tree)
fn tree_size(
    prefix: &Utf8Path,
    tree_id: &ObjectId,
    forest: &Forest,
    size_map: &FxHashMap<&ObjectId, u32>,
    visited_blobs: &mut FxHashSet<ObjectId>,
    s: &mut ForestSizes,
) -> Result<()> {
    let tree: &Tree = forest
        .get(tree_id)
        .ok_or_else(|| anyhow!("Missing tree {tree_id}"))
        .unwrap();

    let ts = size_map
        .get(tree_id)
        .ok_or_else(|| anyhow!("Couldn't find tree {tree_id} to get size"))?;
    let ts = *ts as u64;
    s.tree_bytes += ts;
    if visited_blobs.insert(*tree_id) {
        s.introduced += ts;
    } else {
        s.reused += ts;
    }

    for (name, node) in tree {
        let mut p = prefix.to_owned();
        p.push(name);
        node_size(p, node, forest, size_map, visited_blobs, s)?
    }
    Ok(())
}

/// Get the size of the node if it's a file.
///
/// We've already accounted for tree sizes by summing the forest in [`forest_sizes`].
fn node_size(
    path: Utf8PathBuf,
    node: &Node,
    forest: &Forest,
    size_map: &FxHashMap<&ObjectId, u32>,
    visited_blobs: &mut FxHashSet<ObjectId>,
    s: &mut ForestSizes,
) -> Result<()> {
    match &node.contents {
        NodeContents::File { chunks, .. } => {
            let mut fs = FileSize::default();
            for c in chunks.iter().map(|c| {
                size_map
                    .get(c)
                    .ok_or_else(|| anyhow!("Couldn't find chunk {c} to get size"))
                    .map(|s| (c, *s))
            }) {
                let (chunk_id, chunk_size) = c?;
                let cs = chunk_size as u64;
                if visited_blobs.insert(*chunk_id) {
                    fs.introduced += cs;
                } else {
                    fs.reused += cs;
                }
            }
            s.introduced += fs.introduced;
            s.reused += fs.reused;
            s.chunk_bytes += fs.introduced + fs.reused;
            s.per_file.push((path, fs));
            Ok(())
        }
        NodeContents::Directory { subtree } => {
            tree_size(&path, subtree, forest, size_map, visited_blobs, s)
        }
        NodeContents::Symlink { .. } => Ok(()),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    /// Pack manifest and ID remains stable from build to build.
    fn stability() -> Result<()> {
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
                    size: Some(42),
                    user_id: 1234,
                    group_id: 5678,
                    access_time: "2020-10-30T06:30:25.157873535Z".parse().unwrap(),
                    modify_time: "2020-10-30T06:30:25.034542588Z".parse().unwrap(),
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
                    size: None,
                    creation_time: None,
                    access_time: Some("2020-10-29T09:11:05.701157660Z".parse().unwrap()),
                    write_time: Some("2020-10-24T01:22:27.624697907Z".parse().unwrap()),
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
            "8orh4h6b5dbmv2d7g0hu46rgsfhre7477kn9lgp97cqbe"
        );
        // Contents remain stable
        // (We could just use the ID and length,
        // but having some example CBOR in the repo seems helpful.)
        let from_example = fs::read("tests/references/tree.stability")?;
        assert_eq!(serialized_tree, from_example);
        Ok(())
    }
}

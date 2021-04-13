use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::*;
use chrono::prelude::*;
use log::*;
use rayon::prelude::*;
use serde_derive::*;

use crate::backend;
use crate::counters;
use crate::hashing::ObjectId;
use crate::index;
use crate::pack;
use crate::prettify;

/// The contents of a file or directory.
///
/// Files have chunks, and a directory has a subtree representing
/// everything in that subdirectory.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum NodeContents {
    File { chunks: Vec<ObjectId> },
    Directory { subtree: ObjectId },
}

impl NodeContents {
    // Convenience methods for when we know the type already.

    #[inline]
    pub fn chunks(&self) -> &[ObjectId] {
        match self {
            NodeContents::File { chunks } => chunks,
            _ => panic!("Expected a file, got a directory"),
        }
    }

    #[inline]
    pub fn subtree(&self) -> &ObjectId {
        match self {
            NodeContents::Directory { subtree } => subtree,
            _ => panic!("Expected a directory, got a file"),
        }
    }
}

/// Backup-relevant metadata taken from a `stat()` call on a Posix system.
#[derive(Debug, Serialize, Deserialize)]
pub struct PosixMetadata {
    mode: u32,
    size: u64,
    user_id: u32,
    group_id: u32,
    #[serde(with = "prettify::date_time")]
    access_time: DateTime<Utc>,
    #[serde(with = "prettify::date_time")]
    modify_time: DateTime<Utc>,
    #[serde(with = "prettify::date_time")]
    change_time: DateTime<Utc>,
}

// We don't want to make or break metadata equivalence with access time -
// just looking at a file means its metadata probably wouldn't match.

impl PartialEq for PosixMetadata {
    fn eq(&self, o: &Self) -> bool {
        self.mode == o.mode &&
            self.size == o.size &&
            self.user_id == o.user_id &&
            self.group_id == o.group_id &&
            // Skip access time!
            self.modify_time == o.modify_time &&
            self.change_time == o.change_time
    }
}

impl Eq for PosixMetadata {}

/// Backup-relevant metadata taken from a `GetFileInformationByHandle` call
/// on Windows.
#[derive(Debug, Serialize, Deserialize)]
pub struct WindowsMetadata {
    attributes: u32,
    size: u64,
    #[serde(with = "prettify::date_time_option")]
    creation_time: Option<DateTime<Utc>>,
    #[serde(with = "prettify::date_time_option")]
    access_time: Option<DateTime<Utc>>,
    #[serde(with = "prettify::date_time_option")]
    write_time: Option<DateTime<Utc>>,
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

impl NodeMetadata {
    pub fn is_directory(&self) -> bool {
        match self {
            NodeMetadata::Posix(p) => {
                // man inode
                (p.mode & 0o170000) == 0o0040000
            }
            NodeMetadata::Windows(w) => {
                // https://docs.microsoft.com/en-us/windows/win32/fileio/file-attribute-constants
                (w.attributes & 0x10) != 0
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
}

#[cfg(target_family = "unix")]
pub fn get_metadata(path: &Path) -> Result<NodeMetadata> {
    use std::os::unix::fs::MetadataExt;

    let meta = fs::metadata(path).with_context(|| format!("Couldn't stat {}", path.display()))?;
    let mode = meta.mode();
    let size = meta.size();
    let user_id = meta.uid();
    let group_id = meta.gid();
    let access_time = chrono::Utc.timestamp(meta.atime(), meta.atime_nsec() as u32);
    let modify_time = chrono::Utc.timestamp(meta.mtime(), meta.mtime_nsec() as u32);
    let change_time = chrono::Utc.timestamp(meta.ctime(), meta.ctime_nsec() as u32);

    Ok(NodeMetadata::Posix(PosixMetadata {
        mode,
        size,
        user_id,
        group_id,
        change_time,
        access_time,
        modify_time,
    }))
}

#[cfg(target_family = "windows")]
pub fn get_metadata(path: &Path) -> Result<NodeMetadata> {
    use std::os::windows::fs::MetadataExt;

    let meta = fs::metadata(path).with_context(|| format!("Couldn't stat {}", path.display()))?;
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

#[cfg(target_family = "windows")]
fn windows_timestamp(ts: u64) -> Option<DateTime<Utc>> {
    // Windows returns 100ns intervals since January 1, 1601
    const TICKS_PER_SECOND: u64 = 1_000_000_000 / 100;

    if ts == 0 {
        None
    } else {
        let seconds = ts / TICKS_PER_SECOND;
        let nanos = (ts % TICKS_PER_SECOND) * 100;

        Some(
            Utc.ymd(1601, 1, 1).and_hms(0, 0, 0)
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
    pub fn is_directory(&self) -> bool {
        // Assert that the contents matches the metadata while we're at it.
        // We can be confident this won't be flipped by corruption since
        // we verify a Tree's hash when deserializing it,
        // AND we're confident that we don't screw it up when generating trees
        // in ui/backup.rs
        match &self.contents {
            NodeContents::Directory { .. } => {
                assert!(self.metadata.is_directory());
                true
            }
            NodeContents::File { .. } => {
                assert!(!self.metadata.is_directory());
                false
            }
        }
    }
}

/// A tree represents a single directory of files (with contents),
/// directories (with subtrees), and their metadata, addressed by name.
pub type Tree = BTreeMap<PathBuf, Node>;

/// Serialize the tree into its on-disk CBOR representation and return its
/// ID (hash)
pub fn serialize_and_hash(tree: &Tree) -> Result<(Vec<u8>, ObjectId)> {
    let tree_cbor = serde_cbor::to_vec(tree)?;
    let id = ObjectId::hash(&tree_cbor);
    Ok((tree_cbor, id))
}

/// A collection of trees (which can reference each other as subtrees),
/// used to represent a directory hierarchy.
///
/// We use a HashMap because we never serialize a whole forest to our backup,
/// so we'll take constant-time lookup over deterministic order.
/// We use an Arc<Tree> so that a Forest can be used as a tree cache,
/// doling out references to its trees.
/// We use Arc and not Rc so that functions can operate in parallel on all
/// trees in the forest.
pub type Forest = HashMap<ObjectId, Arc<Tree>>;

/// A read-through cache of trees that extracts them from packs as-needed.
pub struct Cache<'a> {
    /// The master index, used to look up a pack's manifest from its ID
    index: &'a index::Index,

    /// Finds the pack that contains a given blob
    blob_to_pack_map: &'a index::BlobMap,

    /// Gets packs as-needed from the backend.
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
            tree_cache: Forest::new(),
        }
    }

    /// Reads the given tree from the cache,
    /// fishing it out of its packfile if required.
    pub fn read(&mut self, id: &ObjectId) -> Result<Arc<Tree>> {
        if let Some(t) = self.tree_cache.get(id) {
            trace!("Tree {} is in-cache", id);
            counters::bump(counters::Op::TreeCacheHit);
            return Ok(t.clone());
        } else {
            counters::bump(counters::Op::TreeCacheMiss);
        }

        let pack_id = self
            .blob_to_pack_map
            .get(id)
            .ok_or_else(|| anyhow!("No pack contains tree {}", id))?;

        trace!("Reading pack {} to get tree {}", pack_id, id);
        let mut pack_containing_tree = self.pack_cache.read_pack(&pack_id)?;
        let manifest = self
            .index
            .packs
            .get(&pack_id)
            .expect("Pack ID in blob -> pack map but not the index");

        pack::append_to_forest(&mut pack_containing_tree, &manifest, &mut self.tree_cache)?;

        self.tree_cache
            .get(id)
            .ok_or_else(|| anyhow!("Tree {} missing from pack {}", id, pack_id))
            .map(|entry| entry.clone())
    }
}

/// Reads the given tree and all its subtrees from the given tree cache.
pub fn forest_from_root(root: &ObjectId, cache: &mut Cache) -> Result<Forest> {
    trace!("Assembling tree from root {}", root);
    let mut forest = Forest::new();
    let mut stack_set = HashSet::new();
    append_tree(root, &mut forest, cache, &mut stack_set)?;
    Ok(forest)
}

fn append_tree(
    tree_id: &ObjectId,
    forest: &mut Forest,
    cache: &mut Cache,
    stack_set: &mut HashSet<ObjectId>,
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
            NodeContents::File { .. } => {}
        };
    }

    assert!(stack_set.remove(tree_id));
    Ok(())
}

/// Collect the set of chunks for the files in the given forest
pub fn chunks_in_forest(forest: &Forest) -> HashSet<&ObjectId> {
    forest
        .par_iter()
        .map(|(_id, tree)| chunks_in_tree(&*tree))
        .reduce(HashSet::new, |mut a, b| {
            a.extend(b);
            a
        })
}

/// Collect the set of chunks for the files the given tree
pub fn chunks_in_tree(tree: &Tree) -> HashSet<&ObjectId> {
    tree.par_iter()
        .map(|(_, node)| chunks_in_node(node))
        .fold_with(HashSet::new(), |mut set, node_chunks| {
            for chunk in node_chunks {
                set.insert(chunk);
            }
            set
        })
        .reduce(HashSet::new, |mut a, b| {
            a.extend(b);
            a
        })
}

/// Return the slice of chunks in a file node,
/// or an empty slice if `node` is a directory
pub fn chunks_in_node(node: &Node) -> &[ObjectId] {
    match &node.contents {
        NodeContents::Directory { .. } => &[],
        NodeContents::File { chunks, .. } => chunks,
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
            PathBuf::from("UnixFileNode"),
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
            PathBuf::from("WindowsDirNode"),
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
            "1efa18d04cdee3c537e2f06715765079e7f9083ab5ef7d46a8fc4645"
        );
        // Contents remain stable
        // (We could just use the ID and length,
        // but having some example CBOR in the repo seems helpful.)
        let from_example = fs::read("tests/references/tree.stability")?;
        assert_eq!(serialized_tree, from_example);
        Ok(())
    }
}

use anyhow::{anyhow, Result};
use clap::Parser;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{backend, file_util::nice_size, hashing::ObjectId, index, snapshot, tree};

use std::fmt;

/// List the snapshots in this repository from oldest to newest
#[derive(Debug, Parser)]
pub struct Args {
    /// Print newest to oldest
    #[clap(short, long)]
    reverse: bool,
}

pub fn run(repository: &camino::Utf8Path, args: Args) -> Result<()> {
    unsafe {
        crate::prettify::prettify_serialize();
    }

    let (_cfg, cached_backend) = backend::open(repository, backend::CacheBehavior::Normal)?;
    let snapshots = snapshot::load_chronologically(&cached_backend)?;
    let index = index::build_master_index(&cached_backend)?;
    let blob_map = index::blob_to_pack_map(&index)?;
    let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);
    let size_map = index::blob_to_size_map(&index)?;

    struct DecoratedSnapshot {
        snapshot: snapshot::Snapshot,
        id: ObjectId,
        sizes: ForestSizes,
    }

    let mut visited_blobs = FxHashSet::default();
    // NB: We collect at the end because our mapping is stateful;
    // we keep track of the visited blobs as we go.
    // (We do *not* want the DoubleEndedIterator from Map!)
    let snaps = snapshots
        .into_iter()
        .map(|(snapshot, id)| {
            let sizes = forest_sizes(
                &tree::forest_from_root(&snapshot.tree, &mut tree_cache)?,
                &size_map,
                &mut visited_blobs,
            )?;
            Ok(DecoratedSnapshot {
                snapshot,
                id,
                sizes,
            })
        })
        .collect::<Vec<_>>();

    let it: Box<dyn Iterator<Item = Result<DecoratedSnapshot>>> = if !args.reverse {
        Box::new(snaps.into_iter())
    } else {
        Box::new(snaps.into_iter().rev())
    };

    for decorated in it {
        let DecoratedSnapshot {
            snapshot,
            id,
            sizes,
        } = decorated?;
        print!("snapshot {}", id);
        if snapshot.tags.is_empty() {
            println!();
        } else {
            println!(
                " ({})",
                snapshot.tags.into_iter().collect::<Vec<String>>().join(" ")
            );
        }
        println!("{sizes}");
        println!("Author: {}", snapshot.author);

        println!("Date:   {}", snapshot.time.format("%a %F %H:%M:%S %z"));
        for path in snapshot.paths {
            println!("    - {path}");
        }

        println!();
    }

    Ok(())
}

#[derive(Default)]
struct ForestSizes {
    tree_bytes: u64,
    chunk_bytes: u64,
    introduced: u64,
    reused: u64,
}

impl fmt::Display for ForestSizes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        assert_eq!(
            self.tree_bytes + self.chunk_bytes,
            self.introduced + self.reused
        );
        let t = nice_size(self.tree_bytes + self.chunk_bytes);
        let m = nice_size(self.tree_bytes);
        let c = nice_size(self.chunk_bytes);
        let i = nice_size(self.introduced);
        let r = nice_size(self.reused);
        write!(
            f,
            "Sizes : {t} total ({c} files, {m} metadata / {i} new data, {r} reused)"
        )
    }
}

/// Gets the total size of the given forest.
fn forest_sizes(
    forest: &tree::Forest,
    size_map: &FxHashMap<ObjectId, u32>,
    visited_blobs: &mut FxHashSet<ObjectId>,
) -> Result<ForestSizes> {
    let mut s = ForestSizes::default();

    for t in forest.keys().map(|t| {
        size_map
            .get(t)
            .ok_or_else(|| anyhow!("Couldn't find tree {t} to get size"))
            .map(|s| (t, *s))
    }) {
        let (tree_id, tree_size) = t?;
        let ts = tree_size as u64;
        s.tree_bytes += ts;
        if visited_blobs.insert(*tree_id) {
            s.introduced += ts;
        } else {
            s.reused += ts;
        }
    }
    for tree in forest.values() {
        tree_chunks_size(tree, size_map, visited_blobs, &mut s)?;
    }
    Ok(s)
}

/// Get the size of chunks in the given tree
fn tree_chunks_size(
    tree: &tree::Tree,
    size_map: &FxHashMap<ObjectId, u32>,
    visited_blobs: &mut FxHashSet<ObjectId>,
    s: &mut ForestSizes,
) -> Result<()> {
    for node in tree.values() {
        file_size(node, size_map, visited_blobs, s)?
    }
    Ok(())
}

/// Get the size of the node if it's a file.
///
/// We've already accounted for tree sizes by summing the forest in [`forest_size`].
fn file_size(
    node: &tree::Node,
    size_map: &FxHashMap<ObjectId, u32>,
    visited_blobs: &mut FxHashSet<ObjectId>,
    s: &mut ForestSizes,
) -> Result<()> {
    use tree::NodeContents;
    match &node.contents {
        NodeContents::File { chunks, .. } => {
            for c in chunks.iter().map(|c| {
                size_map
                    .get(c)
                    .ok_or_else(|| anyhow!("Couldn't find chunk {c} to get size"))
                    .map(|s| (c, *s))
            }) {
                let (chunk_id, chunk_size) = c?;
                let cs = chunk_size as u64;
                s.chunk_bytes += cs;
                if visited_blobs.insert(*chunk_id) {
                    s.introduced += cs;
                } else {
                    s.reused += cs;
                }
            }
            Ok(())
        }
        NodeContents::Directory { .. } | NodeContents::Symlink { .. } => Ok(()),
    }
}

use std::collections::hash_map::Entry;

use anyhow::Result;
use camino::Utf8Path;
use clap::Parser;
use rustc_hash::{FxHashMap, FxHashSet};
use unicode_segmentation::UnicodeSegmentation;

use crate::{
    backend, diff,
    file_util::nice_size,
    hashing::ObjectId,
    index, ls, snapshot,
    tree::{self, meta_diff_char, Forest, ForestSizes, Node, NodeContents, NodeType},
};

/// List the snapshots in this repository from oldest to newest.
#[derive(Debug, Parser)]
pub struct Args {
    /// Print newest to oldest.
    #[clap(short, long)]
    reverse: bool,

    /// Print files added, removed, or changed by each snapshot.
    ///
    /// Essentially `backpak diff` for multiple snapshots.
    #[clap(long, verbatim_doc_comment)]
    stat: bool,

    /// Include metadata changes in --stat.
    #[clap(short, long)]
    metadata: bool,

    /// Print how much data each snapshot adds to the repository.
    ///
    /// This takes a bit longer - regardless of which snapshots are shown,
    /// we have to walk them all to see which introduced what data.
    /// (Snapshots track the data they reference, not what data is unique.)
    #[clap(short, long, verbatim_doc_comment)]
    sizes: bool,

    /// Print per-file statistics of the size each snapshot adds, largest to smallest.
    ///
    /// Implies --sizes
    #[clap(short, long)]
    file_sizes: bool,

    snapshots: Vec<String>,
}

pub fn run(repository: &camino::Utf8Path, mut args: Args) -> Result<()> {
    unsafe {
        crate::prettify::prettify_serialize();
    }
    if args.file_sizes {
        args.sizes = true;
    }

    let (_cfg, cached_backend) = backend::open(repository, backend::CacheBehavior::Normal)?;
    let snapshots = snapshot::load_chronologically(&cached_backend)?;
    let snapshots_to_print = {
        let sal = snapshot::from_args_list(&snapshots, &args.snapshots)?;
        // If the args list no snapshots, print them all.
        if sal.is_empty() {
            snapshots.clone()
        } else {
            sal
        }
    };

    if !args.sizes {
        // Simplest case: we don't have to walk history to discover when data was introduced.
        // We don't even have to load an index to see what was *in* the snapshots. EZ.
        if !args.stat {
            let it = snapshots_to_print.into_iter();
            let it: Box<dyn Iterator<Item = _>> = if args.reverse {
                Box::new(it.rev())
            } else {
                Box::new(it)
            };

            for (snap, id) in it {
                print_snapshot(&snap, &id, &None);
            }
        }
        // Slightly harder: We need an index to look at the trees in each snapshot,
        // and we also need to get the *previous* tree of each to diff it.
        // Including a null one for the first snapshot.
        else {
            let snapshots_to_print: FxHashSet<ObjectId> =
                snapshots_to_print.into_iter().map(|(_, sid)| sid).collect();

            let index = index::build_master_index(&cached_backend)?;
            let blob_map = index::blob_to_pack_map(&index)?;
            let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);

            // We could naively call forest_from_root() on each snapshot we want to print,
            // and on the previous snapshot to generate a diff. But iterating through sequential
            // snapshots in --reverse would make us call forest_from_root() *twice* on all but one.
            //
            // Instead of convoluted logic to avoid that, just make the assumption we do elsewhere
            // (e.g., the tree cache) - that trees, once built, are cheap to hang onto.
            // Just make a map of all the ones we need.
            let mut indexed_forests: FxHashMap<isize, (ObjectId, Forest)> = FxHashMap::default();
            indexed_forests.insert(-1, diff::null_forest().clone()); // If we wanna --stat the first snap

            let mut needed_indices = FxHashSet::default();
            for (i, (_snap, id)) in snapshots.iter().enumerate() {
                if snapshots_to_print.contains(id) {
                    let i = i as isize;
                    needed_indices.insert(i - 1);
                    needed_indices.insert(i);
                }
            }
            for (i, (snap, _id)) in snapshots.iter().enumerate() {
                let i = i as isize;
                if needed_indices.contains(&i) {
                    if let Entry::Vacant(e) = indexed_forests.entry(i) {
                        e.insert((
                            snap.tree,
                            tree::forest_from_root(&snap.tree, &mut tree_cache)?,
                        ));
                    }
                }
            }
            drop(needed_indices);

            let it = snapshots.iter().enumerate();
            let it: Box<dyn Iterator<Item = _>> = if args.reverse {
                Box::new(it.rev())
            } else {
                Box::new(it)
            };

            for (i, (snap, id)) in it {
                if snapshots_to_print.contains(id) {
                    let i = i as isize;
                    let (previous_root, previous_forest) = &indexed_forests[&(i - 1)];
                    let (current_root, current_forest) = &indexed_forests[&i];
                    assert_eq!(*current_root, snap.tree);
                    print_snapshot(snap, id, &None);
                    tree_diff(
                        (previous_root, previous_forest),
                        (current_root, current_forest),
                        args.metadata,
                        0, // pad
                    )?;
                    println!();
                }
            }
        }
    }
    // Hard mode: walk everything.
    else {
        let index = index::build_master_index(&cached_backend)?;
        let blob_map = index::blob_to_pack_map(&index)?;
        let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);
        let size_map = index::blob_to_size_map(&index)?;

        struct DecoratedSnapshot {
            // Goofy to put indexes in here - maybe we should use some
            // iterator with prev/next below,
            // but indexing is always the right way even if we're --reversed
            index: usize,
            snapshot: snapshot::Snapshot,
            id: ObjectId,
            forest: tree::Forest,
            sizes: Option<ForestSizes>,
        }

        let mut visited_blobs = FxHashSet::default();
        // NB: We collect at the end because our mapping is stateful;
        // we keep track of the visited blobs as we go.
        // (We do *not* want the DoubleEndedIterator from .map()!)
        let snaps = snapshots
            .into_iter()
            .enumerate()
            .map(|(index, (snapshot, id))| {
                let forest = tree::forest_from_root(&snapshot.tree, &mut tree_cache)?;
                let sizes = args
                    .sizes
                    .then(|| {
                        tree::forest_sizes(&snapshot.tree, &forest, &size_map, &mut visited_blobs)
                    })
                    .transpose()?;
                Ok(DecoratedSnapshot {
                    index,
                    snapshot,
                    id,
                    forest,
                    sizes,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let it: Box<dyn Iterator<Item = &DecoratedSnapshot>> = if !args.reverse {
            Box::new(snaps.iter())
        } else {
            Box::new(snaps.iter().rev())
        };

        let snapshots_to_print: FxHashSet<ObjectId> =
            snapshots_to_print.into_iter().map(|(_, sid)| sid).collect();

        for DecoratedSnapshot {
            index,
            snapshot,
            id,
            forest,
            sizes,
        } in it
        {
            if !snapshots_to_print.contains(id) {
                continue;
            }
            print_snapshot(snapshot, id, sizes);
            if args.stat {
                let (previous_root, previous_forest) = if *index == 0 {
                    let nf = diff::null_forest();
                    (&nf.0, &nf.1)
                } else {
                    // The whole dumb reason we carted the index around with us.
                    let prev = &snaps[*index - 1];
                    assert_eq!(prev.index, *index - 1);
                    (&prev.snapshot.tree, &prev.forest)
                };
                let (current_root, current_forest) = (&snapshot.tree, &forest);
                if args.file_sizes {
                    // Two passes to align everything
                    let pad = measure_path_pad(
                        (previous_root, previous_forest),
                        (current_root, current_forest),
                        args.metadata,
                    )?;
                    tree_diff(
                        (previous_root, previous_forest),
                        (current_root, current_forest),
                        args.metadata,
                        pad,
                    )?;
                } else {
                    tree_diff(
                        (previous_root, previous_forest),
                        (current_root, current_forest),
                        args.metadata,
                        0, // pad
                    )?;
                }
                println!();
            } else if args.file_sizes {
                // List any files that introduce new contents
                let mut fs = sizes
                    .as_ref()
                    .unwrap()
                    .per_file
                    .iter()
                    .filter(|(_, s)| s.introduced > 0)
                    .collect::<Vec<_>>();

                if !fs.is_empty() {
                    let max_path = fs
                        .iter()
                        .map(|(p, _)| p.as_str().graphemes(true).count())
                        .max()
                        .unwrap();
                    fs.sort_by_key(|(_, sizes)| sizes.introduced);
                    for (p, sizes) in fs.iter().rev() {
                        let i = nice_size(sizes.introduced);
                        let r = nice_size(sizes.reused);
                        // Don't trust a std::format!() pad
                        // https://stackoverflow.com/a/65822500
                        let plen = p.as_str().graphemes(true).count();
                        assert!(plen <= max_path);
                        let pad: String = std::iter::repeat(' ').take(max_path - plen).collect();
                        println!(" {p}{pad} | {i} new, {r} reused");
                    }
                    println!();
                }
            }
        }
    }
    Ok(())
}

fn print_snapshot(snapshot: &snapshot::Snapshot, id: &ObjectId, sizes: &Option<ForestSizes>) {
    print!("snapshot {}", id);
    if snapshot.tags.is_empty() {
        println!();
    } else {
        println!(
            " ({})",
            snapshot
                .tags
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>()
                .join(" ")
        );
    }
    if let Some(s) = sizes {
        let t = nice_size(s.tree_bytes + s.chunk_bytes);
        let m = nice_size(s.tree_bytes);
        let c = nice_size(s.chunk_bytes);
        let i = nice_size(s.introduced);
        let r = nice_size(s.reused);
        println!("Sizes: {t} total ({c} files, {m} metadata / {i} new, {r} reused)");
    }
    println!("Author: {}", snapshot.author);

    // Very similar to Git's - it's nice! - but put date, then time.
    let datestr = snapshot::strftime(&snapshot.time);
    println!("Date:   {datestr}");

    if !snapshot.paths.is_empty() {
        println!();
    }
    for path in &snapshot.paths {
        println!("  - {path}");
    }
    println!();
}

fn measure_path_pad(
    (id1, forest1): (&ObjectId, &Forest),
    (id2, forest2): (&ObjectId, &Forest),
    metadata: bool,
) -> Result<usize> {
    let mut cb = PadMeasure {
        metadata,
        longest: 0,
    };
    diff::compare_trees((id1, forest1), (id2, forest2), Utf8Path::new(""), &mut cb)?;
    Ok(cb.longest)
}

struct PadMeasure {
    metadata: bool,
    longest: usize,
}

fn path_length(path: &Utf8Path, node: &Node) -> usize {
    let mut l = path.as_str().graphemes(true).count();
    match &node.contents {
        NodeContents::Directory { .. } => {
            if !ls::has_trailing_slash(path) {
                l += 1;
            }
        }
        NodeContents::File { .. } => {}
        NodeContents::Symlink { target } => {
            l += " -> ".len();
            l += target.as_str().graphemes(true).count();
        }
    };
    l
}

fn measure_node(longest: &mut usize, path: &Utf8Path, node: &Node, should_recurse: ls::Recurse) {
    let mut v = |p: &Utf8Path, n: &Node| *longest = (*longest).max(path_length(p, n));
    ls::walk_node(&mut v, path, node, should_recurse);
}

impl diff::Callbacks for PadMeasure {
    fn node_added(&mut self, node_path: &Utf8Path, new_node: &Node, forest: &Forest) -> Result<()> {
        measure_node(
            &mut self.longest,
            node_path,
            new_node,
            ls::Recurse::Yes(forest),
        );
        Ok(())
    }

    fn node_removed(
        &mut self,
        node_path: &Utf8Path,
        old_node: &Node,
        forest: &Forest,
    ) -> Result<()> {
        measure_node(
            &mut self.longest,
            node_path,
            old_node,
            ls::Recurse::Yes(forest),
        );
        Ok(())
    }

    fn contents_changed(
        &mut self,
        node_path: &Utf8Path,
        old_node: &Node,
        new_node: &Node,
    ) -> Result<()> {
        assert!(old_node.kind() == NodeType::File || old_node.kind() == NodeType::Symlink);
        assert_eq!(old_node.kind(), new_node.kind());

        measure_node(&mut self.longest, node_path, old_node, ls::Recurse::No);
        // If it's a symlink, it might have a new target of a different length
        if old_node.kind() == NodeType::Symlink {
            measure_node(&mut self.longest, node_path, new_node, ls::Recurse::No);
        }
        Ok(())
    }

    fn metadata_changed(
        &mut self,
        node_path: &Utf8Path,
        _old_node: &Node,
        new_node: &Node,
    ) -> Result<()> {
        if self.metadata {
            measure_node(&mut self.longest, node_path, new_node, ls::Recurse::No);
        }
        Ok(())
    }
}

fn tree_diff(
    (id1, forest1): (&ObjectId, &Forest),
    (id2, forest2): (&ObjectId, &Forest),
    metadata: bool,
    pad: usize,
) -> Result<()> {
    let mut cb = PrintDiffs { metadata, pad };
    diff::compare_trees((id1, forest1), (id2, forest2), Utf8Path::new(""), &mut cb)
}

// ui::diff::PrintDiffs but with extra space in the prefixes and an optional suffix
struct PrintDiffs {
    metadata: bool,
    pad: usize,
}

fn printer(pad: usize, prefix: &str, path: &Utf8Path, node: &Node) {
    print!("{prefix}");
    let mut p = path.as_str().to_owned();
    match &node.contents {
        NodeContents::Directory { .. } => {
            if !ls::has_trailing_slash(path) {
                p.push(std::path::MAIN_SEPARATOR);
            }
        }
        NodeContents::File { .. } => {}
        NodeContents::Symlink { target } => {
            p += &format!(" -> {target}");
        }
    };
    // Sentinel values are bad, mmmk.
    // But also, if we haven't measured the longest path length in a previous pass,
    // we're not printing file sizes.
    if pad == 0 {
        println!("{p}");
    } else {
        // Don't trust a std::format!() pad
        // https://stackoverflow.com/a/65822500
        let plen = p.graphemes(true).count();
        assert!(plen <= pad);
        let pad: String = std::iter::repeat(' ').take(pad - plen).collect();
        println!("{p}{pad} | SIZE MAYBE HERE IF --stat --file-sizes");
    }
}

fn print_node(pad: usize, prefix: &str, path: &Utf8Path, node: &Node, should_recurse: ls::Recurse) {
    let mut v = |p: &Utf8Path, n: &Node| printer(pad, prefix, p, n);
    ls::walk_node(&mut v, path, node, should_recurse);
}

impl diff::Callbacks for PrintDiffs {
    fn node_added(&mut self, node_path: &Utf8Path, new_node: &Node, forest: &Forest) -> Result<()> {
        print_node(
            self.pad,
            " + ",
            node_path,
            new_node,
            ls::Recurse::Yes(forest),
        );
        Ok(())
    }

    fn node_removed(
        &mut self,
        node_path: &Utf8Path,
        old_node: &Node,
        forest: &Forest,
    ) -> Result<()> {
        print_node(
            self.pad,
            " - ",
            node_path,
            old_node,
            ls::Recurse::Yes(forest),
        );
        Ok(())
    }

    fn contents_changed(
        &mut self,
        node_path: &Utf8Path,
        old_node: &Node,
        new_node: &Node,
    ) -> Result<()> {
        assert!(old_node.kind() == NodeType::File || old_node.kind() == NodeType::Symlink);
        assert_eq!(old_node.kind(), new_node.kind());

        if old_node.kind() == NodeType::Symlink {
            print_node(self.pad, " - ", node_path, old_node, ls::Recurse::No);
            print_node(self.pad, " + ", node_path, new_node, ls::Recurse::No);
        } else {
            print_node(self.pad, " C ", node_path, old_node, ls::Recurse::No);
        }
        Ok(())
    }

    fn metadata_changed(
        &mut self,
        node_path: &Utf8Path,
        old_node: &Node,
        new_node: &Node,
    ) -> Result<()> {
        if self.metadata {
            let leading_char = format!(
                " {} ",
                meta_diff_char(&old_node.metadata, &new_node.metadata).unwrap()
            );
            print_node(
                self.pad,
                &leading_char,
                node_path,
                new_node,
                ls::Recurse::No,
            );
        }
        Ok(())
    }
}

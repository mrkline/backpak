use anyhow::{bail, Result};
use camino::Utf8Path;
use clap::Parser;
use rustc_hash::{FxHashMap, FxHashSet};
use unicode_segmentation::UnicodeSegmentation;

use crate::{
    backend, diff,
    file_util::nice_size,
    hashing::ObjectId,
    index, ls, snapshot,
    tree::{self, meta_diff_char, Forest, ForestSizes, Node, NodeType},
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
                print_snapshot(&snap, &id, None, false);
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
            indexed_forests.insert(-1, diff::null_forest()); // If we wanna --stat the first snap

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
                    // Would be nice to use .entry().or_insert_with() but it's not fallable.
                    if !indexed_forests.contains_key(&i) {
                        assert!(indexed_forests
                            .insert(
                                i,
                                (
                                    snap.tree,
                                    tree::forest_from_root(&snap.tree, &mut tree_cache)?,
                                )
                            )
                            .is_none());
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
                    print_snapshot(&snap, &id, None, false);
                    tree_diff(
                        (&previous_root, &previous_forest),
                        (&current_root, &current_forest),
                        args.metadata,
                    )?;
                    println!();
                }
            }
        }
    }
    // Hard mode: walk everything.
    else {
        if args.stat {
            // Merge the above and the below?
            bail!("--sizes and --stat are not supported together yet, sorry.")
        }

        let index = index::build_master_index(&cached_backend)?;
        let blob_map = index::blob_to_pack_map(&index)?;
        let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);
        let size_map = index::blob_to_size_map(&index)?;

        struct DecoratedSnapshot {
            snapshot: snapshot::Snapshot,
            id: ObjectId,
            sizes: Option<ForestSizes>,
        }

        let mut visited_blobs = FxHashSet::default();
        // NB: We collect at the end because our mapping is stateful;
        // we keep track of the visited blobs as we go.
        // (We do *not* want the DoubleEndedIterator from .map()!)
        let snaps = snapshots
            .into_iter()
            .map(|(snapshot, id)| {
                let sizes = args
                    .sizes
                    .then(|| {
                        tree::forest_sizes(
                            &snapshot.tree,
                            &tree::forest_from_root(&snapshot.tree, &mut tree_cache)?,
                            &size_map,
                            &mut visited_blobs,
                        )
                    })
                    .transpose()?;
                Ok(DecoratedSnapshot {
                    snapshot,
                    id,
                    sizes,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let it: Box<dyn Iterator<Item = DecoratedSnapshot>> = if !args.reverse {
            Box::new(snaps.into_iter())
        } else {
            Box::new(snaps.into_iter().rev())
        };

        let snapshots_to_print: FxHashSet<ObjectId> =
            snapshots_to_print.into_iter().map(|(_, sid)| sid).collect();

        for DecoratedSnapshot {
            snapshot,
            id,
            sizes,
        } in it
        {
            if !snapshots_to_print.contains(&id) {
                continue;
            }
            print_snapshot(&snapshot, &id, sizes, args.file_sizes);
        }
    }
    Ok(())
}

fn print_snapshot(
    snapshot: &snapshot::Snapshot,
    id: &ObjectId,
    sizes: Option<ForestSizes>,
    per_file: bool,
) {
    assert!(sizes.is_some() || !per_file);
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
    if let Some(s) = &sizes {
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

    if per_file {
        // List any files that introduce new contents
        let mut fs = sizes
            .unwrap()
            .per_file
            .into_iter()
            .filter(|(_, s)| s.introduced > 0)
            .collect::<Vec<_>>();

        if !fs.is_empty() {
            println!();
            let max_path = fs
                .iter()
                .map(|(p, _)| p.as_str().graphemes(true).count())
                .max()
                .unwrap();
            fs.sort_by_key(|(_, sizes)| sizes.introduced);
            for (p, sizes) in fs.iter().rev() {
                let i = nice_size(sizes.introduced);
                let r = nice_size(sizes.reused);
                println!(" {p:max_path$} | {i} new, {r} reused");
            }
        }
    }

    println!();
}

fn tree_diff(
    (id1, forest1): (&ObjectId, &Forest),
    (id2, forest2): (&ObjectId, &Forest),
    metadata: bool,
) -> Result<()> {
    let mut cb = PrintDiffs { metadata };

    diff::compare_trees(
        (&id1, &forest1),
        (&id2, &forest2),
        Utf8Path::new(""),
        &mut cb,
    )
}

// ui::diff::PrintDiffs but with extra space in the prefixes
struct PrintDiffs {
    metadata: bool,
}

impl diff::Callbacks for PrintDiffs {
    fn node_added(&mut self, node_path: &Utf8Path, new_node: &Node, forest: &Forest) -> Result<()> {
        ls::print_node(" + ", node_path, new_node, ls::Recurse::Yes(forest));
        Ok(())
    }

    fn node_removed(
        &mut self,
        node_path: &Utf8Path,
        old_node: &Node,
        forest: &Forest,
    ) -> Result<()> {
        ls::print_node(" - ", node_path, old_node, ls::Recurse::Yes(forest));
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
            ls::print_node(" - ", node_path, old_node, ls::Recurse::No);
            ls::print_node(" + ", node_path, new_node, ls::Recurse::No);
        } else {
            ls::print_node(" C ", node_path, old_node, ls::Recurse::No);
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
            ls::print_node(&leading_char, node_path, new_node, ls::Recurse::No);
        }
        Ok(())
    }
}

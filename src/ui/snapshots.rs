use anyhow::Result;
use clap::Parser;
use rustc_hash::FxHashSet;
use unicode_segmentation::UnicodeSegmentation;

use crate::{backend, file_util::nice_size, hashing::ObjectId, index, snapshot, tree};

/// List the snapshots in this repository from oldest to newest
#[derive(Debug, Parser)]
pub struct Args {
    /// Print newest to oldest
    #[clap(short, long)]
    reverse: bool,

    /// Print usage statistics of each snapshot
    ///
    /// This takes a bit longer - regardless of which snapshots are shown,
    /// we have to walk them all to see which introduced what files.
    /// (Snapshots track the data they reference, not what data is unique.)
    #[clap(short, long, verbatim_doc_comment)]
    sizes: bool,

    /// Print per-file statistics of size added to each snapshot
    ///
    /// Implies --sizes
    #[clap(short, long)]
    files: bool,

    snapshots: Vec<String>,
}

pub fn run(repository: &camino::Utf8Path, mut args: Args) -> Result<()> {
    unsafe {
        crate::prettify::prettify_serialize();
    }
    if args.files {
        args.sizes = true;
    }

    let (_cfg, cached_backend) = backend::open(repository, backend::CacheBehavior::Normal)?;
    let snapshots = snapshot::load_chronologically(&cached_backend)?;
    let snapshots_to_print = snapshot::from_args_list(&snapshots, &args.snapshots)?;

    // If we're not getting sizes, we don't have to walk the entire history to figure out
    // when data was introduced. EZ.
    if !args.sizes {
        let it = if snapshots_to_print.is_empty() {
            snapshots.into_iter()
        } else {
            snapshots_to_print.into_iter()
        };
        let it: Box<dyn Iterator<Item = _>> = if args.reverse {
            Box::new(it.rev())
        } else {
            Box::new(it)
        };

        for (snap, id) in it {
            print_snapshot(snap, &id, None, false);
        }
    }
    // Hard mode: walk everything.
    else {
        let index = index::build_master_index(&cached_backend)?;
        let blob_map = index::blob_to_pack_map(&index)?;
        let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);
        let size_map = index::blob_to_size_map(&index)?;

        struct DecoratedSnapshot {
            snapshot: snapshot::Snapshot,
            id: ObjectId,
            sizes: Option<tree::ForestSizes>,
        }

        let mut visited_blobs = FxHashSet::default();
        // NB: We collect at the end because our mapping is stateful;
        // we keep track of the visited blobs as we go.
        // (We do *not* want the DoubleEndedIterator from Map!)
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

        for DecoratedSnapshot {
            snapshot,
            id,
            sizes,
        } in it
        {
            if !snapshots_to_print.is_empty()
                && !snapshots_to_print.iter().any(|(_, sid)| *sid == id)
            {
                continue;
            }
            print_snapshot(snapshot, &id, sizes, args.files);
        }
    }
    Ok(())
}

fn print_snapshot(
    snapshot: snapshot::Snapshot,
    id: &ObjectId,
    sizes: Option<tree::ForestSizes>,
    per_file: bool,
) {
    assert!(sizes.is_some() || !per_file);
    print!("snapshot {}", id);
    if snapshot.tags.is_empty() {
        println!();
    } else {
        println!(
            " ({})",
            snapshot.tags.into_iter().collect::<Vec<String>>().join(" ")
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
    for path in snapshot.paths {
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

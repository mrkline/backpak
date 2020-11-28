use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::*;
use log::*;
use rayon::prelude::*;
use structopt::StructOpt;

use crate::backend;
use crate::hashing::ObjectId;
use crate::index;
use crate::pack;
use crate::snapshot;
use crate::tree;

/// Check the repository for errors
///
/// By default this assumes file integrity of the backup,
/// and only ensure that needed files can be found and downloaded.
/// If --read-packs is specified, ensure that each pack has the expected blobs,
/// that those blobs match its manifest, and that those blobs match the index.
#[derive(Debug, StructOpt)]
#[structopt(verbatim_doc_comment)]
pub struct Args {
    /// Check all blobs in all packs
    #[structopt(short, long)]
    pub read_packs: bool,
}

pub fn run(repository: &Path, args: Args) -> Result<()> {
    info!("Checking {}", repository.display());
    let cached_backend = backend::open(repository)?;

    info!("Checking indexes");
    let index = index::build_master_index(&cached_backend)?;

    info!("Checking packs listed in indexes");
    let borked = AtomicUsize::new(0);
    index.packs.par_iter().for_each(|(pack_id, manifest)| {
        if let Err(e) = check_pack(&cached_backend, pack_id, manifest, args.read_packs) {
            error!("Problem with pack {}: {:?}", pack_id, e);
            borked.fetch_add(1, Ordering::Relaxed);
        }
    });
    let borked = borked.load(Ordering::SeqCst);

    info!("Checking snapshots");
    let blob_map = index::blob_to_pack_map(&index)?;
    cached_backend
        .backend
        .list_snapshots()?
        .par_iter()
        .try_for_each::<_, Result<()>>(|snapshot_path| {
            debug!("Checking {}", snapshot_path);
            let mut snapshot_file = cached_backend
                .read(snapshot_path)
                .with_context(|| format!("Couldn't read snapshot {}", snapshot_path))?;
            let snapshot = snapshot::from_reader(&mut snapshot_file)?;

            // Give each thread its own tree cache.
            // There will probably be plenty of overlap,
            // but it beats reading all the snapshots serially.
            let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);
            let snapshot_tree = tree::forest_from_root(&snapshot.tree, &mut tree_cache)?;

            debug!("Checking all file chunks in tree {}", snapshot.tree);
            // Cool, we've assembled all the trees.
            // Let's check that all the chunks are reachable.
            // TODO: Holy nesting, Batman. Refactor? Parallelize some more?
            for tree in snapshot_tree.values() {
                for (path, node) in tree.iter() {
                    match &node.contents {
                        tree::NodeContents::Directory { .. } => {}
                        tree::NodeContents::File { chunks, .. } => {
                            for chunk in chunks {
                                if !blob_map.contains_key(chunk) {
                                    error!(
                                        "File chunk {} (of {}) isn't reachable",
                                        chunk,
                                        path.display()
                                    );
                                } else {
                                    trace!("Chunk {} (of {}) is reachable", chunk, path.display());
                                }
                            }
                        }
                    };
                }
            }
            Ok(())
        })?;

    if borked == 0 {
        Ok(())
    } else {
        bail!("{} broken packs", borked);
    }
}

#[inline]
fn check_pack(
    cached_backend: &backend::CachedBackend,
    pack_id: &ObjectId,
    manifest: &[pack::PackManifestEntry],
    read_packs: bool,
) -> Result<()> {
    if read_packs {
        let mut pack = cached_backend.read_pack(pack_id)?;
        pack::verify(&mut pack, manifest)?;
        trace!("Pack {} verified", pack_id);
    } else {
        cached_backend.backend.probe_pack(pack_id)?;
        trace!("Pack {} found", pack_id);
    }
    Ok(())
}

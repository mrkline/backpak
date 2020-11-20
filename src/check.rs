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

#[derive(Debug, StructOpt)]
pub struct Args {
    #[structopt(short, long)]
    pub check_packs: bool,
}

pub fn run(repository: &Path, args: Args) -> Result<()> {
    let cached_backend = backend::open(repository)?;
    let index = index::build_master_index(&cached_backend)?;

    // Load up all snapshots and note their trees - we'll use them as roots
    // in our search for all connected objects.

    // TODO: Verify snapshots. Just ensure their tree is a valid one?

    let borked = AtomicUsize::new(0);
    index.packs.par_iter().for_each(|(pack_id, manifest)| {
        if let Err(e) = check_pack(&cached_backend, pack_id, manifest, args.check_packs) {
            error!("Problem with pack {}: {:?}", pack_id, e);
            borked.fetch_add(1, Ordering::Relaxed);
        }
    });
    let borked = borked.load(Ordering::SeqCst);
    if borked > 0 {
        Err(anyhow!("{} broken packs", borked))
    } else {
        Ok(())
    }
}

#[inline]
fn check_pack(
    cached_backend: &backend::CachedBackend,
    pack_id: &ObjectId,
    manifest: &[pack::PackManifestEntry],
    check_packs: bool,
) -> Result<()> {
    let mut pack = cached_backend.read_pack(pack_id)?;
    if check_packs {
        pack::verify(&mut pack, manifest)?;
        trace!("Pack {} successfully verified", pack_id);
    } else {
        trace!("Pack {} found and successfully opened", pack_id);
    }
    Ok(())
}

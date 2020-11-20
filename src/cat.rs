use std::io;
use std::io::prelude::*;
use std::path::Path;

use anyhow::*;
use log::*;
use structopt::StructOpt;

use crate::backend;
use crate::hashing::ObjectId;
use crate::index;
use crate::pack;
use crate::snapshot;
use crate::tree;

#[derive(Debug, StructOpt)]
pub struct Args {
    #[structopt(subcommand)]
    subcommand: Subcommand,
}

#[derive(Debug, StructOpt)]
pub enum Subcommand {
    Blob { id: ObjectId },
    Pack { id: ObjectId },
    Index { id: ObjectId },
    Snapshot { id: ObjectId },
}

pub fn run(repository: &Path, args: Args) -> Result<()> {
    unsafe {
        crate::prettify::prettify_serialize();
    }

    let cached_backend = backend::open(repository)?;

    match args.subcommand {
        Subcommand::Blob { id } => {
            let index = index::build_master_index(&cached_backend)?;
            let blob_map = index::blob_to_pack_map(&index)?;
            let containing_pack_id = blob_map
                .get(&id)
                .ok_or_else(|| anyhow!("Can't find blob {} in the index", id))?;
            info!("Blob {} found in pack {}", id, containing_pack_id);
            let index_manifest = index.packs.get(containing_pack_id).unwrap();

            let mut reader = cached_backend.read_pack(&containing_pack_id)?;

            let (manifest_entry, blob) = pack::extract_blob(&mut reader, &id, &index_manifest)?;

            debug_assert!(manifest_entry.id == id);
            assert!(!blob.is_empty());
            match manifest_entry.blob_type {
                pack::BlobType::Chunk => io::stdout().write_all(&blob)?,
                pack::BlobType::Tree => {
                    let tree: tree::Tree = serde_cbor::from_slice(&blob)
                        .with_context(|| format!("CBOR decoding of tree {} failed", id))?;
                    serde_json::to_writer(io::stdout(), &tree)?;
                }
            }
        }
        Subcommand::Pack { id } => {
            let mut pack = cached_backend.read_pack(&id)?;
            pack::check_magic(&mut pack)?;
            let manifest = pack::manifest_from_reader(&mut pack)?;
            serde_json::to_writer(io::stdout(), &manifest)?;
        }
        Subcommand::Index { id } => {
            let index = index::from_reader(&mut cached_backend.read_index(&id)?)?;
            serde_json::to_writer(io::stdout(), &index)?;
        }
        Subcommand::Snapshot { id } => {
            let snapshot = snapshot::from_reader(&mut cached_backend.read_snapshot(&id)?)?;
            serde_json::to_writer(io::stdout(), &snapshot)?;
        }
    }
    Ok(())
}

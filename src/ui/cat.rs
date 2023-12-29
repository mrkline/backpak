use std::io;
use std::io::prelude::*;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use log::*;

use crate::backend;
use crate::blob;
use crate::hashing::ObjectId;
use crate::index;
use crate::pack;
use crate::snapshot;
use crate::tree;

/// Print objects (as JSON) to stdout
#[derive(Debug, Parser)]
pub struct Args {
    #[clap(subcommand)]
    subcommand: Subcommand,
}

#[derive(Debug, Parser)]
pub enum Subcommand {
    /// Print the blob with the given ID
    ///
    /// A blob is either a chunk (of a file) or a tree (representing a directory).
    #[clap(verbatim_doc_comment)]
    Blob { id: ObjectId },

    /// Print the pack with the given ID
    ///
    /// A pack is a compressed collection of blobs,
    /// with a manifest at the end for reassembling the index (if needed).
    #[clap(verbatim_doc_comment)]
    Pack { id: ObjectId },

    /// Print the index with the given ID
    ///
    /// An index tells us which packs contain which blobs.
    /// Each backup stores a new index.
    /// They can be coalesced with `rebuild-index`
    #[clap(verbatim_doc_comment)]
    Index { id: ObjectId },

    /// Print the snapshot with the given ID
    ///
    /// A snapshot records the time of the backup,
    /// the contents of all files and folders at that time,
    /// and (optionally) an author and tags for later lookup.
    #[clap(verbatim_doc_comment)]
    Snapshot { id_prefix: String },
}

pub fn run(repository: &camino::Utf8Path, args: Args) -> Result<()> {
    unsafe {
        crate::prettify::prettify_serialize();
    }

    let (_cfg, cached_backend) = backend::open(repository)?;

    match args.subcommand {
        Subcommand::Blob { id } => {
            let index = index::build_master_index(&cached_backend)?;
            let blob_map = index::blob_to_pack_map(&index)?;
            let containing_pack_id = blob_map
                .get(&id)
                .ok_or_else(|| anyhow!("Can't find blob {} in the index", id))?;
            info!("Blob {} found in pack {}", id, containing_pack_id);
            let index_manifest = index.packs.get(containing_pack_id).unwrap();

            let mut reader = cached_backend.read_pack(containing_pack_id)?;

            let (manifest_entry, blob) = pack::extract_blob(&mut reader, &id, index_manifest)?;

            debug_assert!(manifest_entry.id == id);
            assert!(!blob.is_empty());
            match manifest_entry.blob_type {
                blob::Type::Chunk => io::stdout().write_all(&blob)?,
                blob::Type::Tree => {
                    let tree: tree::Tree = ciborium::from_reader(&*blob)
                        .with_context(|| format!("CBOR decoding of tree {} failed", id))?;
                    serde_json::to_writer(io::stdout(), &tree)?;
                }
            }
        }
        Subcommand::Pack { id } => {
            let manifest = pack::load_manifest(&id, &cached_backend)?;
            serde_json::to_writer(io::stdout(), &manifest)?;
        }
        Subcommand::Index { id } => {
            let index = index::load(&id, &cached_backend)?;
            serde_json::to_writer(io::stdout(), &index)?;
        }
        Subcommand::Snapshot { id_prefix } => {
            let (snapshot, _id) = snapshot::find_and_load(&id_prefix, &cached_backend)?;
            serde_json::to_writer(io::stdout(), &snapshot)?;
        }
    }
    Ok(())
}

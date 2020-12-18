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

/// Print objects (as JSON) to stdout
#[derive(Debug, StructOpt)]
pub struct Args {
    #[structopt(subcommand)]
    subcommand: Subcommand,
}

#[derive(Debug, StructOpt)]
pub enum Subcommand {
    /// Print the blob with the given ID
    ///
    /// A blob is either a chunk (of a file) or a tree (representing a directory).
    #[structopt(verbatim_doc_comment)]
    Blob { id: ObjectId },

    /// Print the pack with the given ID
    ///
    /// A pack is a compressed collection of blobs,
    /// with a manifest at the end for reassembling the index (if needed).
    #[structopt(verbatim_doc_comment)]
    Pack { id: ObjectId },

    /// Print the index with the given ID
    ///
    /// An index tells us which packs contain which blobs.
    /// Indexes can be split into several files if they get too big.
    #[structopt(verbatim_doc_comment)]
    Index { id: ObjectId },

    /// Print the snapshot with the given ID
    ///
    /// A snapshot records the time of the backup,
    /// the contents of all files and folders at that time,
    /// and (optionally) an author and tags for later lookup.
    #[structopt(verbatim_doc_comment)]
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
            let manifest = pack::load_manifest(&id, &cached_backend)?;
            serde_json::to_writer(io::stdout(), &manifest)?;
        }
        Subcommand::Index { id } => {
            let index = index::load(&id, &cached_backend)?;
            serde_json::to_writer(io::stdout(), &index)?;
        }
        Subcommand::Snapshot { id } => {
            let snapshot = snapshot::load(&id, &cached_backend)?;
            serde_json::to_writer(io::stdout(), &snapshot)?;
        }
    }
    Ok(())
}

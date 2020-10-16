use anyhow::*;
use log::*;
use structopt::StructOpt;

use crate::backend;
use crate::hashing::ObjectId;
use crate::index;
use crate::pack;

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
}

pub fn run(repository: &str, args: Args) -> Result<()> {
    unsafe {
        crate::hashing::hexify_ids();
    } // Shame.

    let backend = backend::open(repository)?;

    match args.subcommand {
        Subcommand::Blob { id } => {
            let index = index::build_master_index(&*backend)?;
            let blob_map = index::blob_to_pack_map(&index)?;
            let containing_pack_id = blob_map
                .get(&id)
                .ok_or_else(|| anyhow!("Can't find blob {} in the index", id))?;
            info!("Blob {} found in pack {}", id, containing_pack_id);
            let index_manifest = index.packs.get(containing_pack_id).unwrap();
            pack::PackfileReader::new(backend.read_pack(&containing_pack_id)?, index_manifest)
                .with_context(|| format!("Couldn't open pack {}", id))?;
        }
        Subcommand::Pack { id } => {
            let manifest = pack::manifest_from_reader(&mut backend.read_pack(&id)?)?;
            serde_json::to_writer(std::io::stdout(), &manifest)?;
        }
        Subcommand::Index { id } => {
            let index = index::from_reader(&mut backend.read_index(&id)?)?;
            serde_json::to_writer(std::io::stdout(), &index)?;
        }
    }
    Ok(())
}

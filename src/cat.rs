use std::path::PathBuf;

use anyhow::Result;
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
    Pack { filename: PathBuf }, // TODO: ID! (once we have indexing)
    Index { id: ObjectId },
}

pub fn run(repository: &str, args: Args) -> Result<()> {
    unsafe {
        crate::hashing::hexify_ids();
    } // Shame.

    match args.subcommand {
        Subcommand::Pack { filename } => {
            let manifest = pack::manifest_from_file(&filename)?;
            serde_json::to_writer(std::io::stdout(), &manifest)?;
        }
        Subcommand::Index { id } => {
            let mut backend = backend::open(repository)?;
            let index = index::from_reader(&mut backend.read_index(id)?)?;
            serde_json::to_writer(std::io::stdout(), &index)?;
        }
    }
    Ok(())
}

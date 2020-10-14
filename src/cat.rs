use std::path::PathBuf;

use anyhow::Result;
use structopt::StructOpt;

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
    Index { filename: PathBuf }, // TODO: ID!
}

pub fn run(args: Args) -> Result<()> {
    unsafe {
        crate::hashing::hexify_ids();
    } // Shame.

    match args.subcommand {
        Subcommand::Pack { filename } => {
            let manifest = pack::manifest_from_file(&filename)?;
            serde_json::to_writer(std::io::stdout(), &manifest)?;
        },
        Subcommand::Index { filename } => {
            let index = index::from_file(&filename)?;
            serde_json::to_writer(std::io::stdout(), &index)?;
        }
    }
    Ok(())
}

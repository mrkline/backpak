use std::path::PathBuf;

use anyhow::Result;
use structopt::StructOpt;

use crate::pack::read_packfile_manifest;

#[derive(Debug, StructOpt)]
pub struct Args {
    #[structopt(subcommand)]
    subcommand: Subcommand,
}

#[derive(Debug, StructOpt)]
pub enum Subcommand {
    Pack { filename: PathBuf }, // TODO: ID! (once we have indexing)
}

pub fn run(args: Args) -> Result<()> {
    unsafe {
        crate::hashing::hexify_ids();
    } // Shame.

    match args.subcommand {
        Subcommand::Pack { filename } => {
            let manifest = read_packfile_manifest(&filename)?;
            serde_json::to_writer(std::io::stdout(), &manifest)?;
        }
    }
    Ok(())
}

use anyhow::Result;
use clap::{Parser, Subcommand};

use crate::backend;

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(short, long, default_value_t = crate::pack::DEFAULT_PACK_SIZE)]
    pack_size: u64,

    #[clap(long)]
    gpg: Option<String>,

    #[clap(subcommand)]
    subcommand: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Filesystem,
    Backblaze {
        #[clap(short, long)]
        key_id: String,
        #[clap(short, long)]
        application_key: String,
        #[clap(short, long)]
        bucket: String,
    },
}

pub fn run(repository: &camino::Utf8Path, args: Args) -> Result<()> {
    let (filter, unfilter) = match args.gpg {
        Some(g) => (
            Some("gpg --encrypt --recipient ".to_owned() + &g),
            Some("gpg --decrypt".to_owned()),
        ),
        None => (None, None),
    };
    match args.subcommand {
        Command::Filesystem => {
            backend::fs::initialize(repository, args.pack_size, filter, unfilter)
        }
        Command::Backblaze {
            key_id,
            application_key,
            bucket,
        } => backend::backblaze::initialize(
            repository,
            args.pack_size,
            key_id,
            application_key,
            bucket,
            filter,
            unfilter,
        ),
    }
}

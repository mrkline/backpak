use std::path::Path;

use anyhow::*;
use log::*;
use structopt::StructOpt;

use crate::backend;
use crate::hashing::ObjectId;

#[derive(Debug, StructOpt)]
pub struct Args {
    #[structopt(short = "n", long)]
    pub dry_run: bool,

    // TODO: Abbreviation matching a la git
    // (useful for all snapshots. Maybe other stuff too...)
    #[structopt(required = true)]
    to_forget: Vec<ObjectId>,
}

pub fn run(repository: &Path, args: Args) -> Result<()> {
    unsafe {
        crate::prettify::prettify_serialize();
    }

    assert!(!args.to_forget.is_empty());

    let mut cached_backend = backend::open(repository)?;

    let mut failure = false;

    for snapshot in &args.to_forget {
        if args.dry_run {
            if let Err(e) = cached_backend.probe_snapshot(snapshot) {
                error!("{:?}", e);
                failure = true;
            } else {
                info!("Would remove {}", snapshot);
            }
        } else if let Err(e) = cached_backend.remove_snapshot(snapshot) {
            error!("{:?}", e);
            failure = true;
        } else {
            info!("Removed snapshot {}", snapshot);
        }
    }

    if failure {
        bail!("Couldn't forget snapshots!");
    } else {
        Ok(())
    }
}
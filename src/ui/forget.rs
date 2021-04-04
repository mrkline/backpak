use std::path::Path;

use anyhow::*;
use log::*;
use structopt::StructOpt;

use crate::backend;

/// Forget snapshots
///
/// Data used by these snapshots is not immediately deleted,
/// but will be thrown out by the next prune.
#[derive(Debug, StructOpt)]
#[structopt(verbatim_doc_comment)]
pub struct Args {
    #[structopt(short = "n", long)]
    pub dry_run: bool,

    #[structopt(required = true)]
    to_forget: Vec<String>,
}

pub fn run(repository: &Path, args: Args) -> Result<()> {
    unsafe {
        crate::prettify::prettify_serialize();
    }

    assert!(!args.to_forget.is_empty());

    let cached_backend = backend::open(repository)?;

    let mut failure = false;

    for id_prefix in &args.to_forget {
        let id = match crate::snapshot::find(id_prefix, &cached_backend) {
            Ok(id) => id,
            Err(e) => {
                error!("{:?}", e);
                failure = true;
                continue;
            }
        };

        if args.dry_run {
            info!("Would remove {}", id);
            continue;
        }

        match cached_backend.remove_snapshot(&id) {
            Ok(()) => info!("Removed snapshot {}", id),
            Err(e) => {
                error!("{:?}", e);
                failure = true;
            }
        }
    }

    if failure {
        bail!("Couldn't forget snapshots!");
    } else {
        Ok(())
    }
}

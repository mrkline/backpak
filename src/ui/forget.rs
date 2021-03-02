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
        let snapshot_path = match cached_backend.find_snapshot(id_prefix) {
            Ok(path) => path,
            Err(e) => {
                error!("{:?}", e);
                failure = true;
                continue;
            }
        };
        let id = backend::id_from_path(&snapshot_path).unwrap();

        if args.dry_run {
            info!("Would remove {}", id);
            continue;
        }

        match cached_backend.remove(&snapshot_path) {
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

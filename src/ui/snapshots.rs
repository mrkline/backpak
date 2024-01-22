use anyhow::Result;
use clap::Parser;

use crate::{backend, hashing, snapshot};

/// List the snapshots in this repository from oldest to newest
#[derive(Debug, Parser)]
pub struct Args {
    /// Print newest to oldest
    #[clap(short, long)]
    reverse: bool,
}

pub fn run(repository: &camino::Utf8Path, args: Args) -> Result<()> {
    unsafe {
        crate::prettify::prettify_serialize();
    }

    let (_cfg, cached_backend) = backend::open(repository, backend::CacheBehavior::Normal)?;
    let snapshots = snapshot::load_chronologically(&cached_backend)?;

    let it: Box<dyn Iterator<Item = (snapshot::Snapshot, hashing::ObjectId)>> = if !args.reverse {
        Box::new(snapshots.into_iter())
    } else {
        Box::new(snapshots.into_iter().rev())
    };

    for (snapshot, id) in it {
        print!("snapshot {}", id);
        if snapshot.tags.is_empty() {
            println!();
        } else {
            println!(
                " ({})",
                snapshot.tags.into_iter().collect::<Vec<String>>().join(" ")
            );
        }
        println!("Author: {}", snapshot.author);
        println!("Date:   {}", snapshot.time.format("%a %F %H:%M:%S %z"));
        for path in snapshot.paths {
            println!("    - {path}");
        }

        println!();
    }

    Ok(())
}

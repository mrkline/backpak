use std::path::Path;

use anyhow::*;
use log::*;

use crate::backend;
use crate::snapshot;

pub fn run(repository: &Path) -> Result<()> {
    unsafe {
        crate::prettify::prettify_serialize();
    }

    info!("Opening repository '{}'", repository.display());
    let cached_backend = backend::open(repository)?;
    info!("Reading snapshots");
    let snapshots = snapshot::load_chronologically(&cached_backend)?;

    for (snapshot, id) in snapshots {
        print!("snapshot {}", id);
        if snapshot.tags.is_empty() {
            println!();
        } else {
            println!("({})", snapshot.tags.into_iter().collect::<Vec<String>>().join(" "));
        }
        println!("Author: {}", snapshot.author);
        println!("Date:   {}", snapshot.time.format("%a %F %H:%M:%S %z"));
        for path in snapshot.paths {
            println!("    - {}", path.display());
        }

        println!();
    }

    Ok(())
}

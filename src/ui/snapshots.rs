use anyhow::Result;

use crate::backend;
use crate::snapshot;

pub fn run(repository: &camino::Utf8Path) -> Result<()> {
    unsafe {
        crate::prettify::prettify_serialize();
    }

    let cached_backend = backend::open(repository)?;
    let snapshots = snapshot::load_chronologically(&cached_backend)?;

    for (snapshot, id) in snapshots.into_iter().rev() {
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

use std::env::set_current_dir;
use std::fs::*;
use std::path::Path;

use anyhow::*;

pub fn run(repository: &Path) -> Result<()> {
    ensure!(
        !repository.exists(),
        "The directory {} already exists",
        repository.display()
    );

    create_dir(repository).with_context(|| format!("Couldn't create {}", repository.display()))?;
    set_current_dir(repository)?;
    create_dir("packs")?;
    for b in 0..=255 {
        create_dir()?;
    }

    create_dir("index")?;

    Ok(())
}

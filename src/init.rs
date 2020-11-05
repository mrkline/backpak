use anyhow::*;

use crate::backend;

pub fn run(repository: &std::path::Path) -> Result<()> {
    backend::initialize(repository)
}

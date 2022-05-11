use anyhow::*;

use crate::backend;

pub fn run(repository: &camino::Utf8Path) -> Result<()> {
    backend::initialize(repository)
}

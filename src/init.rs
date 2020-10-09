use anyhow::*;

use crate::backend;

pub fn run(repository: &str) -> Result<()> {
    backend::initialize(repository)
}

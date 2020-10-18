use anyhow::*;
use structopt::StructOpt;

use crate::backend;
use crate::index;
use crate::pack;

#[derive(Debug, StructOpt)]
pub struct Args {
    #[structopt(short, long)]
    check_data: bool,
}

pub fn run(repository: &str, _args: Args) -> Result<()> {
    let backend = backend::open(repository)?;

    let index = index::build_master_index(&*backend)?;
    for (pack, manifest) in &index.packs {
        let mut pack = backend.read_pack(pack)?;
        pack::verify(&mut pack, manifest)?;
    }
    Ok(())
}

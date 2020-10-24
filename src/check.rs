use anyhow::*;
use log::*;
use rayon::prelude::*;
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
    index
        .packs
        .par_iter()
        .try_for_each_with::<_, _, anyhow::Result<()>>(
            &backend,
            |backend, (pack_id, manifest)| {
                let mut pack = backend.read_pack(pack_id)?;
                if let Err(e) = pack::verify(&mut pack, manifest) {
                    error!("Problem with pack {}: {:?}", pack_id, e);
                }
                Ok(())
            },
        )?;
    Ok(())
}

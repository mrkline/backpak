use std::{
    io::prelude::*,
    process::{self, Stdio},
};

use anyhow::{bail, ensure, Context, Result};
use byte_unit::Byte;
use clap::{Parser, Subcommand};

use crate::backend;

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(short, long, default_value_t = crate::pack::DEFAULT_PACK_SIZE)]
    pack_size: Byte,

    #[clap(long)]
    gpg: Option<String>,

    #[clap(subcommand)]
    subcommand: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Filesystem,
    Backblaze {
        #[clap(short, long)]
        key_id: String,
        #[clap(short, long)]
        application_key: String,
        #[clap(short, long)]
        bucket: String,
    },
}

pub fn run(repository: &camino::Utf8Path, args: Args) -> Result<()> {
    let (filter, unfilter) = match args.gpg {
        Some(g) => (
            Some("gpg --encrypt --quiet --recipient ".to_owned() + &g),
            Some("gpg --decrypt --quiet".to_owned()),
        ),
        None => (None, None),
    };
    if filter.is_some() {
        // Precondition: filter needs an unfilter
        assert!(unfilter.is_some());
        round_trip_filter_test(filter.as_ref().unwrap(), unfilter.as_ref().unwrap())?;
    }
    match args.subcommand {
        Command::Filesystem => {
            backend::fs::initialize(repository, args.pack_size, filter, unfilter)
        }
        Command::Backblaze {
            key_id,
            application_key,
            bucket,
        } => backend::backblaze::initialize(
            repository,
            args.pack_size,
            key_id,
            application_key,
            bucket,
            filter,
            unfilter,
        ),
    }
}

fn round_trip_filter_test(filter: &str, unfilter: &str) -> Result<()> {
    let plaintext = r"I'd like some help remembering stuff.
    I wonder if I could come down and see you,
    and we could drink and talk and remember.
    r";

    let mut f = process::Command::new("sh")
        .arg("-c")
        .arg(filter)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .with_context(|| format!("Couldn't run {filter}"))?;
    f.stdin
        .take()
        .unwrap()
        .write_all(plaintext.as_bytes())
        .with_context(|| format!("Couldn't write to {filter}"))?;
    let fr = f.wait_with_output().unwrap();
    ensure!(fr.status.success(), "{filter} failed");
    let filtered = fr.stdout;

    let mut uf = process::Command::new("sh")
        .arg("-c")
        .arg(unfilter)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .with_context(|| format!("Couldn't run {unfilter}"))?;
    uf.stdin
        .take()
        .unwrap()
        .write_all(&filtered)
        .with_context(|| format!("Couldn't write to {unfilter}"))?;
    let ufr = uf.wait_with_output().unwrap();
    ensure!(ufr.status.success(), "{unfilter} failed");

    let unfiltered = ufr.stdout;
    if unfiltered != plaintext.as_bytes() {
        let nope = String::from_utf8_lossy(&unfiltered);
        bail!("filter didn't round trip!\nExpected:\n{plaintext}\nGot:\n{nope}");
    }
    Ok(())
}

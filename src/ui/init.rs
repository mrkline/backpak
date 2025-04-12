use std::{
    io::prelude::*,
    process::{self, Stdio},
};

use anyhow::{Context, Result, bail, ensure};
use byte_unit::Byte;
use clap::{Parser, Subcommand};

use crate::backend;
use crate::pack;

#[derive(Debug, Parser)]
pub struct Args {
    #[clap(short, long)]
    pack_size: Option<String>,

    #[clap(long)]
    gpg: Option<String>,

    #[clap(subcommand)]
    subcommand: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Backup to a directory on the local filesystem.
    Filesystem {
        /// Normally filesystem backups skip caching in ~/.cache/backpak,
        /// assuming reads & writes directly to the backend are just as fast.
        /// If the given path is over a remote mount (e.g., FTP, SMB, SSHFS)
        /// use this to override that assumption.
        #[clap(long, verbatim_doc_comment)]
        force_cache: bool,
    },
    /// Backup to Backblaze B2
    Backblaze {
        #[clap(short, long)]
        key_id: String,
        #[clap(short, long)]
        application_key: String,
        #[clap(short, long)]
        bucket: String,
        #[clap(short, long, default_value_t = 4)]
        concurrent_connections: u32,
    },
}

pub fn run(repository: &camino::Utf8Path, args: Args) -> Result<()> {
    let pack_size = args
        .pack_size
        .map(|s| Byte::parse_str(s, true)) // Don't interpret b as bits.
        .transpose()
        .context("Couldn't parse --pack-size")?;
    let pack_size = pack_size.unwrap_or(pack::DEFAULT_PACK_SIZE);
    let filter = args.gpg.map(|g| {
        (
            "gpg --encrypt --quiet --recipient ".to_owned() + &g,
            "gpg --decrypt --quiet".to_owned(),
        )
    });
    if let Some((f, u)) = &filter {
        round_trip_filter_test(f, u)?;
    }
    match args.subcommand {
        Command::Filesystem { force_cache } => {
            backend::fs::initialize(repository, pack_size, filter, force_cache)
        }
        Command::Backblaze {
            key_id,
            application_key,
            bucket,
            concurrent_connections,
        } => backend::backblaze::initialize(
            repository,
            pack_size,
            filter,
            key_id,
            application_key,
            bucket,
            concurrent_connections,
        ),
    }
}

const PLAINTEXT: &str = r"I'd like some help remembering stuff.
I wonder if I could come down and see you,
and we could drink and talk and remember.
r";

fn round_trip_filter_test(filter: &str, unfilter: &str) -> Result<()> {
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
        .write_all(PLAINTEXT.as_bytes())
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
    if unfiltered != PLAINTEXT.as_bytes() {
        let nope = String::from_utf8_lossy(&unfiltered);
        bail!("filter didn't round trip!\nExpected:\n{PLAINTEXT}\nGot:\n{nope}");
    }
    Ok(())
}

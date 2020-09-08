use anyhow::Result;
use futures::prelude::*;
use futures::stream::FuturesUnordered;
use log::*;
use simplelog::*;
use structopt::StructOpt;
use tokio::fs::File;
use tokio::prelude::*;

use std::path::{Path, PathBuf};

const MEGA: u64 = 1024*1024;

#[derive(Debug, StructOpt)]
#[structopt(verbatim_doc_comment)]
struct Args {
    /// Verbosity (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    verbose: u8,

    /// Prepend ISO-8601 timestamps to all trace messages (from --verbose).
    /// Useful for benchmarking.
    #[structopt(short, long)]
    timestamps: bool,

    files: Vec<PathBuf>
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::from_args();
    init_logger(args.verbose, args.timestamps);

    let mut processes = args.files.into_iter().map(|file| {
        tokio::spawn(async move { process_file(&file).await })
    }).collect::<FuturesUnordered<_>>();
    while let Some(_fut) = processes.next().await {
    }
    Ok(())
}

async fn process_file(path: &Path) -> Result<()> {
    let _file = open_file(path).await;
    Ok(())
}

/// Set up simplelog to spit messages to stderr.
fn init_logger(verbosity: u8, timestamps: bool) {
    let mut builder = ConfigBuilder::new();
    // Shut a bunch of stuff off - we're just spitting to stderr.
    builder.set_location_level(LevelFilter::Trace);
    builder.set_target_level(LevelFilter::Off);
    builder.set_thread_level(LevelFilter::Off);
    if timestamps {
        builder.set_time_format_str("%+");
        builder.set_time_level(LevelFilter::Error);
    } else {
        builder.set_time_level(LevelFilter::Off);
    }

    let level = match verbosity {
        0 => LevelFilter::Warn,
        1 => LevelFilter::Info,
        2 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };

    TermLogger::init(level, builder.build(), TerminalMode::Stderr).expect("Couldn't init logger");
}

async fn open_file(path: &Path) -> Result<Box<dyn AsRef<[u8]>>>
{
    let mut fh = File::open(path).await?;
    let file_length = fh.metadata().await?.len();
    if file_length < 10*MEGA {
        debug!("{} is < 10MB, reading to buffer", path.display());
        let mut buffer = Vec::new();
        fh.read_to_end(&mut buffer).await?;
        Ok(Box::new(buffer))
    } else {
        debug!("{} is > 10MB, memory mapping", path.display());
        let mapping = unsafe { memmap::Mmap::map(&fh.into_std().await)? };
        Ok(Box::new(mapping))
    }
}

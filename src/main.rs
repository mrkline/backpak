use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

use anyhow::Result;
use log::*;
use simplelog::*;
use structopt::StructOpt;
use rayon::prelude::*;

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

fn main() -> Result<()> {
    let args = Args::from_args();
    init_logger(args.verbose, args.timestamps);

    args.files.into_par_iter().try_for_each(|file| {
        process_file(&file)
    })
}

fn process_file(path: &Path) -> Result<()> {
    let _file = open_file(path);
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

fn open_file(path: &Path) -> Result<Box<dyn AsRef<[u8]>>>
{
    let mut fh = File::open(path)?;
    let file_length = fh.metadata()?.len();
    if file_length < 10*MEGA {
        debug!("{} is < 10MB, reading to buffer", path.display());
        let mut buffer = Vec::new();
        fh.read_to_end(&mut buffer)?;
        Ok(Box::new(buffer))
    } else {
        debug!("{} is > 10MB, memory mapping", path.display());
        let mapping = unsafe { memmap::Mmap::map(&fh)? };
        Ok(Box::new(mapping))
    }
}

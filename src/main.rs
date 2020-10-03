use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::thread;

use anyhow::Result;
use rayon::prelude::*;
use simplelog::*;
use structopt::StructOpt;

mod chunk;
mod hashing;
mod pack;
mod serialize_hash;

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

    files: Vec<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::from_args();
    init_logger(args.verbose, args.timestamps);

    let (tx, rx) = channel();

    let packer = thread::spawn(move || pack::pack(rx));

    args.files
        .into_par_iter()
        .map(|file| chunk::chunk_file(file))
        .try_for_each_with::<_, _, Result<()>>(tx, |tx, chunked_file| {
            tx.send(chunked_file?).expect("Packer exited early");
            Ok(())
        })?;

    packer.join().unwrap()?;
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

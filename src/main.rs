use anyhow::*;
use simplelog::*;
use structopt::StructOpt;

mod backend;
mod backup;
mod cat;
mod chunk;
mod file_util;
mod hashing;
mod index;
mod init;
mod pack;
mod tree;

pub const DEFAULT_TARGET_SIZE: u64 = 1024 * 1024 * 100; // 100 MB

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

    #[structopt(short, long)]
    repository: String,

    #[structopt(subcommand)]
    subcommand: Subcommand,
}

#[derive(Debug, StructOpt)]
enum Subcommand {
    Init,
    Backup(backup::Args),
    Cat(cat::Args),
}

fn main() -> Result<()> {
    let args = Args::from_args();
    init_logger(args.verbose, args.timestamps);

    match args.subcommand {
        Subcommand::Init => init::run(&args.repository),
        Subcommand::Backup(b) => backup::run(&args.repository, b),
        Subcommand::Cat(b) => cat::run(b),
    }
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

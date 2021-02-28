use std::path::PathBuf;

use anyhow::*;
use simplelog::*;
use structopt::StructOpt;

use backpak::timers;
use backpak::ui::*;

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
    repository: PathBuf,

    #[structopt(subcommand)]
    subcommand: Subcommand,
}

#[derive(Debug, StructOpt)]
enum Subcommand {
    /// Initialize a backup repository
    Init,
    Backup(backup::Args),
    Cat(cat::Args),
    Check(check::Args),
    Forget(forget::Args),
    Ls(ls::Args),
    Prune(prune::Args),
    /// List the snapshots in this repository
    Snapshots,
    /// Build a new index from all existing packs
    RebuildIndex,
}

fn main() -> Result<()> {
    let args = Args::from_args();
    init_logger(args.verbose, args.timestamps)?;

    match args.subcommand {
        Subcommand::Init => init::run(&args.repository),
        Subcommand::Backup(b) => backup::run(&args.repository, b),
        Subcommand::Cat(c) => cat::run(&args.repository, c),
        Subcommand::Check(c) => check::run(&args.repository, c),
        Subcommand::Forget(f) => forget::run(&args.repository, f),
        Subcommand::Ls(l) => ls::run(&args.repository, l),
        Subcommand::Prune(p) => prune::run(&args.repository, p),
        Subcommand::Snapshots => snapshots::run(&args.repository),
        Subcommand::RebuildIndex => rebuild_index::run(&args.repository),
    }?;

    timers::log_times();
    Ok(())
}

/// Set up simplelog to spit messages to stderr.
fn init_logger(verbosity: u8, timestamps: bool) -> Result<()> {
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

    let config = builder.build();

    if cfg!(test) {
        TestLogger::init(level, config).context("Couldn't init test logger")
    } else {
        TermLogger::init(level, config.clone(), TerminalMode::Stderr)
            .or_else(|_| SimpleLogger::init(level, config))
            .context("Couldn't init logger")
    }
}

use anyhow::{Context, Result};
use camino::Utf8PathBuf;
use clap::{ArgAction, Parser, Subcommand};
use simplelog::*;

use backpak::counters;
use backpak::ui::*;

#[derive(Debug, Parser)]
struct Args {
    /// Verbosity (-v, -vv, -vvv, etc.)
    #[clap(short, long, action(ArgAction::Count))]
    verbose: u8,

    #[clap(short, long, value_enum, default_value = "auto")]
    color: Color,

    /// Prepend ISO-8601 timestamps to all trace messages (from --verbose).
    /// Useful for benchmarking.
    #[clap(short, long, verbatim_doc_comment)]
    timestamps: bool,

    /// Change to the given directory before doing anything else
    #[clap(short = 'C', long, name = "path")]
    working_directory: Option<Utf8PathBuf>,

    #[clap(short, long)]
    repository: Utf8PathBuf,

    #[clap(subcommand)]
    subcommand: Command,
}

#[derive(Debug, Copy, Clone, clap::ValueEnum)]
enum Color {
    Auto,
    Always,
    Never,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Initialize a backup repository
    Init,
    Backup(backup::Args),
    Cat(cat::Args),
    Check(check::Args),
    Diff(diff::Args),
    Dump(dump::Args),
    Forget(forget::Args),
    Ls(ls::Args),
    Prune(prune::Args),
    Restore(restore::Args),
    /// List the snapshots in this repository
    Snapshots,
    /// Build a new index from all existing packs and delete all old ones
    RebuildIndex,
}

fn main() {
    run().unwrap_or_else(|e| {
        log::error!("{:?}", e);
        std::process::exit(1);
    });
}

fn run() -> Result<()> {
    let args = Args::parse();
    init_logger(&args);

    if let Some(dir) = &args.working_directory {
        std::env::set_current_dir(dir).expect("Couldn't change working directory");
    }

    match args.subcommand {
        Command::Init => init::run(&args.repository),
        Command::Backup(b) => backup::run(&args.repository, b),
        Command::Cat(c) => cat::run(&args.repository, c),
        Command::Check(c) => check::run(&args.repository, c),
        Command::Diff(d) => diff::run(&args.repository, d),
        Command::Dump(d) => dump::run(&args.repository, d),
        Command::Forget(f) => forget::run(&args.repository, f),
        Command::Ls(l) => ls::run(&args.repository, l),
        Command::Prune(p) => prune::run(&args.repository, p),
        Command::Restore(r) => restore::run(&args.repository, r),
        Command::Snapshots => snapshots::run(&args.repository),
        Command::RebuildIndex => rebuild_index::run(&args.repository),
    }?;

    counters::log_counts();
    Ok(())
}

/// Set up simplelog to spit messages to stderr.
fn init_logger(args: &Args) {
    let mut builder = ConfigBuilder::new();
    builder.set_target_level(LevelFilter::Off);
    builder.set_thread_level(LevelFilter::Off);
    if args.timestamps {
        builder.set_time_format_rfc3339();
        builder.set_time_level(LevelFilter::Error);
    } else {
        builder.set_time_level(LevelFilter::Off);
    }

    let level = match args.verbose {
        0 => LevelFilter::Warn,
        1 => LevelFilter::Info,
        2 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };

    if level == LevelFilter::Trace {
        builder.set_location_level(LevelFilter::Error);
    }
    builder.set_level_padding(LevelPadding::Left);

    let config = builder.build();

    if cfg!(test) {
        TestLogger::init(level, config).context("Couldn't init test logger")
    } else {
        let color = match args.color {
            Color::Always => ColorChoice::AlwaysAnsi,
            Color::Auto => {
                if atty::is(atty::Stream::Stderr) {
                    ColorChoice::Auto
                } else {
                    ColorChoice::Never
                }
            }
            Color::Never => ColorChoice::Never,
        };

        TermLogger::init(level, config.clone(), TerminalMode::Stderr, color)
            .or_else(|_| SimpleLogger::init(level, config))
            .context("Couldn't init logger")
    }
    .unwrap()
}

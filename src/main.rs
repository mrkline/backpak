use anyhow::Result;
use camino::Utf8PathBuf;
use clap::{ArgAction, Parser, Subcommand};
use tracing::*;

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
    #[clap(short = 'C', long, name = "PATH")]
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
    Init(init::Args),
    Backup(backup::Args),
    Cat(cat::Args),
    Check(check::Args),
    Copy(copy::Args),
    Diff(diff::Args),
    Dump(dump::Args),
    FilterSnapshot(filter_snapshot::Args),
    Forget(forget::Args),
    Ls(ls::Args),
    Prune(prune::Args),
    Restore(restore::Args),
    Snapshots(snapshots::Args),
    /// Build a new index from all existing packs and delete all old ones.
    RebuildIndex(rebuild_index::Args),
    /// Print repository size stats.
    Usage,
}

fn main() {
    run().unwrap_or_else(|e| {
        error!("{:?}", e);
        std::process::exit(1);
    });
}

fn run() -> Result<()> {
    let args = Args::parse();
    let logmode = match args.subcommand {
        Command::Diff(_) | Command::Dump(_) | Command::Ls(_) => LogMode::Quiet,
        _ => LogMode::InfoStdout,
    };
    init_logger(&args, logmode);

    if let Some(dir) = &args.working_directory {
        std::env::set_current_dir(dir).expect("Couldn't change working directory");
    }

    match args.subcommand {
        Command::Init(i) => init::run(&args.repository, i),
        Command::Backup(b) => backup::run(&args.repository, b),
        Command::Cat(c) => cat::run(&args.repository, c),
        Command::Check(c) => check::run(&args.repository, c),
        Command::Copy(c) => copy::run(&args.repository, c),
        Command::Diff(d) => diff::run(&args.repository, d),
        Command::Dump(d) => dump::run(&args.repository, d),
        Command::FilterSnapshot(f) => filter_snapshot::run(&args.repository, f),
        Command::Forget(f) => forget::run(&args.repository, f),
        Command::Ls(l) => ls::run(&args.repository, l),
        Command::Prune(p) => prune::run(&args.repository, p),
        Command::Restore(r) => restore::run(&args.repository, r),
        Command::Snapshots(s) => snapshots::run(&args.repository, s),
        Command::RebuildIndex(r) => rebuild_index::run(&args.repository, r),
        Command::Usage => usage::run(&args.repository),
    }?;

    counters::log_counts();
    Ok(())
}

enum LogMode {
    /// Print INTO to stdout (for noisy commands like backup, check, etc.)
    InfoStdout,
    /// Don't print INFO to stdout (for commands like ls, diff, etc.)
    Quiet,
}

/// Set up simplelog to spit messages to stderr based on -v,
/// and unadorned INFO messages and up to stdout as part of the progress
fn init_logger(args: &Args, m: LogMode) {
    use tracing_subscriber::prelude::*;

    let level = match args.verbose {
        0 => Level::WARN,
        1 => Level::INFO,
        2 => Level::DEBUG,
        _ => Level::TRACE,
    };
    let ansis = match args.color {
        Color::Always => true,
        Color::Auto => {
            use std::io::IsTerminal;
            std::io::stderr().is_terminal()
        }
        Color::Never => false,
    };

    let stderr_layer = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr.with_max_level(level))
        .with_ansi(ansis);

    let stderr_layer = if level == Level::TRACE {
        stderr_layer.with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
    } else {
        stderr_layer.with_target(false)
    };

    let stderr_layer = if args.timestamps {
        stderr_layer
            .with_timer(tracing_subscriber::fmt::time::SystemTime)
            .boxed()
    } else {
        stderr_layer.without_time().boxed()
    };

    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_writer(
            std::io::stdout
                .with_max_level(Level::INFO)
                .with_min_level(Level::INFO),
        )
        .without_time()
        .with_level(false)
        .with_target(false)
        .with_ansi(ansis); // Not used for anything at the moment

    let reg = tracing_subscriber::registry().with(stderr_layer);

    match m {
        LogMode::Quiet => reg.init(),
        LogMode::InfoStdout => reg.with(stdout_layer).init(),
    }
}

use std::path::PathBuf;

use anyhow::*;
use simplelog::*;
use structopt::clap::arg_enum;
use structopt::StructOpt;

use backpak::counters;
use backpak::ui::*;

#[derive(Debug, StructOpt)]
#[structopt(verbatim_doc_comment)]
struct Args {
    /// Verbosity (-v, -vv, -vvv, etc.)
    #[structopt(short, long, parse(from_occurrences))]
    verbose: u8,

    #[structopt(short, long, case_insensitive = true, default_value = "auto")]
    #[structopt(name = "always/auto/never")]
    color: Color,

    /// Prepend ISO-8601 timestamps to all trace messages (from --verbose).
    /// Useful for benchmarking.
    #[structopt(short, long)]
    timestamps: bool,

    /// Change to the given directory before doing anything else
    #[structopt(short = "C", long, name = "path")]
    #[structopt(verbatim_doc_comment)]
    working_directory: Option<PathBuf>,

    #[structopt(short, long)]
    repository: PathBuf,

    #[structopt(subcommand)]
    subcommand: Subcommand,
}

arg_enum! {
    #[derive(Debug)]
    enum Color {
        Auto,
        Always,
        Never
    }
}

#[derive(Debug, StructOpt)]
enum Subcommand {
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
    /// List the snapshots in this repository
    Snapshots,
    /// Build a new index from all existing packs
    RebuildIndex,
}

fn main() {
    run().unwrap_or_else(|e| {
        log::error!("{:?}", e);
        std::process::exit(1);
    });
}

fn run() -> Result<()> {
    let args = Args::from_args();
    init_logger(&args);

    if let Some(dir) = &args.working_directory {
        std::env::set_current_dir(dir).expect("Couldn't change working directory");
    }

    match args.subcommand {
        Subcommand::Init => init::run(&args.repository),
        Subcommand::Backup(b) => backup::run(&args.repository, b),
        Subcommand::Cat(c) => cat::run(&args.repository, c),
        Subcommand::Check(c) => check::run(&args.repository, c),
        Subcommand::Diff(d) => diff::run(&args.repository, d),
        Subcommand::Dump(d) => dump::run(&args.repository, d),
        Subcommand::Forget(f) => forget::run(&args.repository, f),
        Subcommand::Ls(l) => ls::run(&args.repository, l),
        Subcommand::Prune(p) => prune::run(&args.repository, p),
        Subcommand::Snapshots => snapshots::run(&args.repository),
        Subcommand::RebuildIndex => rebuild_index::run(&args.repository),
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
        builder.set_time_format_str("%+");
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
    }.unwrap()
}

// Smoke test of gets and puts we need for B2 usage.
use std::io::prelude::*;

use anyhow::{Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use clap::{ArgAction, Parser, Subcommand};
use serde_derive::{Deserialize, Serialize};
use simplelog::*;

#[derive(Debug, Parser)]
struct Args {
    /// Verbosity (-v, -vv, -vvv, etc.)
    #[clap(short, long, action(ArgAction::Count))]
    verbose: u8,
    /// Prepend ISO-8601 timestamps to all trace messages (from --verbose).
    /// Useful for benchmarking.
    #[clap(short, long, verbatim_doc_comment)]
    timestamps: bool,

    #[clap(subcommand)]
    subcommand: Command,

    #[clap(short, long)]
    credentials: Utf8PathBuf,

    #[clap(short, long)]
    bucket: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Credentials {
    pub key_id: String,
    pub application_key: String,
}

fn read_creds(p: &Utf8Path) -> Result<Credentials> {
    let s = std::fs::read_to_string(p).with_context(|| format!("Couldn't read creds from {p}"))?;
    let c = toml::from_str(&s)?;
    Ok(c)
}

#[derive(Debug, Subcommand)]
enum Command {
    List,
    Get { name: String },
    Put { name: String },
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
    let creds = read_creds(&args.credentials)?;

    match args.subcommand {
        Command::List => {
            let s = b2::Session::new(&creds.key_id, &creds.application_key, &args.bucket)?;
            let files = s.list()?;
            for f in files {
                println!("{f}");
            }
            Ok(())
        }
        Command::Get { name } => {
            let s = b2::Session::new(&creds.key_id, &creds.application_key, &args.bucket)?;
            let bytes = s.get(&name)?;
            std::io::stdout().lock().write_all(&bytes)?;
            Ok(())
        }
        Command::Put { name } => {
            let s = b2::Session::new(&creds.key_id, &creds.application_key, &args.bucket)?;
            let mut to_put = vec![];
            std::io::stdin().lock().read_to_end(&mut to_put)?;
            s.put(&name, &to_put)?;
            Ok(())
        }
    }
}

// Copied from the backpak main:

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

    TermLogger::init(
        level,
        config.clone(),
        TerminalMode::Stderr,
        ColorChoice::Auto,
    )
    .or_else(|_| SimpleLogger::init(level, config))
    .context("Couldn't init logger")
    .unwrap()
}

use std::io;
use std::io::prelude::*;

use anyhow::{bail, Context, Result};
use camino::{Utf8Component, Utf8Path, Utf8PathBuf};
use clap::Parser;
use tracing::*;

use crate::backend;
use crate::index;
use crate::read;
use crate::snapshot;
use crate::tree;

/// Print a given file or directory from a given snapshot
#[derive(Debug, Parser)]
pub struct Args {
    /// Write to the given file instead of stdout
    #[clap(short, long, name = "FILE")]
    output: Option<Utf8PathBuf>,

    snapshot: String,

    path: Utf8PathBuf,
}

pub fn run(repository: &Utf8Path, args: Args) -> Result<()> {
    unsafe {
        crate::prettify::prettify_serialize();
    }

    let (_cfg, cached_backend) = backend::open(repository, backend::CacheBehavior::Normal)?;
    let snapshots = snapshot::load_chronologically(&cached_backend)?;
    let (snapshot, id) = snapshot::find(&snapshots, &args.snapshot)?;
    let index = index::build_master_index(&cached_backend)?;
    let blob_map = index::blob_to_pack_map(&index)?;
    let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);
    let mut current_tree_id = snapshot.tree;
    let mut path_so_far = Utf8PathBuf::new();

    info!("Printing {} from snapshot {}", args.path, id);

    let mut components = args.path.components().peekable();
    while let Some(component) = components.next() {
        let is_last_component = components.peek().is_none();
        let component = match component {
            Utf8Component::CurDir => continue,
            Utf8Component::Normal(c) => Utf8Path::new(c),
            _ => bail!("dump doesn't support absolute paths, .., etc."),
        };

        debug!(
            "Looking for {} in {} (tree {})",
            component,
            if path_so_far.as_str().is_empty() {
                Utf8Path::new("root tree")
            } else {
                &path_so_far
            },
            current_tree_id
        );

        path_so_far.push(component);
        let current_tree = tree_cache.read(&current_tree_id)?;
        let node = match current_tree.get(component) {
            None => {
                bail!("Couldn't find {} in the given snapshot", path_so_far);
            }
            Some(n) => n,
        };

        match &node.contents {
            tree::NodeContents::Directory { subtree } => {
                if is_last_component {
                    let tree_to_dump = tree_cache.read(subtree)?;
                    dump_dir(&tree_to_dump, &path_so_far, &args.output)?;
                } else {
                    current_tree_id = *subtree; // Continue our search.
                }
            }
            tree::NodeContents::Symlink { target } => {
                dump_symlink(target, &path_so_far, &args.output)?;
            }
            tree::NodeContents::File { chunks } => {
                if is_last_component {
                    dump_file(chunks, &cached_backend, &index, &blob_map, &args.output)?;
                } else {
                    bail!("{path_so_far} is a file, not a directory");
                }
            }
        };
    }
    Ok(())
}

fn dump_dir(
    tree_to_dump: &tree::Tree,
    path_so_far: &Utf8Path,
    output_path: &Option<Utf8PathBuf>,
) -> Result<()> {
    let mut writer = open_writer(output_path)?;
    writeln!(writer, "{}{}", path_so_far, std::path::MAIN_SEPARATOR)?;
    for (path, node) in tree_to_dump {
        write!(
            writer,
            "{}{}{}",
            path_so_far,
            std::path::MAIN_SEPARATOR,
            path
        )?;
        match &node.contents {
            tree::NodeContents::Directory { .. } => {
                // If it's a directory, write a trailing /
                writeln!(writer, "{}", std::path::MAIN_SEPARATOR)
            }
            tree::NodeContents::Symlink { target } => {
                writeln!(writer, "-> {target}")
            }
            tree::NodeContents::File { .. } => writeln!(writer),
        }?;
    }
    writer.flush()?;
    Ok(())
}

fn dump_symlink(
    target: &Utf8Path,
    path_so_far: &Utf8Path,
    output_path: &Option<Utf8PathBuf>,
) -> Result<()> {
    let mut writer = open_writer(output_path)?;
    writeln!(writer, "{path_so_far} -> {target}")?;
    writer.flush()?;
    Ok(())
}

fn dump_file(
    chunks: &[crate::hashing::ObjectId],
    cached_backend: &backend::CachedBackend,
    index: &index::Index,
    blob_map: &index::BlobMap,
    output_path: &Option<Utf8PathBuf>,
) -> Result<()> {
    let mut reader = read::ChunkReader::new(cached_backend, index, blob_map);
    let mut writer = open_writer(output_path)?;

    for chunk_id in chunks {
        let chunk = reader.read_blob(chunk_id)?;
        writer.write_all(&chunk)?;
    }
    writer.flush()?;
    Ok(())
}

fn open_writer(output_path: &Option<Utf8PathBuf>) -> Result<io::BufWriter<Box<dyn Write>>> {
    let writer: Box<dyn Write> = match output_path {
        Some(p) => {
            if p == "-" {
                Box::new(io::stdout().lock())
            } else {
                Box::new(
                    std::fs::File::create(p)
                        .with_context(|| format!("Couldn't create file {p}"))?,
                )
            }
        }
        None => Box::new(io::stdout().lock()),
    };
    Ok(io::BufWriter::new(writer))
}

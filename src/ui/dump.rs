use std::io;
use std::io::prelude::*;
use std::path::{Component, Path, PathBuf};

use anyhow::*;
use lazy_static::lazy_static;
use log::*;
use structopt::StructOpt;

use crate::backend;
use crate::index;
use crate::read;
use crate::snapshot;
use crate::tree;

/// Print a given file from a given snapshot
#[derive(Debug, StructOpt)]
pub struct Args {
    #[structopt(short, long)]
    output: Option<PathBuf>,

    snapshot: String,
    path: PathBuf,
}

pub fn run(repository: &Path, args: Args) -> Result<()> {
    unsafe {
        crate::prettify::prettify_serialize();
    }

    let cached_backend = backend::open(repository)?;
    let (snapshot, id) = snapshot::find_and_load(&args.snapshot, &cached_backend)?;
    let index = index::build_master_index(&cached_backend)?;
    let blob_map = index::blob_to_pack_map(&index)?;
    let mut tree_cache = tree::Cache::new(&index, &blob_map, &cached_backend);
    let mut current_tree_id = snapshot.tree;
    let mut path_so_far = PathBuf::new();

    info!("Printing {} from snapshot {}", args.path.display(), id);

    let mut components = args.path.components().peekable();
    while let Some(component) = components.next() {
        let is_last_component = components.peek().is_none();
        let component = match component {
            Component::CurDir => continue,
            Component::Normal(c) => Path::new(c),
            _ => bail!("dump doesn't support absolute paths, .., etc."),
        };

        debug!(
            "Looking for {} in {} (tree {})",
            component.display(),
            if path_so_far.as_os_str().is_empty() {
                Path::new("root tree").display()
            } else {
                path_so_far.display()
            },
            current_tree_id
        );

        path_so_far.push(component);
        let current_tree = tree_cache.read(&current_tree_id)?;
        let node = match current_tree.get(component) {
            None => {
                bail!(
                    "Couldn't find {} in the given snapshot",
                    path_so_far.display()
                );
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
                    bail!("{} is a file, not a directory", path_so_far.display());
                }
            }
        };
    }
    Ok(())
}

fn dump_dir(
    tree_to_dump: &tree::Tree,
    path_so_far: &Path,
    output_path: &Option<PathBuf>,
) -> Result<()> {
    let mut writer = open_writer(output_path)?;
    writeln!(
        writer,
        "{}{}",
        path_so_far.display(),
        std::path::MAIN_SEPARATOR
    )?;
    for (path, node) in tree_to_dump {
        write!(
            writer,
            "{}{}{}",
            path_so_far.display(),
            std::path::MAIN_SEPARATOR,
            path.display()
        )?;
        match &node.contents {
            tree::NodeContents::Directory { .. } => {
                // If it's a directory, write a trailing /
                writeln!(writer, "{}", std::path::MAIN_SEPARATOR)
            }
            tree::NodeContents::Symlink { target } => {
                writeln!(writer, "-> {}", target.display())
            }
            tree::NodeContents::File { .. } => writeln!(writer),
        }?;
    }
    writer.flush()?;
    Ok(())
}

fn dump_symlink(target: &Path, path_so_far: &Path, output_path: &Option<PathBuf>) -> Result<()> {
    let mut writer = open_writer(output_path)?;
    writeln!(writer, "{} -> {}", path_so_far.display(), target.display())?;
    writer.flush()?;
    Ok(())
}

fn dump_file(
    chunks: &[crate::hashing::ObjectId],
    cached_backend: &backend::CachedBackend,
    index: &index::Index,
    blob_map: &index::BlobMap,
    output_path: &Option<PathBuf>,
) -> Result<()> {
    let mut reader = read::BlobReader::new(cached_backend, index, blob_map);
    let mut writer = open_writer(output_path)?;

    for chunk_id in chunks {
        let chunk = reader.read_blob(chunk_id)?;
        writer.write_all(&chunk)?;
    }
    writer.flush()?;
    Ok(())
}

fn open_writer(output_path: &Option<PathBuf>) -> Result<io::BufWriter<Box<dyn Write>>> {
    lazy_static! {
        static ref STDOUT: io::Stdout = io::stdout();
    }

    let writer: Box<dyn Write> = match output_path {
        Some(p) => {
            if p.to_str() == Some("-") {
                Box::new(STDOUT.lock())
            } else {
                Box::new(
                    std::fs::File::create(p)
                        .with_context(|| format!("Couldn't create file {}", p.display()))?,
                )
            }
        }
        None => Box::new(STDOUT.lock()),
    };
    Ok(io::BufWriter::new(writer))
}

//! Print [trees](crate::tree)

use std::path::Path;

use anyhow::anyhow;

use crate::hashing::ObjectId;
use crate::tree;

pub enum Recurse<'a> {
    Yes(&'a tree::Forest),
    No,
}

// https://users.rust-lang.org/t/trailing-in-paths/43166/2
#[cfg(windows)]
fn has_trailing_slash(p: &Path) -> bool {
    use std::os::windows::ffi::OsStrExt;

    let last = p.as_os_str().encode_wide().last();
    last == Some(b'\\' as u16) || last == Some(b'/' as u16)
}

#[cfg(unix)]
fn has_trailing_slash(p: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    p.as_os_str().as_bytes().last() == Some(&b'/')
}

pub fn print_node(prefix: &str, path: &Path, node: &tree::Node, should_recurse: Recurse) {
    print!("{}{}", prefix, path.display());
    match &node.contents {
        tree::NodeContents::Directory { subtree } => {
            if !has_trailing_slash(path) {
                println!("{}", std::path::MAIN_SEPARATOR);
            } else {
                println!();
            }
            if let Recurse::Yes(forest) = should_recurse {
                print_tree(prefix, path, subtree, forest);
            }
        }
        tree::NodeContents::File { .. } => {
            println!();
        }
        tree::NodeContents::Symlink { target } => {
            println!(" -> {}", target.display());
        }
    };
}

pub fn print_tree(prefix: &str, tree_path: &Path, tree_id: &ObjectId, forest: &tree::Forest) {
    let tree: &tree::Tree = forest
        .get(tree_id)
        .ok_or_else(|| anyhow!("Missing tree {}", tree_id))
        .unwrap();

    for (path, node) in tree {
        let mut node_path = tree_path.to_owned();
        node_path.push(path);
        print_node(prefix, &node_path, node, Recurse::Yes(forest));
    }
}

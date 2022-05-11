//! Print [trees](crate::tree)

use anyhow::anyhow;
use camino::Utf8Path;

use crate::hashing::ObjectId;
use crate::tree;

pub enum Recurse<'a> {
    Yes(&'a tree::Forest),
    No,
}

#[cfg(windows)]
fn has_trailing_slash(p: &Utf8Path) -> bool {
    let last = p.as_str().as_bytes().last();
    last == Some(b'\\') || last == Some(b'/')
}

#[cfg(unix)]
fn has_trailing_slash(p: &Utf8Path) -> bool {
    p.as_str().as_bytes().last() == Some(&b'/')
}

pub fn print_node(prefix: &str, path: &Utf8Path, node: &tree::Node, should_recurse: Recurse) {
    print!("{prefix}{path}");
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
            println!(" -> {target}");
        }
    };
}

pub fn print_tree(prefix: &str, tree_path: &Utf8Path, tree_id: &ObjectId, forest: &tree::Forest) {
    let tree: &tree::Tree = forest
        .get(tree_id)
        .ok_or_else(|| anyhow!("Missing tree {tree_id}"))
        .unwrap();

    for (path, node) in tree {
        let mut node_path = tree_path.to_owned();
        node_path.push(path);
        print_node(prefix, &node_path, node, Recurse::Yes(forest));
    }
}

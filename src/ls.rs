//! Print [trees](crate::tree)

use std::path::Path;

use anyhow::*;

use crate::hashing::ObjectId;
use crate::tree;

pub fn print_node(prefix: &str, path: &Path, node: &tree::Node, forest: &tree::Forest) {
    print!("{}{}", prefix, path.display());
    match &node.contents {
        tree::NodeContents::Directory { subtree } => {
            println!("{}", std::path::MAIN_SEPARATOR);
            print_tree(prefix, &path, subtree, forest);
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
        print_node(prefix, &node_path, node, forest);
    }
}

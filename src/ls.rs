//! Print [trees](crate::tree)

use anyhow::anyhow;
use camino::Utf8Path;

use crate::hashing::ObjectId;
use crate::tree::{Forest, Node, NodeContents, Tree};

// Should this live somewhere else?
#[cfg(windows)]
pub fn has_trailing_slash(p: &Utf8Path) -> bool {
    let last = p.as_str().as_bytes().last();
    last == Some(&b'\\') || last == Some(&b'/')
}

#[cfg(unix)]
pub fn has_trailing_slash(p: &Utf8Path) -> bool {
    p.as_str().as_bytes().last() == Some(&b'/')
}

fn printer(prefix: &str, path: &Utf8Path, node: &Node) {
    print!("{prefix}{path}");
    match &node.contents {
        NodeContents::Directory { .. } => {
            if !has_trailing_slash(path) {
                print!("{}", std::path::MAIN_SEPARATOR);
            }
        }
        NodeContents::File { .. } => {}
        NodeContents::Symlink { target } => {
            print!(" -> {target}");
        }
    };
    println!();
}

// I tried turning walk_node() and walk_tree() into something general we could use for all
// tree-walking activities - forest_size(), blobs_in_forest, etc. but it doesn't seem worth it.
// For printing things, our action ("visitor"? I've almost cured myself of the OOP-brain)
// just needs the path. But in other cases we don't give a rat's ass for that, we want the ID!
// And on and on. Plumbing every permutation doesn't seem worth the squeeze.
// Maybe I'll come back some day when I grok recursion schemes more and laugh at this.

#[derive(Debug)]
pub enum Recurse<'a> {
    No,
    Yes(&'a Forest),
}

/// Walk a node given some action, its path, and whether we should recurse.
pub fn walk_node<V>(v: &mut V, path: &Utf8Path, node: &Node, should_recurse: Recurse)
where
    V: FnMut(&Utf8Path, &Node),
{
    v(path, node);
    if let Recurse::Yes(forest) = should_recurse {
        match &node.contents {
            NodeContents::Directory { subtree } => walk_tree(v, path, subtree, forest),
            NodeContents::File { .. } | NodeContents::Symlink { .. } => (),
        }
    }
}

/// Walk a tree given some action, its path, its forest.
pub fn walk_tree<V>(v: &mut V, tree_path: &Utf8Path, tree_id: &ObjectId, forest: &Forest)
where
    V: FnMut(&Utf8Path, &Node),
{
    let tree: &Tree = forest
        .get(tree_id)
        .ok_or_else(|| anyhow!("Missing tree {tree_id}"))
        .unwrap();

    for (path, node) in tree {
        let mut node_path = tree_path.to_owned();
        node_path.push(path);
        walk_node(v, &node_path, node, Recurse::Yes(forest));
    }
}

pub fn print_node(prefix: &str, path: &Utf8Path, node: &Node, should_recurse: Recurse) {
    let mut v = |p: &Utf8Path, n: &Node| printer(prefix, p, n);
    walk_node(&mut v, path, node, should_recurse);
}

pub fn print_tree(prefix: &str, tree_path: &Utf8Path, tree_id: &ObjectId, forest: &Forest) {
    let mut v = |p: &Utf8Path, n: &Node| printer(prefix, p, n);
    walk_tree(&mut v, tree_path, tree_id, forest);
}

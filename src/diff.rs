use std::collections::BTreeSet;
use std::path::Path;

use anyhow::*;

use crate::hashing::ObjectId;
use crate::tree::{Forest, Node, NodeType, Tree};

pub trait Callbacks {
    fn node_added(&mut self, node_path: &Path, new_node: &Node, forest: &Forest);
    fn node_removed(&mut self, node_path: &Path, old_node: &Node, forest: &Forest);
    fn contents_changed(&mut self, node_path: &Path, node: &Node);
    fn metadata_changed(&mut self, node_path: &Path, node: &Node);
    fn nothing_changed(&mut self, _node_path: &Path, _node: &Node) {}
    fn type_changed(
        &mut self,
        node_path: &Path,
        old_node: &Node,
        old_forest: &Forest,
        new_node: &Node,
        new_forest: &Forest,
    );
}

pub fn compare_trees(
    (id1, forest1): (&ObjectId, &Forest),
    (id2, forest2): (&ObjectId, &Forest),
    tree_path: &Path,
    callbacks: &mut dyn Callbacks,
) {
    let tree1: &Tree = forest1
        .get(id1)
        .ok_or_else(|| anyhow!("Missing tree {}", id1))
        .unwrap();

    let tree2: &Tree = forest2
        .get(id2)
        .ok_or_else(|| anyhow!("Missing tree {}", id2))
        .unwrap();

    let all_paths = tree1.keys().chain(tree2.keys()).collect::<BTreeSet<_>>();
    for path in all_paths {
        let mut node_path = tree_path.to_owned();
        node_path.push(path);
        match (tree1.get(path), tree2.get(path)) {
            (None, None) => unreachable!(),
            (None, Some(new_node)) => callbacks.node_added(&node_path, new_node, forest2),
            (Some(old_node), None) => callbacks.node_removed(&node_path, old_node, forest1),
            (Some(l), Some(r)) => {
                compare_nodes((l, forest1), (r, forest2), &node_path, callbacks);
            }
        };
    }
}

pub fn compare_nodes(
    (node1, forest1): (&Node, &Forest),
    (node2, forest2): (&Node, &Forest),
    path: &Path,
    callbacks: &mut dyn Callbacks,
) {
    match (node1.kind(), node2.kind()) {
        (NodeType::File, NodeType::File) => {
            if node1.contents != node2.contents {
                callbacks.contents_changed(path, node1);
            } else if node1.metadata != node2.metadata {
                // trace!("{:#?} != {:#?}", node1.metadata, node2.metadata);
                callbacks.metadata_changed(path, node1);
            } else {
                callbacks.nothing_changed(path, node1);
            }
        }
        (NodeType::Directory, NodeType::Directory) => {
            // Both are directories
            compare_trees(
                (node1.contents.subtree(), forest1),
                (node2.contents.subtree(), forest2),
                path,
                callbacks,
            );
        }
        _ => {
            callbacks.type_changed(&path, node1, forest1, node2, forest2);
        }
    }
}

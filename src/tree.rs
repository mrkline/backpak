use std::path::PathBuf;

use serde_derive::*;

use crate::hashing::ObjectId;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeType {
    File { content: Vec<ObjectId> },
    Dir { subtree: ObjectId },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Node {
    pub name: PathBuf,

    #[serde(rename = "type")]
    pub node_type: NodeType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Tree {
    pub nodes: Vec<Node>,
}

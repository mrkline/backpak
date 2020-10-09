use std::collections::{BTreeMap, BTreeSet};

use serde_derive::*;

use crate::hashing::ObjectId;
use crate::pack::PackManifest;

#[derive(Debug, Serialize, Deserialize)]
pub struct Index {
    pub supersedes: BTreeSet<ObjectId>,
    pub packs: BTreeMap<ObjectId, PackManifest>,
}

// TODO: A "fill it up and start a new one" loop similar to the packfile one.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::*;
use chrono::{offset::Utc, DateTime, TimeZone};
use serde_derive::*;

use crate::hashing::ObjectId;
use crate::prettify;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum NodeContents {
    File { chunks: Vec<ObjectId> },
    Dir { subtree: ObjectId },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeMetadata {
    // POSIX-specific stuff. Could be none if run in Windows land.
    mode: Option<u32>,
    user_id: Option<u32>,
    group_id: Option<u32>,
    #[serde(with = "prettify::date_time")]
    change_time: DateTime<Utc>,
    #[serde(with = "prettify::date_time")]
    access_time: DateTime<Utc>,
    #[serde(with = "prettify::date_time")]
    modify_time: DateTime<Utc>,
}

#[cfg(target_family = "unix")]
pub fn get_metadata(path: &Path) -> Result<NodeMetadata> {
    use std::os::unix::fs::MetadataExt;

    let meta = fs::metadata(path).with_context(|| format!("Couldn't stat {}", path.display()))?;
    let mode = Some(meta.mode());
    let user_id = Some(meta.uid());
    let group_id = Some(meta.gid());
    let change_time = chrono::Utc.timestamp(meta.ctime(), meta.ctime_nsec() as u32);
    let access_time = chrono::Utc.timestamp(meta.atime(), meta.atime_nsec() as u32);
    let modify_time = chrono::Utc.timestamp(meta.mtime(), meta.mtime_nsec() as u32);

    Ok(NodeMetadata {
        mode,
        user_id,
        group_id,
        change_time,
        access_time,
        modify_time,
    })
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Node {
    #[serde(flatten)]
    pub contents: NodeContents,
    pub metadata: NodeMetadata,
}

pub type Tree = BTreeMap<PathBuf, Node>;

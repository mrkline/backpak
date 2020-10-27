use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::*;
use chrono::{offset::Utc, DateTime, TimeZone};
use serde_derive::*;

use crate::hashing::ObjectId;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeContents {
    File {
        // Restic calls this "content", but "contents" seems more common:
        // https://english.stackexchange.com/questions/56831/file-content-vs-file-contents
        contents: Vec<ObjectId>,
    },
    Dir {
        subtree: ObjectId,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LinuxMetadata {
    mode: u32,
    uid: u32,
    gid: u32,
    ctime: DateTime<Utc>,
    atime: DateTime<Utc>,
    mtime: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NodeMetadata {
    Linux(LinuxMetadata),
    // TODO: Windows? POSIX?
}

#[cfg(target_os = "linux")]
pub fn get_metadata(path: &Path) -> Result<NodeMetadata> {
    use std::os::linux::fs::MetadataExt;

    let meta = fs::metadata(path).with_context(|| format!("Couldn't stat {}", path.display()))?;
    let mode = meta.st_mode();
    let uid = meta.st_uid();
    let gid = meta.st_gid();
    let ctime = chrono::Utc.timestamp(meta.st_ctime(), meta.st_ctime_nsec() as u32);
    let atime = chrono::Utc.timestamp(meta.st_atime(), meta.st_atime_nsec() as u32);
    let mtime = chrono::Utc.timestamp(meta.st_mtime(), meta.st_mtime_nsec() as u32);

    Ok(NodeMetadata::Linux(LinuxMetadata {
        mode,
        uid,
        gid,
        ctime,
        atime,
        mtime,
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Node {
    pub contents: NodeContents,
    pub metadata: NodeMetadata,
}

pub type Tree = BTreeMap<PathBuf, Node>;

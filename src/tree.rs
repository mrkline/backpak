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
    File { chunks: Vec<ObjectId>, length: u64 },
    Dir { subtree: ObjectId },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PosixMetadata {
    mode: u32,
    user_id: u32,
    group_id: u32,
    #[serde(with = "prettify::date_time")]
    access_time: DateTime<Utc>,
    #[serde(with = "prettify::date_time")]
    modify_time: DateTime<Utc>,
    #[serde(with = "prettify::date_time")]
    change_time: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WindowsMetadata {
    attributes: u32,
    #[serde(with = "prettify::date_time_option")]
    creation_time: Option<DateTime<Utc>>,
    #[serde(with = "prettify::date_time_option")]
    access_time: Option<DateTime<Utc>>,
    #[serde(with = "prettify::date_time_option")]
    write_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum NodeMetadata {
    Posix(PosixMetadata),
    Windows(WindowsMetadata),
}

#[cfg(target_family = "unix")]
pub fn get_metadata(path: &Path) -> Result<NodeMetadata> {
    use std::os::unix::fs::MetadataExt;

    let meta = fs::metadata(path).with_context(|| format!("Couldn't stat {}", path.display()))?;
    let mode = meta.mode();
    let user_id = meta.uid();
    let group_id = meta.gid();
    let access_time = chrono::Utc.timestamp(meta.atime(), meta.atime_nsec() as u32);
    let modify_time = chrono::Utc.timestamp(meta.mtime(), meta.mtime_nsec() as u32);
    let change_time = chrono::Utc.timestamp(meta.ctime(), meta.ctime_nsec() as u32);

    Ok(NodeMetadata::Posix(PosixMetadata {
        mode,
        user_id,
        group_id,
        change_time,
        access_time,
        modify_time,
    }))
}

#[cfg(target_family = "windows")]
pub fn get_metadata(path: &Path) -> Result<NodeMetadata> {
    use std::os::windows::fs::MetadataExt;

    let meta = fs::metadata(path).with_context(|| format!("Couldn't stat {}", path.display()))?;
    let attributes = meta.file_attributes();

    let creation_time = windows_timestamp(meta.creation_time());
    let access_time = windows_timestamp(meta.last_access_time());
    let write_time = windows_timestamp(meta.last_write_time());

    Ok(NodeMetadata::Windows(WindowsMetadata {
        attributes,
        creation_time,
        access_time,
        write_time,
    }))
}

#[cfg(target_family = "windows")]
pub fn windows_timestamp(ts: u64) -> Option<DateTime<Utc>> {
    // Windows returns 100ns intervals since January 1, 1601
    match ts {
        0 => None,
        stamp => Some(
            Utc.ymd(1601, 1, 1).and_hms(0, 0, 0)
                + chrono::Duration::nanoseconds(stamp as i64 * 100),
        ),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Node {
    #[serde(flatten)]
    pub contents: NodeContents,
    pub metadata: NodeMetadata,
}

pub type Tree = BTreeMap<PathBuf, Node>;

pub fn serialize_and_hash(tree: &Tree) -> Result<(Vec<u8>, ObjectId)> {
    let tree_cbor = serde_cbor::to_vec(tree)?;
    let id = ObjectId::hash(&tree_cbor);
    Ok((tree_cbor, id))
}

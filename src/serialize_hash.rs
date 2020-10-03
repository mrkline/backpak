use std::result::Result;

use crate::hashing::Sha224Sum;

pub fn serialize<S>(hash: &Sha224Sum, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_bytes(hash.as_slice())
}

pub fn deserialize<'de, D>(deserializer: D) -> Result<Sha224Sum, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let bytes: &[u8] = serde_bytes::deserialize(deserializer)?;
    Ok(*Sha224Sum::from_slice(bytes))
}

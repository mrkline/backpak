pub mod backend;
pub mod backup;
pub mod cat;
pub mod check;
pub mod chunk;
pub mod file_util;
pub mod hashing;
pub mod index;
pub mod init;
pub mod pack;
pub mod prettify;
pub mod snapshot;
pub mod tree;

pub const DEFAULT_TARGET_SIZE: u64 = 1024 * 1024 * 100; // 100 MB

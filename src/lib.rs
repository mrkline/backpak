pub mod backend;
pub mod blob;
pub mod chunk;
pub mod file_util;
pub mod hashing;
pub mod index;
pub mod pack;
pub mod prettify;
pub mod read;
pub mod snapshot;
pub mod tree;
pub mod upload;

// CLI stuff:
pub mod ui;

pub const DEFAULT_TARGET_SIZE: u64 = 1024 * 1024 * 100; // 100 MB

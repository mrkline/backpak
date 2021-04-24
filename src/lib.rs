pub mod backend;
pub mod backup;
pub mod blob;
pub mod chunk;
pub mod counters;
pub mod diff;
pub mod file_util;
pub mod fs_tree;
pub mod hashing;
pub mod index;
pub mod ls;
pub mod pack;
pub mod prettify;
pub mod read;
pub mod snapshot;
pub mod tree;
pub mod upload;

// CLI stuff:
pub mod ui;

/// The desired size of [pack] files
pub const DEFAULT_TARGET_SIZE: u64 = 1024 * 1024 * 100; // 100 MB

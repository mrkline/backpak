//! Some big dumb backup system.
//!
//! See the [`backup`] module for an overview and a crappy block diagram.

pub mod backend;
pub mod backup;
pub mod blob;
pub mod chunk;
pub mod config;
pub mod counters;
pub mod diff;
pub mod file_util;
pub mod filter;
pub mod fs_tree;
pub mod hashing;
pub mod index;
pub mod ls;
pub mod pack;
pub mod prettify;
pub mod progress;
pub mod rcu;
pub mod read;
pub mod repack;
pub mod snapshot;
pub mod tree;
pub mod upload;

// CLI stuff:
pub mod ui;

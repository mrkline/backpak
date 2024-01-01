//! Performance counters: Count how many times we do various important operations.

use std::sync::atomic::{fence, AtomicUsize, Ordering};

use enum_map::{Enum, EnumMap};
use lazy_static::lazy_static;
use log::*;

#[derive(Debug, Copy, Clone, Enum)]
pub enum Op {
    IndexLoad,
    BackendCacheWrite,
    BackendCacheHit,
    BackendCacheMiss,
    BackendCacheEviction,
    BackendCacheSpill,
    FileToBuffer,
    FileToMmap,
    TreeCacheHit,
    TreeCacheMiss,
    PackSkippedBlob,
    PackStreamRestart,
}

lazy_static! {
    static ref COUNTER_MAP: EnumMap<Op, AtomicUsize> = EnumMap::default();
}

#[inline]
pub fn bump(which: Op) {
    add(which, 1);
}

pub fn add(to: Op, amount: usize) {
    COUNTER_MAP[to].fetch_add(amount, Ordering::Relaxed);
}

pub fn log_counts() {
    // Probably not needed; but we're probably calling this once at program exit.
    fence(Ordering::SeqCst);

    let counts = COUNTER_MAP
        .iter()
        .map(|(k, v)| (k, v.load(Ordering::Relaxed)))
        .filter(|(_k, v)| *v > 0) // Ignore things we didn't do
        .collect::<Vec<_>>();

    if counts.is_empty() {
        return;
    }

    let opname = |op| match op {
        Op::IndexLoad => "indexes loaded",
        Op::BackendCacheWrite => "backend cache writes",
        Op::BackendCacheHit => "backend cache hits",
        Op::BackendCacheMiss => "backend cache misses",
        Op::BackendCacheEviction => "backend cache evictions",
        Op::BackendCacheSpill => "backend cache spills",
        Op::FileToBuffer => "input files buffered",
        Op::FileToMmap => "input files memory mapped",
        Op::TreeCacheHit => "tree cache hits",
        Op::TreeCacheMiss => "tree cache misses",
        Op::PackSkippedBlob => "blobs skipped reading packs",
        Op::PackStreamRestart => "pack read restarts",
    };

    debug!("Counters:");
    for (op, count) in &counts {
        debug!("{:6} {}", count, opname(*op),);
    }
}

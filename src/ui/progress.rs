use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread::park_timeout,
    time::{Duration, Instant},
};

/// Do a thing (presumably draw some progress UI) at the given rate until the exit flag is set.
pub fn periodically<F: FnMut()>(rate: Duration, exit: AtomicBool, mut f: F) {
    loop {
        let start = Instant::now();
        let next = start + rate;

        f();

        // Could we simplify this with a futex_wait on exit?
        // Fork Mara's atomic-wait to fix the cpp-brain on Mac?
        loop {
            if exit.load(Ordering::Acquire) {
                return;
            }
            let now = Instant::now();
            if now >= next {
                break;
            } else {
                // Meanwhile, callers can unpark this guy.
                park_timeout(next - now);
            }
        }
    }
}

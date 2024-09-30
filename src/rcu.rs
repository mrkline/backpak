//! A simple SPMC RCU doodad.
//!
//! Like [genzero](https://crates.io/crates/genzero) but without the TX/RX split.

use std::fmt;
use std::sync::atomic::Ordering;

use crossbeam_epoch::*;

pub struct Rcu<T> {
    inner: Atomic<T>,
}

impl<T: Default> Default for Rcu<T> {
    fn default() -> Self {
        let inner = Atomic::new(T::default());
        Self { inner }
    }
}

impl<T> Drop for Rcu<T> {
    // Lifted from
    // https://docs.rs/crossbeam/latest/crossbeam/epoch/struct.Atomic.html#method.into_owned
    fn drop(&mut self) {
        // If we're being dropped, normal Rust lifetime fun ensures we're the only thread
        // still referencing this.
        unsafe {
            drop(std::mem::replace(&mut self.inner, Atomic::null()).into_owned());
        }
    }
}

impl<T> Rcu<T> {
    pub fn new(v: T) -> Self {
        let inner = Atomic::new(v);
        Self { inner }
    }

    /// Publish a new value.
    pub fn update(&self, v: T) {
        let guard = pin();
        let prev = self.inner.swap(Owned::new(v), Ordering::Release, &guard);
        assert!(!prev.is_null());
        unsafe {
            guard.defer_destroy(prev);
        }
    }

    /// Borrows the current value for as long as you want.
    ///
    /// Just because you *can* hold onto this borrow indefinitely dones't mean you should.
    /// The producer is presumably publishing new versions, making it increasingly stale!
    pub fn borrow(&self) -> Borrow<T> {
        let guard = pin();
        let shared = self.inner.load_consume(&guard).as_raw(); // This one's for Paul.
        assert!(!shared.is_null());
        Borrow {
            _guard: guard,
            shared,
        }
    }
}

pub struct Borrow<T> {
    _guard: Guard,
    // Ideally this would be a Shared,
    // but that depends on the lifetime of the guard, and Rust doesn't like self-reference.
    // SAFETY: the pointer is valid so long as we have the guard (i.e., epoch) we loaded it from.
    shared: *const T,
}

impl<T> std::ops::Deref for Borrow<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: the pointer is valid so long as we hold the guard.
        // Invariant: We don't make a Borrow pointing to null.
        unsafe { self.shared.as_ref().unwrap() }
    }
}

impl<T: fmt::Display> fmt::Display for Borrow<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        fmt::Display::fmt(&**self, f)
    }
}

//! Change the entire program's serialization scheme with this one weird trick!

static mut PRETTIFY: bool = false;

/// Indicate that various fields should be prettified for JSON output.
///
/// We store things compactly: SHA224 as an array of bytes,
/// timestamps as 64 bits of nanoseconds since 1970-01-01, etc.
/// But we want to display them nicely, e.g., as hex and ISO-8601, respectively.
/// Unfortunately, `serde` doesn't have this sort of mechanism out of the box.
///
/// Since the commands that want pretty output don't re-serialize anything to
/// disk (cat, ls, etc.), we can hijack serialize calls to prettify them.
/// Those commands should call `prettify_serialize()` once when starting up;
/// from there the relevant serializers will check `should_prettify()` when
/// writing JSON output.
///
/// # Safety
/// We could make this atomic for thread safety, but there's no need for a bunch
/// of load-acquires for something that's set once at program start before
/// threads get spun up.
pub unsafe fn prettify_serialize() {
    PRETTIFY = true;
}

#[inline]
pub fn should_prettify() -> bool {
    // SAFETY: Callers are trusted to call `prettify_serialize()`
    // once, at the start of a run, before threads are spun up.
    unsafe { PRETTIFY }
}

pub mod date_time {
    use chrono::serde::ts_nanoseconds;
    use chrono::{offset::Utc, DateTime};
    use serde::{Deserializer, Serialize, Serializer};

    pub fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if super::should_prettify() {
            dt.serialize(serializer)
        } else {
            ts_nanoseconds::serialize(dt, serializer)
        }
    }

    pub fn deserialize<'de, D>(d: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        ts_nanoseconds::deserialize(d)
    }
}

pub mod date_time_option {
    use chrono::serde::ts_nanoseconds_option;
    use chrono::{offset::Utc, DateTime};
    use serde::{Deserializer, Serialize, Serializer};

    pub fn serialize<S>(dt: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if super::should_prettify() {
            dt.serialize(serializer)
        } else {
            ts_nanoseconds_option::serialize(dt, serializer)
        }
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        ts_nanoseconds_option::deserialize(d)
    }
}

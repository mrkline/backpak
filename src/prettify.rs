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

pub mod instant {
    use jiff::Timestamp;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(dt: &Timestamp, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if super::should_prettify() {
            dt.serialize(serializer)
        } else {
            (dt.as_nanosecond() as i64).serialize(serializer)
        }
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Timestamp, D::Error>
    where
        D: Deserializer<'de>,
    {
        let i = i64::deserialize(d)?;
        Timestamp::from_nanosecond(i as i128).map_err(serde::de::Error::custom)
    }
}

pub mod instant_option {
    use jiff::Timestamp;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(dt: &Option<Timestamp>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if super::should_prettify() {
            dt.serialize(serializer)
        } else {
            dt.map(|t| t.as_nanosecond() as i64).serialize(serializer)
        }
    }

    pub fn deserialize<'de, D>(d: D) -> Result<Option<Timestamp>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let oi = Option::<i64>::deserialize(d)?;
        oi.map(|i| Timestamp::from_nanosecond(i as i128))
            .transpose()
            .map_err(serde::de::Error::custom)
    }
}

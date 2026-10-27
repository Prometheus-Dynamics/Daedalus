//! `created_at` packed into one `i64` of nanoseconds, so a payload carries its creation time in
//! 8 bytes instead of an `Instant`'s 16. The reading is taken from `Instant` relative to a fixed
//! origin (the first reading packed in this process) and decodes back to the same `Instant`.

use daedalus_core::platform::Instant;

#[cfg(all(
    feature = "std",
    not(all(target_family = "wasm", target_os = "unknown"))
))]
mod imp {
    use core::time::Duration;
    use std::sync::OnceLock;

    use daedalus_core::platform::Instant;

    fn origin() -> Instant {
        static ORIGIN: OnceLock<Instant> = OnceLock::new();
        *ORIGIN.get_or_init(Instant::now)
    }

    pub(super) fn encode(at: Instant) -> i64 {
        let origin = origin();
        // A signed offset from the origin: readings before it encode as negative nanoseconds.
        if at >= origin {
            i64::try_from(at.duration_since(origin).as_nanos()).unwrap_or(i64::MAX)
        } else {
            -i64::try_from(origin.duration_since(at).as_nanos()).unwrap_or(i64::MAX)
        }
    }

    pub(super) fn decode(nanos: i64) -> Instant {
        let origin = origin();
        if nanos >= 0 {
            origin + Duration::from_nanos(nanos as u64)
        } else {
            origin - Duration::from_nanos(nanos.unsigned_abs())
        }
    }
}

#[cfg(not(all(
    feature = "std",
    not(all(target_family = "wasm", target_os = "unknown"))
)))]
mod imp {
    use core::time::Duration;

    use daedalus_core::platform::Instant;

    pub(super) fn encode(at: Instant) -> i64 {
        i64::try_from(at.saturating_duration_since(Instant::default()).as_nanos())
            .unwrap_or(i64::MAX)
    }

    pub(super) fn decode(nanos: i64) -> Instant {
        Instant::default() + Duration::from_nanos(nanos.max(0) as u64)
    }
}

/// `at` packed as signed nanoseconds from the process origin.
#[inline]
pub(super) fn encode(at: Instant) -> i64 {
    imp::encode(at)
}

/// The `Instant` a packed reading stands for.
#[inline]
pub(super) fn decode(nanos: i64) -> Instant {
    imp::decode(nanos)
}

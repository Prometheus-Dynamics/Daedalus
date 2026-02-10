use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Stable, deterministic 128-bit id derived from `(domain, value)`.
///
/// This is meant for runtime hot paths (e.g. handler dispatch) where string-keyed maps
/// are too expensive. We also use it as a collision detector: registries should
/// refuse to register two distinct strings that map to the same id.
pub fn stable_id128(domain: &str, value: &str) -> u128 {
    fn hash64(domain: &str, value: &str, salt: u64) -> u64 {
        let mut h = DefaultHasher::new();
        domain.hash(&mut h);
        salt.hash(&mut h);
        value.hash(&mut h);
        h.finish()
    }

    let a = hash64(domain, value, 0);
    let b = hash64(domain, value, 1);
    ((a as u128) << 64) | (b as u128)
}


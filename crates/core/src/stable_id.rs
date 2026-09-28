const FNV1A64_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

/// 64-bit FNV-1a: tiny, dependency-free, and stable across toolchains and platforms.
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    fnv1a64_extend(FNV1A64_OFFSET, bytes)
}

/// Continue an FNV-1a hash `hash` with `bytes` (hashing the concatenation of several slices).
fn fnv1a64_extend(hash: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(hash, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV1A64_PRIME)
    })
}

/// Stable, deterministic 128-bit id derived from `(domain, value)`.
///
/// This is meant for runtime hot paths (e.g. handler dispatch) where string-keyed maps
/// are too expensive. We also use it as a collision detector: registries should
/// refuse to register two distinct strings that map to the same id.
pub fn stable_id128(domain: &str, value: &str) -> u128 {
    let half = |salt: u64| {
        [
            &salt.to_le_bytes()[..],
            domain.as_bytes(),
            &[0xff],
            value.as_bytes(),
        ]
        .into_iter()
        .fold(FNV1A64_OFFSET, fnv1a64_extend)
    };
    (u128::from(half(0)) << 64) | u128::from(half(1))
}

#[cfg(test)]
mod tests {
    use super::{fnv1a64, stable_id128};

    #[test]
    fn fnv1a64_matches_reference_vectors() {
        assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a64(b"foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    fn stable_ids_are_pinned() {
        assert_eq!(
            stable_id128("node", "demo.add"),
            0x7a7d_d50d_46b1_67a4_f2d5_f544_da9f_1993
        );
        assert_eq!(
            stable_id128("node", "io.host_bridge"),
            0x4c63_02d1_8f37_873e_6b9a_c261_8d54_9425
        );
        assert_eq!(
            stable_id128("plugin", "daedalus.builtin.primitive_types"),
            0xd68a_1efc_ba14_c374_76ff_8dfe_fbe3_1803
        );
    }

    #[test]
    fn domain_is_part_of_stable_id() {
        assert_ne!(stable_id128("node", "same"), stable_id128("plugin", "same"));
    }
}

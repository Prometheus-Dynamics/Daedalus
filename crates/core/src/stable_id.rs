/// Stable, deterministic 128-bit id derived from `(domain, value)`.
///
/// This is meant for runtime hot paths (e.g. handler dispatch) where string-keyed maps
/// are too expensive. We also use it as a collision detector: registries should
/// refuse to register two distinct strings that map to the same id.
pub fn stable_id128(domain: &str, value: &str) -> u128 {
    fn fnv1a64(salt: u64, domain: &str, value: &str) -> u64 {
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;

        let mut hash = FNV_OFFSET;
        for byte in salt
            .to_le_bytes()
            .iter()
            .copied()
            .chain(domain.bytes())
            .chain([0xff])
            .chain(value.bytes())
        {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    let a = fnv1a64(0, domain, value);
    let b = fnv1a64(1, domain, value);
    ((a as u128) << 64) | (b as u128)
}

#[cfg(test)]
mod tests {
    use super::stable_id128;

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

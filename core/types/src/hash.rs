//! Hashes that stay the same across toolchains and platforms.

const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const PRIME: u64 = 0x0000_0100_0000_01b3;

pub(crate) const fn fnv1a64_extend(mut hash: u64, bytes: &[u8]) -> u64 {
    let mut i = 0;
    while i < bytes.len() {
        hash = (hash ^ bytes[i] as u64).wrapping_mul(PRIME);
        i += 1;
    }
    hash
}

pub(crate) const FNV1A64_OFFSET: u64 = OFFSET;

/// 64-bit FNV-1a.
pub const fn fnv1a64(bytes: &[u8]) -> u64 {
    fnv1a64_extend(OFFSET, bytes)
}

const fn half(salt: u64, domain: &str, value: &str) -> u64 {
    let hash = fnv1a64_extend(OFFSET, &salt.to_le_bytes());
    let hash = fnv1a64_extend(hash, domain.as_bytes());
    let hash = fnv1a64_extend(hash, &[0xff]);
    fnv1a64_extend(hash, value.as_bytes())
}

/// A stable 128-bit id derived from `(domain, value)`.
pub const fn stable_id128(domain: &str, value: &str) -> u128 {
    (half(0, domain, value) as u128) << 64 | half(1, domain, value) as u128
}

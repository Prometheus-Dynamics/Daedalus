//! The checksum a stored plan ends with.

/// CRC-32 (IEEE 802.3, the zlib checksum). Bitwise, not table-driven: a table costs 1 KiB of flash,
/// and a plan is only checked once per load.
pub const fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    let mut at = 0;
    while at < bytes.len() {
        crc ^= bytes[at] as u32;
        let mut bit = 0;
        while bit < 8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
            bit += 1;
        }
        at += 1;
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::crc32;

    #[test]
    fn matches_the_published_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b""), 0);
    }
}

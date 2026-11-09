//! Dirty and carry bits: one bit per step, in `u32` words sized at load.

/// How many `u32` words hold `steps` bits.
pub(crate) const fn words_for(steps: usize) -> usize {
    steps.div_ceil(32)
}

pub(crate) fn get(words: &[u32], index: usize) -> bool {
    words[index / 32] & (1 << (index % 32)) != 0
}

pub(crate) fn set(words: &mut [u32], index: usize) {
    words[index / 32] |= 1 << (index % 32);
}

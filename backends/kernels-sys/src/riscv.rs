//! RISC-V extension detection at runtime through the kernel's `riscv_hwprobe` syscall (stable
//! Rust has no `is_riscv_feature_detected!`). Linux only; the raw `ecall` keeps it `no_std`.

use core::sync::atomic::{AtomicU64, Ordering};

const SYS_RISCV_HWPROBE: usize = 258;
const KEY_IMA_EXT_0: i64 = 4;
/// `RISCV_HWPROBE_EXT_ZVBC` in `RISCV_HWPROBE_KEY_IMA_EXT_0`.
pub(crate) const EXT_ZVBC: u64 = 1 << 18;

/// `RISCV_HWPROBE_KEY_IMA_EXT_0` for all CPUs, cached; 0 when the kernel has no hwprobe.
pub(crate) fn ima_ext_0() -> u64 {
    // Bit 63 marks the cached value as known.
    static CACHE: AtomicU64 = AtomicU64::new(0);
    let cached = CACHE.load(Ordering::Relaxed);
    if cached & (1 << 63) != 0 {
        return cached & !(1 << 63);
    }
    let mut pair: [u64; 2] = [KEY_IMA_EXT_0 as u64, 0];
    let ret: isize;
    // SAFETY: riscv_hwprobe(pairs, 1, 0, NULL, 0) writes one key/value pair into `pair`.
    unsafe {
        core::arch::asm!(
            "ecall",
            inlateout("a0") pair.as_mut_ptr() as isize => ret,
            in("a1") 1usize,
            in("a2") 0usize,
            in("a3") 0usize,
            in("a4") 0usize,
            in("a7") SYS_RISCV_HWPROBE,
            options(nostack),
        );
    }
    // The kernel sets the key to -1 when it does not know it.
    let value = if ret == 0 && pair[0] as i64 == KEY_IMA_EXT_0 {
        pair[1]
    } else {
        0
    };
    CACHE.store(value | (1 << 63), Ordering::Relaxed);
    value
}

/// Whether every CPU has Zvbc (vector carry-less multiply).
pub(crate) fn has_zvbc() -> bool {
    ima_ext_0() & EXT_ZVBC != 0
}

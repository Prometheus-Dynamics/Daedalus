//! Stack high-water mark on Cortex-M: paint the free stack with a pattern, run, and find the
//! deepest word the run overwrote. Uses `cortex-m-rt`'s `_stack_start` (the top) and `__sheap`
//! (the end of static data, where the stack's growth room ends).

const PAINT: u32 = 0xA5A5_A5A5;
/// Words left unpainted below the current stack pointer, for the painting function's own frame.
const MARGIN: usize = 64;

// SAFETY: both are symbols of cortex-m-rt's linker script, and only their addresses are taken.
#[allow(non_upper_case_globals)]
unsafe extern "C" {
    static _stack_start: u32;
    static __sheap: u32;
}

fn bounds() -> (usize, usize) {
    // Linker symbols: only their addresses are taken.
    (
        core::ptr::addr_of!(__sheap) as usize,
        core::ptr::addr_of!(_stack_start) as usize,
    )
}

/// Paint the stack below the current frame. Call from `main` before a run, with nothing else
/// using the stack (interrupts that do would be counted, which is what a measurement wants).
pub fn paint() {
    let (bottom, _) = bounds();
    let sp = cortex_m::register::msp::read() as usize;
    let mut at = bottom;
    while at + 4 * MARGIN < sp {
        // SAFETY: `[bottom, sp - margin)` is free stack: below every live frame, above statics.
        unsafe { core::ptr::write_volatile(at as *mut u32, PAINT) };
        at += 4;
    }
}

/// Bytes of stack used since [`paint`]: from the top down to the deepest overwritten word.
pub fn used() -> usize {
    let (bottom, top) = bounds();
    let mut at = bottom;
    // SAFETY: reads within the stack region `[bottom, top)`.
    while at < top && unsafe { core::ptr::read_volatile(at as *const u32) } == PAINT {
        at += 4;
    }
    top - at
}

//! The timed sections of the tool. Each prints plain lines: what was measured, its unit, and the
//! median of the timed runs after a warm-up. A section that cannot run on the device says why.

pub mod allocations;
pub mod chains;
pub mod fences;
pub mod gaps;
#[cfg(target_os = "linux")]
pub mod images;
pub mod transfers;

use std::time::{Duration, Instant};

/// Runs before the timed ones, so pipelines, buffers and caches are built.
pub const WARM: usize = 5;
/// Timed runs for each measurement, and the median is what is printed.
pub const RUNS: usize = 50;
pub const MIB: u64 = 1 << 20;

/// The median of `runs` timed calls of `body`, after `warm` untimed calls.
pub fn median(warm: usize, runs: usize, mut body: impl FnMut()) -> Duration {
    for _ in 0..warm {
        body();
    }
    let mut times: Vec<Duration> = (0..runs)
        .map(|_| {
            let start = Instant::now();
            body();
            start.elapsed()
        })
        .collect();
    median_of(&mut times)
}

/// The median of `times`, which is sorted in place.
pub fn median_of(times: &mut [Duration]) -> Duration {
    times.sort_unstable();
    times.get(times.len() / 2).copied().unwrap_or_default()
}

pub fn ms(time: Duration) -> f64 {
    time.as_secs_f64() * 1e3
}

pub fn us(time: Duration) -> f64 {
    time.as_secs_f64() * 1e6
}

/// `len` bytes of a fixed xorshift stream, so every run and every board sees the same input.
pub fn pattern(len: usize, seed: u32) -> Vec<u8> {
    let mut x = seed.wrapping_mul(2_654_435_761) | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            x as u8
        })
        .collect()
}

/// A buffer size that the device takes: a whole word, and never empty.
pub fn word_size(len: u64) -> u64 {
    len.next_multiple_of(4).max(4)
}

/// The median of `runs` timed calls of `body`, each given what `setup` made untimed just before.
pub fn median_with_setup<T>(
    warm: usize,
    runs: usize,
    mut setup: impl FnMut() -> T,
    mut body: impl FnMut(T),
) -> Duration {
    for _ in 0..warm {
        body(setup());
    }
    let mut times: Vec<Duration> = (0..runs)
        .map(|_| {
            let input = setup();
            let start = Instant::now();
            body(input);
            start.elapsed()
        })
        .collect();
    median_of(&mut times)
}

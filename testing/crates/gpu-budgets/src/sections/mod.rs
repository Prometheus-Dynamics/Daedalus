//! The timed sections of the tool. Each prints plain lines: what was measured, its unit, and the
//! median of the timed runs after a warm-up. A section that cannot run on the device says why.

pub mod allocations;
#[cfg(target_os = "linux")]
pub mod baselines;
pub mod chains;
pub mod fences;
pub mod gaps;
pub mod grind;
pub mod region_batch;
pub mod timings;
pub mod transfers;

use std::time::{Duration, Instant};

use daedalus_wgpu::{Options, WgpuBackend};

/// Runs before the timed ones, so pipelines, buffers and caches are built.
pub(crate) const WARM: usize = 5;
/// Timed runs for each measurement, and the median is what is printed.
pub const RUNS: usize = 50;
pub(crate) const MIB: u64 = 1 << 20;

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
pub(crate) fn word_size(len: u64) -> u64 {
    len.next_multiple_of(4).max(4)
}

/// The median of `runs` timed calls of `body`, each given what `setup` made untimed just before.
pub(crate) fn median_with_setup<T>(
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

/// The figures a timed measure prints: the median, the 99th percentile and the best of its runs.
#[derive(Clone, Copy, Debug)]
pub struct Stats {
    pub runs: usize,
    pub median: Duration,
    pub p99: Duration,
    pub best: Duration,
}

/// The stats of `times`, which is sorted in place. The p99 is the run at or above which one run
/// in a hundred falls, so with fewer than 100 runs it is the slowest run.
pub fn summary(times: &mut [Duration]) -> Stats {
    times.sort_unstable();
    let last = times.len().saturating_sub(1);
    let p99 = times[(times.len() * 99).div_ceil(100).saturating_sub(1).min(last)];
    Stats {
        runs: times.len(),
        median: median_of(times),
        p99,
        best: times.first().copied().unwrap_or_default(),
    }
}

/// `warm` untimed calls of `body`, then `runs` timed ones, summarised.
pub fn measure(warm: usize, runs: usize, body: impl FnMut()) -> Stats {
    let mut times = timed(warm, runs, body);
    summary(&mut times)
}

/// The times of `runs` calls of `body`, after `warm` untimed ones.
pub fn timed(warm: usize, runs: usize, mut body: impl FnMut()) -> Vec<Duration> {
    for _ in 0..warm {
        body();
    }
    (0..runs)
        .map(|_| {
            let start = Instant::now();
            body();
            start.elapsed()
        })
        .collect()
}

/// The figures of `stats` on one line, in milliseconds: `median 1.234 ms (p99 1.5, best 1.1, n 50)`.
pub fn line_ms(stats: Stats) -> String {
    format!(
        "median {:.4} ms (p99 {:.4}, best {:.4}, n {})",
        ms(stats.median),
        ms(stats.p99),
        ms(stats.best),
        stats.runs
    )
}

/// A device of its own on the shared device's adapter, with `tune` applied to its options.
/// `None` when there is no device to open.
pub fn device_with(tune: impl FnOnce(Options) -> Options) -> Option<WgpuBackend> {
    let shared = WgpuBackend::open(Options::default());
    let record = shared.start().ok()?;
    let backend = WgpuBackend::open(tune(Options {
        adapter: Some(record.adapter.name),
        ..Options::default()
    }));
    backend.start().ok()?;
    Some(backend)
}

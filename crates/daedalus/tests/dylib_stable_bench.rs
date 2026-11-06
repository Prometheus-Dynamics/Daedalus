//! Timing of the stable handler path against the static install, for the cost per constant, a
//! `Detections` crossing through `Value` and as a plugin-local handle, and a 12-stage handle chain
//! (`examples/plugins/stable_bench`). Each case alternates the two installs within a run, so cache
//! and frequency drift hit both; the output is one `BENCH` line per case and path, and a `fit`
//! line per constant series.
//!
//! `cargo test -p daedalus-rs --features engine,dylib-plugins --release --test dylib_stable_bench
//! -- --ignored --nocapture`. A board run names prebuilt cdylibs with
//! `DAEDALUS_CDYLIB_DAEDALUS_PLUGINS_STABLE_BENCH` (see `support/cdylib.rs`).
// musl executables are static and cannot dlopen (see `dylib::library`): no loader there.
#![cfg(all(unix, not(target_env = "musl")))]

#[path = "support/stable_bench.rs"]
mod bench;
#[path = "support/cdylib.rs"]
mod cdylib;
#[path = "support/frames.rs"]
mod frames;

use std::time::Instant;

use daedalus::PluginLibrary;
use daedalus::runtime::plugins::PluginRegistry;

use bench::{Host, Step, stable_registry, static_registry};

/// Interleaved samples per case and path (odd, so the p50 is a middle sample). Each sample is a
/// few milliseconds, so a burst of load from elsewhere lands on both installs alike.
const REPEATS: usize = 101;
const WARM_UP: usize = 300;

/// Interleaved samples of one case, per path: `[static, stable]` in ns per tick.
fn measure(
    name: &str,
    build: &dyn Fn(&PluginRegistry) -> (Host, Step),
    ticks: usize,
    statics: &PluginRegistry,
    stables: &PluginRegistry,
) -> [Vec<f64>; 2] {
    let (mut static_host, mut static_step) = build(statics);
    let (mut stable_host, mut stable_step) = build(stables);
    let (expected, got) = (static_step(&mut static_host), stable_step(&mut stable_host));
    assert_eq!(
        expected, got,
        "{name}: the stable path returns what the static install does"
    );
    for _ in 0..WARM_UP {
        std::hint::black_box(static_step(&mut static_host));
        std::hint::black_box(stable_step(&mut stable_host));
    }
    let mut samples = [Vec::with_capacity(REPEATS), Vec::with_capacity(REPEATS)];
    for repeat in 0..REPEATS {
        let order = if repeat % 2 == 0 { [0, 1] } else { [1, 0] };
        for which in order {
            let start = Instant::now();
            match which {
                0 => (0..ticks).for_each(|_| {
                    std::hint::black_box(static_step(&mut static_host));
                }),
                _ => (0..ticks).for_each(|_| {
                    std::hint::black_box(stable_step(&mut stable_host));
                }),
            }
            samples[which].push(start.elapsed().as_nanos() as f64 / ticks as f64);
        }
    }
    samples
}

fn percentile(samples: &[f64], p: f64) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

/// Least-squares line through `(x, y)`: (slope, intercept).
fn fit(points: &[(f64, f64)]) -> (f64, f64) {
    let n = points.len() as f64;
    let mean_x = points.iter().map(|p| p.0).sum::<f64>() / n;
    let mean_y = points.iter().map(|p| p.1).sum::<f64>() / n;
    let (mut num, mut den) = (0.0, 0.0);
    for (x, y) in points {
        num += (x - mean_x) * (y - mean_y);
        den += (x - mean_x) * (x - mean_x);
    }
    let slope = num / den;
    (slope, mean_y - slope * mean_x)
}

#[test]
#[ignore = "timing; run manually"]
#[allow(clippy::print_stdout)]
fn stable_bench_overhead() {
    let path = cdylib::build_cdylib(
        "daedalus-plugins-stable-bench",
        "daedalus_plugins_stable_bench",
        "dylib",
    );
    // Safety: a trusted test plugin, kept loaded for the process.
    let library: &'static PluginLibrary =
        Box::leak(Box::new(unsafe { PluginLibrary::load(&path) }.unwrap()));
    let (statics, stables) = (static_registry(), stable_registry(library));

    // (group, case, ticks per sample, builder)
    type Build = Box<dyn Fn(&PluginRegistry) -> (Host, Step)>;
    let mut cases: Vec<(&str, String, usize, Build)> = Vec::new();
    for (mixed, variant) in [(false, "scalars"), (true, "quarter_strings")] {
        for n in [0, 8, 16, 49] {
            if n == 0 && mixed {
                continue;
            }
            cases.push((
                "constants",
                format!("{variant} n={n}"),
                500,
                Box::new(move |registry: &PluginRegistry| bench::config_case(registry, mixed, n)),
            ));
        }
    }
    for tags in [0_i64, 4, 16] {
        cases.push((
            "detections_value",
            format!("Detections through Value, tags={tags}"),
            200,
            Box::new(move |registry: &PluginRegistry| {
                bench::detections_case(registry, false, tags)
            }),
        ));
        cases.push((
            "detections_handle",
            format!("Detections as a plugin-local handle, tags={tags}"),
            200,
            Box::new(move |registry: &PluginRegistry| bench::detections_case(registry, true, tags)),
        ));
    }
    for constants in [false, true] {
        cases.push((
            "staged",
            format!(
                "12 stages, handle in/out + frame, {} consts",
                if constants { 10 } else { 0 }
            ),
            60,
            Box::new(move |registry: &PluginRegistry| bench::staged_case(registry, constants)),
        ));
    }

    // Constant series for the fit: (variant, path) -> (n, p50) points.
    let mut series: Vec<(&str, &str, f64, f64)> = Vec::new();
    for (group, name, ticks, build) in &cases {
        let [base, stable] = measure(name, build.as_ref(), *ticks, &statics, &stables);
        for (path, samples) in [("static", &base), ("stable", &stable)] {
            let (p50, p99) = (percentile(samples, 0.5), percentile(samples, 0.99));
            println!(
                "BENCH group={group} case=\"{name}\" path={path} p50_ns={p50:.0} p99_ns={p99:.0} \
                 repeats={REPEATS} ticks={ticks}"
            );
            if *group == "constants" {
                let n: f64 = name.rsplit("n=").next().unwrap().parse().unwrap();
                let variant = if name.starts_with("scalars") {
                    "scalars"
                } else {
                    "quarter_strings"
                };
                series.push((variant, path, n, p50));
            }
        }
    }
    // The n=0 baseline belongs to both variants.
    for variant in ["scalars", "quarter_strings"] {
        for path in ["static", "stable"] {
            let zero = series
                .iter()
                .find(|s| s.0 == "scalars" && s.1 == path && s.2 == 0.0)
                .map(|s| s.3)
                .unwrap();
            let points: Vec<(f64, f64)> = series
                .iter()
                .filter(|s| s.0 == variant && s.1 == path)
                .map(|s| (s.2, s.3))
                .chain((variant != "scalars").then_some((0.0, zero)))
                .collect();
            let (slope, intercept) = fit(&points);
            println!(
                "BENCH group=constants fit variant={variant} path={path} \
                 slope_ns_per_constant={slope:.1} intercept_ns={intercept:.0}"
            );
        }
    }
}

/// Hardware counters for the zero-constant rows, opt-in: `DAEDALUS_BENCH_COUNTERS=1`. It prints one
/// `COUNTERS` line per repeat and path (static, stable) with the per-call values. Without the env
/// var it returns at once, so the timing run above is unchanged.
#[test]
#[ignore = "timing; run manually with DAEDALUS_BENCH_COUNTERS=1"]
#[allow(clippy::print_stdout)]
fn stable_bench_counters() {
    if std::env::var_os("DAEDALUS_BENCH_COUNTERS").is_none() {
        return;
    }
    let path = cdylib::build_cdylib(
        "daedalus-plugins-stable-bench",
        "daedalus_plugins_stable_bench",
        "dylib",
    );
    // Safety: a trusted test plugin, kept loaded for the process.
    let library: &'static PluginLibrary =
        Box::leak(Box::new(unsafe { PluginLibrary::load(&path) }.unwrap()));
    let (statics, stables) = (static_registry(), stable_registry(library));
    let mut counters =
        match daedalus_bench_counters::Counters::new(&daedalus_bench_counters::Event::ALL) {
            Ok(counters) => counters,
            Err(err) => {
                println!("COUNTERS unavailable: {err}");
                return;
            }
        };
    for missing in counters.missing() {
        println!("COUNTERS missing event={}", missing.name());
    }
    counters_row(
        &mut counters,
        "scalars n=0",
        &|r| bench::config_case(r, false, 0),
        &statics,
        &stables,
    );
    counters_row(
        &mut counters,
        "Detections through Value, tags=0",
        &|r| bench::detections_case(r, false, 0),
        &statics,
        &stables,
    );
}

/// One case, both installs: interleaved repeats of a fixed number of calls each.
#[allow(clippy::print_stdout)]
fn counters_row(
    counters: &mut daedalus_bench_counters::Counters,
    case: &str,
    build: &dyn Fn(&PluginRegistry) -> (Host, Step),
    statics: &PluginRegistry,
    stables: &PluginRegistry,
) {
    const ITERATIONS: u64 = 20_000;
    const COUNTER_REPEATS: usize = 15;
    let (mut static_host, mut static_step) = build(statics);
    let (mut stable_host, mut stable_step) = build(stables);
    for _ in 0..WARM_UP {
        std::hint::black_box(static_step(&mut static_host));
        std::hint::black_box(stable_step(&mut stable_host));
    }
    for repeat in 0..COUNTER_REPEATS {
        let order = if repeat % 2 == 0 { [0, 1] } else { [1, 0] };
        for which in order {
            let (name, result) = if which == 0 {
                (
                    "static",
                    counters.measure(ITERATIONS, || {
                        std::hint::black_box(static_step(&mut static_host));
                    }),
                )
            } else {
                (
                    "stable",
                    counters.measure(ITERATIONS, || {
                        std::hint::black_box(stable_step(&mut stable_host));
                    }),
                )
            };
            match result {
                Ok(sample) => {
                    let value = |e| {
                        sample
                            .per_call(e)
                            .map_or("n/a".to_string(), |v| format!("{v:.2}"))
                    };
                    println!(
                        "COUNTERS case=\"{case}\" path={name} repeat={repeat} iterations={} ns_per_call={:.1} running={:.3} cycles={} instructions={} branch_misses={} l1d_read_misses={} llc_read_misses={}",
                        sample.iterations(),
                        sample.elapsed().as_nanos() as f64 / sample.iterations() as f64,
                        sample.running_fraction(),
                        value(daedalus_bench_counters::Event::Cycles),
                        value(daedalus_bench_counters::Event::Instructions),
                        value(daedalus_bench_counters::Event::BranchMisses),
                        value(daedalus_bench_counters::Event::L1dReadMisses),
                        value(daedalus_bench_counters::Event::LlcReadMisses),
                    );
                }
                Err(err) => {
                    println!("COUNTERS case=\"{case}\" path={name} repeat={repeat} error=\"{err}\"")
                }
            }
        }
    }
}

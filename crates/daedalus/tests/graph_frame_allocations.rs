//! Allocation budget for one frame of a detector-like graph (see `support/detector_graph.rs`).
//!
//! A counting global allocator counts every allocation (all threads, so pool workers count too)
//! while a frame runs. `DAEDALUS_ALLOC_TRACE=1` also captures a backtrace per allocation and
//! prints allocations per frame grouped by the first Daedalus call site:
//!
//! ```text
//! DAEDALUS_ALLOC_TRACE=1 cargo test -p daedalus-rs --features engine,plugins \
//!     --test graph_frame_allocations -- --nocapture --test-threads 1
//! ```
// The allocation report is this test's output.
#![allow(clippy::print_stdout)]

#[path = "support/detector_graph.rs"]
mod detector_graph;

use std::alloc::{GlobalAlloc, Layout, System};
use std::backtrace::Backtrace;
use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use daedalus::engine::{MetricsLevel, RuntimeMode};
use detector_graph::{Frame, compile, drive_frame};

struct CountingAlloc;

static ARMED: AtomicBool = AtomicBool::new(false);
static TRACE: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static TRACES: Mutex<Vec<Backtrace>> = Mutex::new(Vec::new());
/// One measurement at a time: the counter is process-wide.
static SERIAL: Mutex<()> = Mutex::new(());

thread_local! {
    static IN_TRACE: Cell<bool> = const { Cell::new(false) };
}

fn record() {
    if !ARMED.load(Ordering::Relaxed) {
        return;
    }
    let reentrant = IN_TRACE.try_with(Cell::get).unwrap_or(true);
    if reentrant {
        return;
    }
    ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    if TRACE.load(Ordering::Relaxed) {
        IN_TRACE.with(|flag| flag.set(true));
        let trace = Backtrace::force_capture();
        TRACES.lock().unwrap_or_else(|e| e.into_inner()).push(trace);
        IN_TRACE.with(|flag| flag.set(false));
    }
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record();
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record();
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

const WARMUP_FRAMES: usize = 16;
const MEASURED_FRAMES: usize = 20;

/// Allocations per frame for `mode`/`metrics`, after warm-up, plus the per-call-site breakdown
/// when tracing.
fn allocations_per_frame(mode: RuntimeMode, metrics: MetricsLevel) -> f64 {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let mut host = compile(mode.clone(), metrics).expect("compile detector graph");
    let frame = Arc::new(Frame::new(7));
    for _ in 0..WARMUP_FRAMES {
        assert!(drive_frame(&mut host, &frame).is_some(), "warm-up report");
    }
    let trace = std::env::var_os("DAEDALUS_ALLOC_TRACE").is_some();
    TRACE.store(trace, Ordering::SeqCst);
    ALLOCATIONS.store(0, Ordering::SeqCst);
    ARMED.store(true, Ordering::SeqCst);
    let mut reports = 0;
    for _ in 0..MEASURED_FRAMES {
        reports += usize::from(drive_frame(&mut host, &frame).is_some());
    }
    ARMED.store(false, Ordering::SeqCst);
    TRACE.store(false, Ordering::SeqCst);
    assert_eq!(reports, MEASURED_FRAMES, "every frame produces a report");
    let per_frame = ALLOCATIONS.load(Ordering::SeqCst) as f64 / MEASURED_FRAMES as f64;
    println!("{mode:?}/{metrics:?}: {per_frame:.1} allocations per frame");
    if trace {
        print_call_sites();
    }
    per_frame
}

/// Group captured backtraces by their first Daedalus frame and print counts per frame.
fn print_call_sites() {
    let traces = std::mem::take(&mut *TRACES.lock().unwrap_or_else(|e| e.into_inner()));
    let mut sites: BTreeMap<String, usize> = BTreeMap::new();
    for trace in &traces {
        *sites.entry(call_site(&trace.to_string())).or_default() += 1;
    }
    let mut sites: Vec<_> = sites.into_iter().collect();
    sites.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    for (site, count) in sites {
        println!("  {:>6.2}  {site}", count as f64 / MEASURED_FRAMES as f64);
    }
}

/// The first Daedalus frame as `symbol (file:line)`, then its Daedalus callers (this harness
/// excluded) up to the handler or executor that triggered the allocation.
fn call_site(trace: &str) -> String {
    let mut chain: Vec<String> = Vec::new();
    let mut lines = trace.lines().map(str::trim).peekable();
    while let Some(line) = lines.next() {
        let Some((_, symbol)) = line.split_once(": ") else {
            continue;
        };
        let generated = symbol.contains("detector_graph");
        // Trait impls print as `<daedalus_runtime::... as ...>::f` or `<daedalus_...>::f`.
        let daedalus = symbol.starts_with("daedalus") || symbol.starts_with("<daedalus");
        if !(daedalus || generated) {
            continue;
        }
        let symbol = match symbol.rsplit_once("::h") {
            Some((name, hash)) if hash.chars().all(|c| c.is_ascii_hexdigit()) => name,
            _ => symbol,
        };
        if chain.is_empty() {
            let location = lines
                .peek()
                .and_then(|next| next.strip_prefix("at "))
                .map(|at| at.rsplit("/crates/").next().unwrap_or(at))
                .unwrap_or_default();
            chain.push(format!("{symbol} ({location})"));
        } else if chain.len() < 4 && !chain.iter().any(|c| c.starts_with(symbol)) {
            chain.push(symbol.to_string());
        }
        if generated {
            break;
        }
    }
    if chain.is_empty() {
        return "<outside daedalus>".to_string();
    }
    chain.join("\n            <- ")
}

/// Payloads one frame creates, one `Arc` each (the value's storage): six node outputs (`stats`,
/// `roi`, `histogram`, `track`, `report`, and `refine`'s `Option<Roi>`) and two adapter results
/// (the `FrameInfo` of each `frame_info` edge). The primitive outputs (`count`, `exposure`,
/// `sharpness`, `quality`) are held inline, `preview` and the merged detections are `Arc`s the
/// node already holds, and `exposure` borrows its `String` constant (`&String`).
const PAYLOAD_ALLOCATIONS: f64 = 6.0 + 2.0;
/// Budget per frame, every mode and every metrics level. Lower it, never raise it.
const FRAME_BUDGET: f64 = PAYLOAD_ALLOCATIONS;

/// Every metrics level, from `Off` up to `Trace`.
const METRICS_LEVELS: [MetricsLevel; 7] = [
    MetricsLevel::Off,
    MetricsLevel::Basic,
    MetricsLevel::Timing,
    MetricsLevel::Detailed,
    MetricsLevel::Hardware,
    MetricsLevel::Profile,
    MetricsLevel::Trace,
];

#[test]
fn detector_graph_frame_allocation_budget() {
    // The metrics feature is on for this test binary (the dev-dependency on `daedalus-runtime`
    // with `metrics`), so the levels above `Off` record, and allocate, as they do in `engine-full`.
    for mode in [
        RuntimeMode::Serial,
        RuntimeMode::Parallel,
        RuntimeMode::Adaptive,
    ] {
        for metrics in METRICS_LEVELS {
            let per_frame = allocations_per_frame(mode.clone(), metrics);
            assert!(
                per_frame <= FRAME_BUDGET,
                "{mode:?}/{metrics:?}: {per_frame} allocations per frame, budget {FRAME_BUDGET}"
            );
        }
    }
}

#[test]
fn detector_graph_outputs_match_across_modes() {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let reports = |mode: RuntimeMode| {
        let mut host = compile(mode, MetricsLevel::Off).expect("compile detector graph");
        (0..8u8)
            .map(|seed| drive_frame(&mut host, &Arc::new(Frame::new(seed))))
            .collect::<Vec<_>>()
    };
    let serial = reports(RuntimeMode::Serial);
    assert!(serial.iter().all(Option::is_some), "every frame reports");
    assert_eq!(reports(RuntimeMode::Parallel), serial);
    assert_eq!(reports(RuntimeMode::Adaptive), serial);
}

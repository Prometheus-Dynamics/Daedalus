// The report is this example's output.
#![allow(clippy::print_stdout)]

mod graph;
mod stages;

use std::sync::Arc;
use std::time::Instant;

use daedalus::data::daedalus_type::DaedalusTypeExpr as _;
use daedalus::{
    engine::{Engine, EngineConfig, HostGraph, MetricsLevel, RuntimeMode},
    runtime::handler_registry::HandlerRegistry,
    transport::Payload,
};
use daedalus_frame_bench::{
    ALL_EVENTS, CacheCounters, CountingAllocator, FrameFeed, FrameSourceConfig, IpSampler,
    SyntheticFrameSource, env_usize,
};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator::system();

type Error = Box<dyn std::error::Error + Send + Sync>;

/// Node id prefix of the mirror plugin.
pub(crate) const PLUGIN: &str = "eidos";
/// Host inputs and outputs, as the live graph names them (`edges.txt`).
pub(crate) const FRAME: &str = "frame";
pub(crate) const CAMERA: &str = "camera";
pub(crate) const EXTRINSICS: &str = "extrinsics";
pub(crate) const HOST_OUTPUTS: [&str; 6] = [
    "detections",
    "rejected",
    "refined_corners",
    "poses",
    "pose_solutions",
    "multi_tag_pose",
];
/// Frame-overhead window, as on the live engine (`window 512`).
pub(crate) const WINDOW: usize = 512;
/// The `Blob` value type's key (its `#[type_key]` below).
pub(crate) const BLOB_KEY: &str = "daedalus.frame_bench.mirror:blob";
/// Stub outputs carry this much data, refilled in place.
pub(crate) const STUB_ITEMS: [f32; 4] = [0.0; 4];

use graph::{mirror_graph, mirror_registry};
use stages::Blob;

/// Compile and prepare the mirror at `level`, with frame-overhead recording on or off.
fn compile(level: MetricsLevel, record: bool) -> Result<HostGraph<HandlerRegistry>, Error> {
    let registry = mirror_registry()?;
    let graph = mirror_graph(&registry)?;
    let mut config = EngineConfig::default()
        .with_metrics_level(level)
        .with_runtime_mode(RuntimeMode::Serial)
        .with_host_event_recording(false);
    if record {
        config = config.with_frame_overhead(WINDOW);
    }
    let mut host = Engine::new(config)?.compile_registry(&registry, graph)?;
    host.prepare()?;
    Ok(host)
}

/// One measured run: per-tick wall time, instructions and allocations, and the overhead report.
struct Measured {
    tick_p50_ns: u64,
    tick_mean_ns: f64,
    /// Per tick, per hardware event in `ALL_EVENTS` order; `None` where the kernel refuses it.
    hardware_per_tick: Vec<(&'static str, Option<f64>)>,
    allocs_per_tick: Option<[f64; 4]>,
    report: Option<daedalus::runtime::executor::FrameOverheadReport>,
}

fn measure(
    level: MetricsLevel,
    record: bool,
    ticks: usize,
    warmup: usize,
) -> Result<Measured, Error> {
    let mut host = compile(level, record)?;
    let mut source = SyntheticFrameSource::new(FrameSourceConfig {
        feed: FrameFeed::Interface,
        ..FrameSourceConfig::default()
    })?;
    let camera = Payload::shared(Blob::TYPE_KEY, Arc::new(Blob::default()));
    let extrinsics = Payload::shared(Blob::TYPE_KEY, Arc::new(Blob::default()));
    let frame_port = daedalus::runtime::PortId::new(FRAME);
    let camera_port = daedalus::runtime::PortId::new(CAMERA);
    let extrinsics_port = daedalus::runtime::PortId::new(EXTRINSICS);
    let mut tick = |host: &mut HostGraph<HandlerRegistry>| -> Result<(), Error> {
        host.push_payload(frame_port.clone(), source.next_payload());
        host.push_payload(camera_port.clone(), camera.clone());
        host.push_payload(extrinsics_port.clone(), extrinsics.clone());
        host.tick()?;
        for output in HOST_OUTPUTS {
            while host.take_payload(output).is_some() {}
        }
        Ok(())
    };
    for _ in 0..warmup {
        tick(&mut host)?;
    }
    host.reset_frame_overhead();
    let mut wall = vec![0u64; ticks];
    let before = daedalus::alloc_probe::counts();
    let counters = CacheCounters::start(&ALL_EVENTS);
    for slot in wall.iter_mut() {
        let start = Instant::now();
        tick(&mut host)?;
        *slot = start.elapsed().as_nanos() as u64;
    }
    let hardware = counters.read();
    let during = daedalus::alloc_probe::counts().since(&before);
    let mean = wall.iter().sum::<u64>() as f64 / ticks as f64;
    wall.sort_unstable();
    let per = |count: u64| count as f64 / ticks as f64;
    Ok(Measured {
        tick_p50_ns: wall[(ticks / 2).min(ticks - 1)],
        tick_mean_ns: mean,
        hardware_per_tick: hardware
            .into_iter()
            .map(|(name, count)| (name, count.map(|count| count as f64 / ticks as f64)))
            .collect(),
        allocs_per_tick: daedalus::alloc_probe::is_installed().then(|| {
            [
                per(during.runtime),
                per(during.node),
                per(during.host),
                per(during.other),
            ]
        }),
        report: host.frame_overhead(),
    })
}

fn print_run(label: &str, run: &Measured) {
    println!("== {label} ==");
    println!(
        "tick (push + tick + take): p50 {} ns, mean {:.0} ns",
        run.tick_p50_ns, run.tick_mean_ns
    );
    for (name, per_tick) in &run.hardware_per_tick {
        match per_tick {
            Some(count) => println!("{name} per tick: {count:.1}"),
            None => println!("{name} per tick: n/a (the kernel refuses this event)"),
        }
    }
    if let Some([runtime, node, host, other]) = run.allocs_per_tick {
        println!(
            "allocations per tick: runtime {runtime:.3}, node {node:.3}, host {host:.3}, other {other:.3}"
        );
    }
    if let Some(report) = &run.report {
        println!("{report}");
    }
}

/// Symbols of one mapped file, sorted by address (`nm`; dynamic symbols for shared objects).
fn file_symbols(path: &str, dynamic: bool) -> Vec<(u64, String)> {
    let mut args = vec!["-n", "--defined-only"];
    if dynamic {
        args.insert(0, "-D");
    }
    let Ok(output) = std::process::Command::new("nm")
        .args(&args)
        .arg(path)
        .output()
    else {
        return Vec::new();
    };
    let mut symbols: Vec<(u64, String)> = Vec::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let mut parts = line.splitn(3, ' ');
        let (Some(addr), Some(kind), Some(name)) = (parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        if matches!(kind, "t" | "T" | "w" | "W")
            && let Ok(addr) = u64::from_str_radix(addr, 16)
        {
            symbols.push((addr, name.to_string()));
        }
    }
    symbols
}

/// Resolves instruction pointers to `file:symbol` through the process's mappings.
struct Symbolizer {
    mappings: Vec<(u64, u64, u64, String)>,
    tables: std::collections::BTreeMap<String, Vec<(u64, String)>>,
    exe: String,
}

impl Symbolizer {
    fn new() -> Result<Self, Error> {
        // (start, end, file offset, path) of every executable mapping of a file.
        let maps = std::fs::read_to_string("/proc/self/maps")?;
        let mut mappings: Vec<(u64, u64, u64, String)> = Vec::new();
        for line in maps.lines() {
            let mut fields = line.split_whitespace();
            let (Some(range), Some(perms), Some(offset), Some(_dev), Some(_inode)) = (
                fields.next(),
                fields.next(),
                fields.next(),
                fields.next(),
                fields.next(),
            ) else {
                continue;
            };
            let path = fields.next().unwrap_or("");
            if !perms.contains('x') || path.is_empty() || path.starts_with('[') {
                continue;
            }
            let Some((start, end)) = range.split_once('-') else {
                continue;
            };
            if let (Ok(start), Ok(end), Ok(offset)) = (
                u64::from_str_radix(start, 16),
                u64::from_str_radix(end, 16),
                u64::from_str_radix(offset, 16),
            ) {
                mappings.push((start, end, offset, path.to_string()));
            }
        }
        Ok(Self {
            mappings,
            tables: Default::default(),
            exe: std::env::current_exe()?.to_string_lossy().to_string(),
        })
    }

    /// `symbol` for `ip`, prefixed with the file name for shared objects.
    fn name(&mut self, ip: u64) -> String {
        let Some((start, _, offset, path)) = self
            .mappings
            .iter()
            .find(|(start, end, _, _)| (*start..*end).contains(&ip))
            .cloned()
        else {
            return "(unmapped)".to_string();
        };
        let file_addr = ip - start + offset;
        let table = self.tables.entry(path.clone()).or_insert_with(|| {
            let symbols = file_symbols(&path, false);
            if symbols.is_empty() {
                file_symbols(&path, true)
            } else {
                symbols
            }
        });
        let index = table.partition_point(|(addr, _)| *addr <= file_addr);
        let symbol = index
            .checked_sub(1)
            .map_or("?".to_string(), |i| table[i].1.clone());
        if path == self.exe {
            symbol
        } else {
            let file = path.rsplit('/').next().unwrap_or(&path);
            format!("{file}:{symbol}")
        }
    }
}

/// Print the hottest symbols of sampled call chains: self (leaf) time, inclusive time (a function
/// anywhere in a chain, once per sample), and the leaf with its immediate caller.
fn profile_symbols(chains: &[Vec<u64>]) -> Result<(), Error> {
    use std::collections::{BTreeMap, BTreeSet};
    let mut symbolizer = Symbolizer::new()?;
    let total = chains.len() as f64;
    let mut leaf: BTreeMap<String, u64> = BTreeMap::new();
    let mut inclusive: BTreeMap<String, u64> = BTreeMap::new();
    let mut pairs: BTreeMap<(String, String), u64> = BTreeMap::new();
    for chain in chains {
        let names: Vec<String> = chain.iter().map(|&ip| symbolizer.name(ip)).collect();
        let Some(first) = names.first() else {
            continue;
        };
        *leaf.entry(first.clone()).or_default() += 1;
        let caller = names.get(1).cloned().unwrap_or_default();
        *pairs.entry((first.clone(), caller)).or_default() += 1;
        let frames: BTreeSet<&String> = names.iter().collect();
        for name in frames {
            *inclusive.entry(name.clone()).or_default() += 1;
        }
    }
    let print = |title: &str, counts: &BTreeMap<String, u64>, take: usize| {
        let mut ranked: Vec<(&String, &u64)> = counts.iter().collect();
        ranked.sort_by(|a, b| b.1.cmp(a.1));
        println!("== {title} ({} samples)", chains.len());
        for (name, count) in ranked.iter().take(take) {
            println!(
                "{:6.2}% {:>8}  {name}",
                **count as f64 * 100.0 / total,
                count
            );
        }
    };
    print("self (leaf)", &leaf, 40);
    print("inclusive", &inclusive, 50);
    let mut ranked: Vec<(&(String, String), &u64)> = pairs.iter().collect();
    ranked.sort_by(|a, b| b.1.cmp(a.1));
    println!("== leaf <- caller ({} samples)", chains.len());
    for ((leaf_name, caller_name), count) in ranked.iter().take(30) {
        println!(
            "{:6.2}% {:>8}  {leaf_name}\n                   <- {caller_name}",
            **count as f64 * 100.0 / total,
            count
        );
    }
    Ok(())
}

/// `MIRROR_PROFILE=1`: sample the instruction pointer of the measured ticks (metrics Timing, frame
/// overhead recorded unless `MIRROR_PROFILE_RECORD=0`), then print the hottest symbols.
fn profile(ticks: usize, warmup: usize) -> Result<(), Error> {
    let record = env_usize("MIRROR_PROFILE_RECORD", 1) != 0;
    let mut host = compile(MetricsLevel::Timing, record)?;
    let mut source = SyntheticFrameSource::new(FrameSourceConfig {
        feed: FrameFeed::Interface,
        ..FrameSourceConfig::default()
    })?;
    let camera = Payload::shared(Blob::TYPE_KEY, Arc::new(Blob::default()));
    let extrinsics = Payload::shared(Blob::TYPE_KEY, Arc::new(Blob::default()));
    let frame_port = daedalus::runtime::PortId::new(FRAME);
    let camera_port = daedalus::runtime::PortId::new(CAMERA);
    let extrinsics_port = daedalus::runtime::PortId::new(EXTRINSICS);
    let mut tick = |host: &mut HostGraph<HandlerRegistry>| -> Result<(), Error> {
        host.push_payload(frame_port.clone(), source.next_payload());
        host.push_payload(camera_port.clone(), camera.clone());
        host.push_payload(extrinsics_port.clone(), extrinsics.clone());
        host.tick()?;
        for output in HOST_OUTPUTS {
            while host.take_payload(output).is_some() {}
        }
        Ok(())
    };
    for _ in 0..warmup {
        tick(&mut host)?;
    }
    host.reset_frame_overhead();
    let period = env_usize("MIRROR_PROFILE_PERIOD", 20_000) as u64;
    let callchain = env_usize("MIRROR_PROFILE_CALLCHAIN", 1) != 0;
    let started = if callchain {
        IpSampler::start_callchain(period)
    } else {
        IpSampler::start(period)
    };
    let Some(sampler) = started else {
        println!("instruction sampling unavailable (perf_event_paranoid or no PMU)");
        return Ok(());
    };
    for _ in 0..ticks {
        tick(&mut host)?;
    }
    let chains = sampler.finish();
    profile_symbols(&chains)
}

fn main() -> Result<(), Error> {
    if let Some(cpu) = daedalus_frame_bench::pin_from_env()? {
        println!("pinned to CPU {cpu}");
    }
    let ticks = env_usize("MIRROR_TICKS", 5000).max(1);
    let warmup = env_usize("MIRROR_WARMUP", ticks / 5);
    println!(
        "apriltag_mirror: {} {}, 21 stages, serial, {} measured ticks per run, window {WINDOW}",
        std::env::consts::ARCH,
        std::env::consts::OS,
        ticks
    );
    if std::env::var("MIRROR_PROFILE").is_ok() {
        return profile(ticks, warmup);
    }
    // Untimed run so the first measured one does not pay for cold caches and clock ramp-up.
    measure(MetricsLevel::Off, false, ticks / 4, warmup)?;
    for (name, level) in [("Timing", MetricsLevel::Timing), ("Off", MetricsLevel::Off)] {
        let plain = measure(level, false, ticks, warmup)?;
        print_run(&format!("metrics {name}, no recording"), &plain);
        let recorded = measure(level, true, ticks, warmup)?;
        print_run(
            &format!("metrics {name}, frame overhead recorded"),
            &recorded,
        );
    }
    Ok(())
}

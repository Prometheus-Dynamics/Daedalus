//! Runs one benchmark of the budget table, a given number of times.
//!
//! ```text
//! budget-harness idle TICKS [STAGES]          a chain ticked with nothing fed
//! budget-harness chain TICKS [STAGES] [extra|unfused]  a chain with a value in it, fused unless told
//! budget-harness detector FRAMES              the detector-shaped graph
//! budget-harness plugin TICKS LIBRARY         a dylib plugin's node
//! budget-harness allocations [planted]        allocations per steady-state tick, one row a line
//! ```
//!
//! An instruction count is the difference between two runs with different counts, divided by
//! the difference in the counts, so the cost of building the graph cancels out.

#![forbid(unsafe_code)]
#![allow(clippy::print_stdout, clippy::print_stderr)] // a command-line tool

use std::hint::black_box;
use std::path::Path;
use std::process::ExitCode;

use daedalus_abi::counting::{CountingAlloc, counted};
use daedalus_budget_harness::detector::{detector, frame, picture};
use daedalus_budget_harness::plugin::{plugin_node, plugin_tick};
use daedalus_budget_harness::{
    Stage, big, chain, chain_tick, idle_tick, large_feed, large_handle, large_step,
};
use daedalus_handoff_bench as handoff;

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

/// Ticks run before anything is counted, so pools and queues are at their steady size.
const WARM_UP: u64 = 64;

fn number(text: Option<&String>, default: u64) -> Result<u64, String> {
    match text {
        None => Ok(default),
        Some(text) => text.parse().map_err(|_| format!("`{text}` is not a count")),
    }
}

/// Allocations per tick over 1,000 steady-state ticks of each graph.
fn allocations(planted: bool) {
    const TICKS: u64 = 1000;
    let kind = if planted {
        Stage::Allocates
    } else {
        Stage::Plain
    };
    let mut fused = chain(16, kind, true);
    for value in 0..WARM_UP {
        chain_tick(&mut fused, value);
    }
    let ((), count) = counted(|| {
        for value in 0..TICKS {
            black_box(chain_tick(&mut fused, value));
        }
    });
    println!("alloc_fused_tick {}", count as u64 / TICKS);

    for (name, moved) in [
        ("alloc_large_handle_tick", false),
        ("alloc_large_handle_moved_tick", true),
    ] {
        let mut host = large_handle(moved);
        let shared = big();
        // A reader borrows the handle, so one value serves every tick. A node that takes the
        // value needs the only handle, so each tick gets a new one, made outside the count.
        let handle = |shared: &std::sync::Arc<_>| match moved {
            true => big(),
            false => std::sync::Arc::clone(shared),
        };
        for _ in 0..WARM_UP {
            large_feed(&mut host, handle(&shared));
            large_step(&mut host);
        }
        let mut total = 0;
        for _ in 0..TICKS {
            large_feed(&mut host, handle(&shared));
            let (sum, count) = counted(|| large_step(&mut host));
            assert_eq!(sum, 3);
            total += count as u64;
        }
        println!("{name} {}", total / TICKS);
    }

    let mut host = detector();
    let picture = picture();
    for _ in 0..WARM_UP {
        frame(&mut host, &picture);
    }
    let ((), count) = counted(|| {
        for _ in 0..TICKS {
            assert_eq!(frame(&mut host, &picture), 3);
        }
    });
    println!("alloc_detector_frame {}", count as u64 / TICKS);
    if !planted {
        handoff_allocations();
    }
}

/// The size of the value the handoff rows pass: large enough that a copy shows, small enough
/// that making one each tick doesn't drown the rest.
const HANDOFF_BYTES: usize = 4096;

/// Allocations per tick of each way to pass a large value, read by eight nodes and handed on
/// through eight stages.
fn handoff_allocations() {
    for path in handoff::Path::ALL {
        for (shape, name) in [
            (handoff::Shape::Readers(8), "read"),
            (handoff::Shape::Chain(8), "chain"),
            (handoff::Shape::Handed(8), "handed"),
        ] {
            let cost = handoff::cost(path, shape, 1 << 20, 200);
            println!(
                "alloc_handoff_{}_{name}_tick {}",
                path.name(),
                cost.allocations
            );
        }
    }
}

fn plugin_allocations(library: &Path) {
    const TICKS: u64 = 1000;
    let mut host = plugin_node(library);
    for value in 0..WARM_UP as u32 {
        plugin_tick(&mut host, value);
    }
    let ((), count) = counted(|| {
        for value in 0..TICKS as u32 {
            assert_eq!(plugin_tick(&mut host, value), value);
        }
    });
    println!("alloc_plugin_node_tick {}", count as u64 / TICKS);
}

fn run(args: &[String]) -> Result<(), String> {
    let command = args.first().map(String::as_str).unwrap_or("");
    match command {
        "idle" => {
            let ticks = number(args.get(1), 0)?;
            let stages = number(args.get(2), 16)? as u32;
            let mut host = chain(stages, Stage::Plain, true);
            // One value through first, so every queue has been used, and then nothing.
            chain_tick(&mut host, 1);
            let mut idle = 0;
            for _ in 0..ticks {
                idle += u64::from(black_box(idle_tick(&mut host)));
            }
            println!("idle: {stages} stages, {ticks} ticks, {idle} idle");
            if idle != ticks {
                return Err("a tick with nothing fed was not idle".to_owned());
            }
        }
        "chain" => {
            let ticks = number(args.get(1), 0)?;
            let stages = number(args.get(2), 16)? as u32;
            let kind = match args.get(3).map(String::as_str) {
                Some("extra") => Stage::ExtraOp,
                _ => Stage::Plain,
            };
            let merge = args.get(3).map(String::as_str) != Some("unfused");
            let mut host = chain(stages, kind, merge);
            let mut sum = 0u64;
            for value in 0..ticks {
                sum = sum.wrapping_add(black_box(chain_tick(&mut host, value)));
            }
            println!("chain: {stages} stages, {ticks} ticks, sum {sum}");
        }
        "detector" => {
            let frames = number(args.get(1), 0)?;
            let mut host = detector();
            let picture = picture();
            let mut taken = 0u64;
            for _ in 0..frames {
                taken += u64::from(black_box(frame(&mut host, &picture)));
            }
            println!("detector: {frames} frames, {taken} outputs");
            if taken != frames * 3 {
                return Err("a frame did not give its three outputs".to_owned());
            }
        }
        "plugin" => {
            let ticks = number(args.get(1), 0)?;
            let library = args.get(2).ok_or("plugin needs the library's path")?;
            if args.get(3).map(String::as_str) == Some("allocations") {
                plugin_allocations(Path::new(library));
                return Ok(());
            }
            let mut host = plugin_node(Path::new(library));
            let mut sum = 0u64;
            for value in 0..ticks {
                sum += u64::from(black_box(plugin_tick(&mut host, value as u32)));
            }
            println!("plugin: {ticks} ticks, sum {sum}");
        }
        "handoff" => {
            // handoff <ticks> <handle|shared|vec> <chain|handed|readers> [stages]
            let ticks = number(args.get(1), 0)?;
            let path = args
                .get(2)
                .and_then(|text| handoff::Path::parse(text))
                .ok_or("handoff needs a path: handle, shared or vec")?;
            let stages = number(args.get(4), 8)? as u32;
            let shape = match args.get(3).map(String::as_str) {
                Some("chain") => handoff::Shape::Chain(stages),
                Some("handed") => handoff::Shape::Handed(stages),
                Some("readers") => handoff::Shape::Readers(stages),
                _ => return Err("handoff needs a shape: chain, handed or readers".to_owned()),
            };
            let mut host = handoff::graph(path, shape);
            let kept = handoff::value(path, HANDOFF_BYTES);
            let mut sum = 0u64;
            for _ in 0..ticks {
                // A chain takes the value, so it is fed a new one each tick: that is in the
                // count for every path alike.
                let fed = match shape {
                    handoff::Shape::Readers(_) => kept.again(),
                    _ => handoff::value(path, HANDOFF_BYTES),
                };
                handoff::feed(&mut host, fed);
                sum += black_box(handoff::tick(&mut host));
            }
            println!(
                "handoff: {} {shape:?}, {ticks} ticks, sum {sum}",
                path.name()
            );
        }
        "allocations" => allocations(args.get(1).map(String::as_str) == Some("planted")),
        other => return Err(format!("unknown benchmark `{other}`")),
    }
    Ok(())
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("budget-harness: {message}");
            ExitCode::FAILURE
        }
    }
}

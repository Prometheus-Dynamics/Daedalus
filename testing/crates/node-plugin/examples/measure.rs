//! Ticks one small graph of this crate's nodes a given number of times, with the nodes linked
//! in or loaded from the crate's library, so the two can be counted against each other:
//!
//! ```text
//! measure triple|pair|bytes linked|unmerged|plugin TICKS [LIBRARY]
//! ```
//!
//! `triple` is one node. `pair` is two with a small struct between them, and `bytes` two with
//! a megabyte between them: the edge inside is what differs between the forms. An instruction
//! count is the difference between two runs with different counts, divided by the difference.

#![allow(clippy::print_stdout)] // a command-line tool

use std::hint::black_box;
use std::process::ExitCode;
use std::sync::Arc;

use daedalus_cpu::CpuBackend;
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_loader::{Loader, PluginBackend};
use daedalus_node_plugin::NODES;
use daedalus_planner::{Config, plan};
use daedalus_registry::Registry;

fn edge(from: usize, from_port: &str, to: usize, to_port: &str) -> EdgeDoc {
    EdgeDoc {
        from: PortRefDoc {
            node: from as u32,
            port: from_port.to_string(),
        },
        to: PortRefDoc {
            node: to as u32,
            port: to_port.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    }
}

fn chain(nodes: &[(&str, &str, &str)]) -> GraphBody {
    let mut bridge = NodeDoc::new("host");
    bridge.kind = NodeKindDoc::HostBridge;
    for port in ["x", "out"] {
        bridge
            .port_types
            .insert(port.to_string(), "u32".to_string());
    }
    let mut docs = vec![bridge];
    let mut edges = Vec::new();
    let mut last = (0, "x");
    for (at, (id, input, output)) in nodes.iter().enumerate() {
        docs.push(NodeDoc::new(*id));
        edges.push(edge(last.0, last.1, at + 1, input));
        last = (at + 1, output);
    }
    edges.push(edge(last.0, last.1, 0, "out"));
    GraphBody {
        nodes: docs,
        edges,
        groups: Vec::new(),
    }
}

fn run(args: &[String]) -> Result<(), String> {
    let [kind, mode, ticks, rest @ ..] = args else {
        return Err("measure triple|pair|bytes linked|unmerged|plugin TICKS [LIBRARY]".to_owned());
    };
    let graph = match kind.as_str() {
        "triple" => chain(&[("np.triple", "x", "y")]),
        "pair" => chain(&[("np.split", "x", "pair"), ("np.join", "pair", "y")]),
        "bytes" => chain(&[("np.fill", "x", "bytes"), ("np.first", "bytes", "y")]),
        other => return Err(format!("no graph `{other}`")),
    };
    let ticks: u32 = ticks.parse().map_err(|_| "TICKS is a number".to_owned())?;
    let mut registry = Registry::new();
    let planned = match mode.as_str() {
        "linked" | "unmerged" => {
            registry
                .register_all(&NODES)
                .map_err(|errors| format!("{errors:?}"))?;
            let config = Config {
                merge: mode == "linked",
                ..Config::default()
            };
            plan(&graph, &registry, &[&CpuBackend], &config)
        }
        "plugin" => {
            let library = rest.first().ok_or("plugin needs the library's path")?;
            let plugin = Loader::new()
                .open(library)
                .map_err(|error| error.to_string())?;
            plugin
                .join(&mut registry)
                .map_err(|error| error.to_string())?;
            let backend = PluginBackend::new(Arc::new(plugin), 0);
            plan(&graph, &registry, &[&backend], &Config::default())
        }
        other => return Err(format!("no mode `{other}`")),
    }
    .map_err(|error| error.to_string())?;
    let mut host =
        LoadedHost::load(planned, LoadOptions::default()).map_err(|error| error.to_string())?;
    let mut sum = 0u64;
    for at in 0..ticks {
        let io = host.host();
        io.feed_inline("x", &(at | 1).to_le_bytes())
            .map_err(|error| error.to_string())?;
        io.step().map_err(|error| error.to_string())?;
        let out = io
            .take_copy::<u32>("out")
            .map_err(|error| error.to_string())?;
        sum += u64::from(black_box(out).ok_or("a tick gave nothing")?);
    }
    println!("{kind} {mode}: {ticks} ticks, sum {sum}");
    Ok(())
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(why) => {
            println!("{why}");
            ExitCode::FAILURE
        }
    }
}

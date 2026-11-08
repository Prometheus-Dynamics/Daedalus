// The report is this example's output.
#![allow(clippy::print_stdout, clippy::print_stderr)]

//! Instruction counts for the const-port lookup and config resolve, for `perf stat -e
//! instructions:u`. Instruction counts do not depend on host load, so a before/after pair can
//! be compared on a shared machine. Each run does a fixed number of operations and nothing else;
//! subtract the zero-iteration run to get the cost per operation.
//!
//! `const_instructions lookup PORTS ITERS`: `get_payload` of the last of PORTS const ports, ITERS
//! times. `const_instructions config ITERS`: `ConfigCache::get` for the 49-field config on a
//! 52-port unchanged call, ITERS times.

use std::hint::black_box;
use std::sync::Arc;

use daedalus::data::model::Value;
use daedalus::data::typing::TypeRegistry;
use daedalus::registry::capability::PortDecl;
use daedalus::runtime::NodeError;
use daedalus::runtime::config::{ConfigError, NodeConfig, Sanitized};
use daedalus::runtime::const_cache::ConfigCache;
use daedalus::runtime::executor::CorrelatedPayload;
use daedalus::runtime::handles::PortId;
use daedalus::runtime::io::{NodeConstInputs, NodeIo};
use daedalus::transport::Payload;

macro_rules! wide_config {
    ($name:ident; $($f:ident: $t:ty = $v:expr),* $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq)]
        struct $name { $($f: $t),* }

        impl $name {
            fn const_inputs() -> Vec<(PortId, CorrelatedPayload)> {
                vec![$((
                    PortId::from_static(stringify!($f)),
                    CorrelatedPayload::from_edge(Payload::owned(stringify!($t), $v as $t)),
                )),*]
            }
        }

        impl NodeConfig for $name {
            fn ports(_: &TypeRegistry) -> Vec<PortDecl> {
                Vec::new()
            }
            fn port_names() -> &'static [&'static str] {
                &[$(stringify!($f)),*]
            }
            fn from_io(io: &NodeIo) -> Result<Self, NodeError> {
                Ok(Self {
                    $($f: io
                        .get_typed::<$t>(stringify!($f))
                        .ok_or_else(|| NodeError::missing_input(stringify!($f)))?),*
                })
            }
            fn sanitize(self) -> Result<Sanitized<Self>, ConfigError> {
                Ok(Sanitized { value: self, changes: Vec::new() })
            }
            fn validate(&self) -> Result<(), ConfigError> {
                Ok(())
            }
        }
    };
}

wide_config! {
    Wide;
    f00: usize = 4, f01: i16 = -3, f02: u8 = 7, f03: u32 = 1000, f04: f32 = 0.5, f05: bool = true,
    f06: usize = 4, f07: i16 = -3, f08: u8 = 7, f09: u32 = 1000, f10: f32 = 0.5, f11: bool = true,
    f12: usize = 4, f13: i16 = -3, f14: u8 = 7, f15: u32 = 1000, f16: f32 = 0.5, f17: bool = true,
    f18: usize = 4, f19: i16 = -3, f20: u8 = 7, f21: u32 = 1000, f22: f32 = 0.5, f23: bool = true,
    f24: usize = 4, f25: i16 = -3, f26: u8 = 7, f27: u32 = 1000, f28: f32 = 0.5, f29: bool = true,
    f30: usize = 4, f31: i16 = -3, f32: u8 = 7, f33: u32 = 1000, f34: f32 = 0.5, f35: bool = true,
    f36: usize = 4, f37: i16 = -3, f38: u8 = 7, f39: u32 = 1000, f40: f32 = 0.5, f41: bool = true,
    f42: usize = 4, f43: i16 = -3, f44: u8 = 7, f45: u32 = 1000, f46: f32 = 0.5, f47: bool = true,
    f48: usize = 4,
}

/// A shared const list of `ports` entries: the 49 config consts, then fixed ones (as the bench).
fn const_list(ports: usize) -> Arc<NodeConstInputs> {
    let mut list = Wide::const_inputs();
    list.truncate(ports.min(49));
    let mut fixed = 0;
    while list.len() < ports {
        let name = format!("fixed_{fixed}");
        list.push((
            PortId::new(name),
            CorrelatedPayload::from_edge(Payload::owned("value", Value::Int(1))),
        ));
        fixed += 1;
    }
    Arc::new(list.into())
}

fn call_io(list: &Arc<NodeConstInputs>) -> NodeIo {
    NodeIo::from_inputs(std::iter::empty()).with_shared_consts(Some(list.clone()))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let hits = match args.get(1).map(String::as_str) {
        Some("lookup") => {
            let ports: usize = args[2].parse().expect("PORTS");
            let iters: usize = args[3].parse().expect("ITERS");
            let list = const_list(ports);
            let last = list.last().expect("non-empty").0.as_str().to_string();
            let io = call_io(&list);
            let mut hits = 0usize;
            for _ in 0..iters {
                if black_box(io.get_payload(black_box(last.as_str()))).is_some() {
                    hits += 1;
                }
            }
            hits
        }
        Some("config") => {
            let iters: usize = args[2].parse().expect("ITERS");
            let list = const_list(52);
            let mut cache = ConfigCache::<Wide>::default();
            black_box(cache.get(&call_io(&list), "idle").copied().ok());
            let mut hits = 0usize;
            for _ in 0..iters {
                let io = call_io(&list);
                if black_box(cache.get(&io, "idle").copied().ok()).is_some() {
                    hits += 1;
                }
            }
            hits
        }
        Some("edge_build") => {
            // The harness's own cost per call (the io and its edge payload), without the cache.
            let iters: usize = args[2].parse().expect("ITERS");
            let list = const_list(52);
            let mut hits = 0usize;
            for _ in 0..iters {
                let edge = (
                    PortId::new("value"),
                    CorrelatedPayload::from_edge(Payload::owned("value", Value::Int(1))),
                );
                let io = NodeIo::from_inputs(std::iter::once(edge))
                    .with_shared_consts(Some(list.clone()));
                if black_box(io.edge_inputs().len()) > 0 {
                    hits += 1;
                }
            }
            hits
        }
        Some("config_edge") => {
            // An edge input on a non-config port (as a fused node gets its frame each call), so
            // ConfigCache's no-edge shortcut does not apply and it checks every config name.
            let iters: usize = args[2].parse().expect("ITERS");
            let list = const_list(52);
            let mut cache = ConfigCache::<Wide>::default();
            let call = || {
                let edge = (
                    PortId::new("value"),
                    CorrelatedPayload::from_edge(Payload::owned("value", Value::Int(1))),
                );
                NodeIo::from_inputs(std::iter::once(edge)).with_shared_consts(Some(list.clone()))
            };
            black_box(cache.get(&call(), "idle").map(|cfg| *cfg).ok());
            let mut hits = 0usize;
            for _ in 0..iters {
                let io = call();
                if black_box(cache.get(&io, "idle").map(|cfg| *cfg).ok()).is_some() {
                    hits += 1;
                }
            }
            hits
        }
        _ => {
            eprintln!("usage: const_instructions lookup PORTS ITERS | config ITERS");
            std::process::exit(2);
        }
    };
    println!("{hits}");
}

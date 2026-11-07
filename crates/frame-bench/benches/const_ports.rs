//! Per-call framework cost of an idle node as its const-port count grows: 1 port vs 52 (49 config
//! consts and three fixed ones, as a group stage has). The node's body is empty, so what is
//! measured is the io it is handed and the config it resolves per call.
//!
//! `io_build/N`: a call's `NodeIo` over an N-entry shared const list (`for_call`'s public form).
//! `port_lookup/N`: a by-name lookup of the list's last port (the handler's per-argument read).
//! `config_resolve/52`: `ConfigCache::get` for the 49-field `Copy` config, on an unchanged call.
//!
//! Run with `cargo bench -p daedalus-frame-bench --bench const_ports`.

use std::hint::black_box;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
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

/// A config of 49 scalar fields, each read as its own typed const input (what
/// `#[derive(NodeConfig)]` generates: one `get_typed` per field).
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

/// A shared const list of `ports` entries: the 49 config consts, then fixed ones.
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

/// A call's io over `list`, with no edge inputs (an idle node).
fn call_io(list: &Arc<NodeConstInputs>) -> NodeIo {
    NodeIo::from_inputs(std::iter::empty()).with_shared_consts(Some(list.clone()))
}

fn bench_const_ports(c: &mut Criterion) {
    let mut io_build = c.benchmark_group("io_build");
    for ports in [1usize, 52] {
        let list = const_list(ports);
        io_build.bench_with_input(BenchmarkId::from_parameter(ports), &list, |b, list| {
            b.iter(|| black_box(call_io(list)));
        });
    }
    io_build.finish();

    let mut lookup = c.benchmark_group("port_lookup");
    for ports in [1usize, 52, 260] {
        let list = const_list(ports);
        let last = list.last().expect("non-empty").0.as_str().to_string();
        let io = call_io(&list);
        lookup.bench_with_input(
            BenchmarkId::from_parameter(ports),
            &(io, last),
            |b, (io, last)| {
                b.iter(|| black_box(io.get_payload(black_box(last.as_str()))));
            },
        );
    }
    lookup.finish();

    let mut config = c.benchmark_group("config_resolve");
    let list = const_list(52);
    let mut cache = ConfigCache::<Wide>::default();
    // Warm: the first call decodes and arms the shared-list fast path.
    let _ = black_box(cache.get(&call_io(&list), "idle").copied());
    config.bench_function("unchanged_52_ports", |b| {
        b.iter(|| {
            let io = call_io(&list);
            black_box(cache.get(&io, "idle").copied())
        });
    });
    config.finish();
}

criterion_group!(benches, bench_const_ports);
criterion_main!(benches);

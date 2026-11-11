//! A CPU node takes handed bytes by value, with no copy: the bytes the host fed are the bytes the
//! node takes, and the host takes them back at the same address, released once when dropped.

use daedalus_abi::SharedBytes;
use daedalus_cpu::CpuBackend;
use daedalus_cpu_author::Fault;
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_macros::node;
use daedalus_planner::{Config, plan};
use daedalus_registry::{NodeRecord, Registry};

/// Takes the bytes, and gives them back: the node moves the handle through.
#[node(id = "bytes.keep", inputs(port(name = "x", ty = Vec<u8>)), outputs(port(name = "y", ty = Vec<u8>)))]
fn keep(x: SharedBytes) -> Result<SharedBytes, Fault> {
    Ok(x)
}

fn edge(from: u32, from_port: &str, to: u32, to_port: &str) -> EdgeDoc {
    EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: from_port.to_string(),
        },
        to: PortRefDoc {
            node: to,
            port: to_port.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    }
}

#[test]
fn a_cpu_node_takes_handed_bytes_by_value_and_the_host_takes_them_back_at_the_same_address() {
    let mut bridge = NodeDoc::new("host");
    bridge.kind = NodeKindDoc::HostBridge;
    bridge.label = Some("io".to_string());
    bridge
        .port_types
        .insert("x".to_string(), "bytes".to_string());
    bridge
        .port_types
        .insert("y".to_string(), "bytes".to_string());
    let graph = GraphBody {
        nodes: vec![bridge, NodeDoc::new("bytes.keep")],
        edges: vec![edge(0, "x", 1, "x"), edge(1, "y", 0, "y")],
        groups: Vec::new(),
    };
    let mut registry = Registry::new();
    let records: [&'static NodeRecord; 1] = [&KEEP_NODE];
    registry.register_all(&records).expect("the node registers");
    let planned = plan(&graph, &registry, &[&CpuBackend], &Config::default()).expect("plans");
    let mut host = LoadedHost::load(planned, LoadOptions::default()).expect("loads");

    let bytes = SharedBytes::from_vec(vec![9u8; 1 << 20]);
    let address = bytes.as_ptr() as usize;
    host.host().feed_handed("x", bytes).expect("a feed");
    host.step().expect("a tick");
    let out = host
        .host()
        .take_value("y")
        .expect("a take")
        .expect("a value");
    assert_eq!(
        out.bytes().expect("bytes").as_ptr() as usize,
        address,
        "no copy on the way"
    );
    assert_eq!(
        out.bytes().expect("bytes").len(),
        1 << 20,
        "the whole value arrives"
    );
}

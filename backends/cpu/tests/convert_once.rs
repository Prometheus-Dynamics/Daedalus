//! A conversion several readers need is run once a value, and holds one state: three readers of
//! a converted type, and a fourth that reads the value as it is.

use std::sync::Arc;

use daedalus_cpu::CpuBackend;
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, NodeKindDoc, PortRefDoc};
use daedalus_host::{LoadOptions, LoadedHost};
use daedalus_macros::node;
use daedalus_planner::explain::explain_plan;
use daedalus_planner::{Config, Planned, plan};
use daedalus_registry::{NodeRecord, Registry};
use daedalus_types::{TypeExpr, TypeKey, Typed};

/// What the host feeds.
struct Frame(i64);

impl Typed for Frame {
    const KEY: TypeKey = TypeKey::new("once:frame");
    const EXPR: TypeExpr = TypeExpr::Opaque(Self::KEY);
}

/// What the conversion makes: the frame's number, and how many pyramids this conversion has
/// made so far, counting this one.
struct Pyramid {
    of: i64,
    made: i64,
}

impl Typed for Pyramid {
    const KEY: TypeKey = TypeKey::new("once:pyramid");
    const EXPR: TypeExpr = TypeExpr::Opaque(Self::KEY);
}

/// The conversion's state: how many it has made.
#[derive(Default)]
struct Made(i64);

#[node(
    id = "once.pyramid",
    inputs(frame),
    outputs(out),
    conversion(cost = 1),
    state(Made)
)]
fn pyramid(frame: &Frame, made: &mut Made) -> Pyramid {
    made.0 += 1;
    Pyramid {
        of: frame.0,
        made: made.0,
    }
}

/// Gives `frame * 1000 + made`: which frame the pyramid is of, and which one of the
/// conversion's it is.
#[node(id = "once.level", inputs(pyramid), outputs(out))]
fn level(pyramid: &Pyramid) -> i64 {
    pyramid.of * 1000 + pyramid.made
}

#[node(id = "once.whole", inputs(frame), outputs(out))]
fn whole(frame: &Frame) -> i64 {
    frame.0
}

const NODES: [&NodeRecord; 3] = [&PYRAMID_NODE, &LEVEL_NODE, &WHOLE_NODE];

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

/// The host's `frame` to three `once.level` readers, which take a pyramid, and one `once.whole`.
fn planned() -> Planned {
    let mut host = NodeDoc::new("host");
    host.kind = NodeKindDoc::HostBridge;
    host.port_types.insert("frame".into(), "once:frame".into());
    for out in ["a", "b", "c", "w"] {
        host.port_types.insert(out.into(), "i64".into());
    }
    let graph = GraphBody {
        nodes: vec![
            host,
            NodeDoc::new("once.level"),
            NodeDoc::new("once.level"),
            NodeDoc::new("once.level"),
            NodeDoc::new("once.whole"),
        ],
        edges: vec![
            edge(0, "frame", 1, "pyramid"),
            edge(0, "frame", 2, "pyramid"),
            edge(0, "frame", 3, "pyramid"),
            edge(0, "frame", 4, "frame"),
            edge(1, "out", 0, "a"),
            edge(2, "out", 0, "b"),
            edge(3, "out", 0, "c"),
            edge(4, "out", 0, "w"),
        ],
        groups: Vec::new(),
    };
    let mut registry = Registry::new();
    registry.register_all(&NODES).unwrap();
    plan(&graph, &registry, &[&CpuBackend], &Config::default()).unwrap()
}

#[test]
fn three_readers_of_a_converted_type_get_one_conversion_run_once_a_frame() {
    let mut host = LoadedHost::load(planned(), LoadOptions::default()).unwrap();
    for frame in 1..=3_i64 {
        host.host()
            .feed_typed("frame", Arc::new(Frame(frame)))
            .unwrap();
        host.step().unwrap();
        let take = |host: &mut LoadedHost, port| host.host().take_copy::<i64>(port).unwrap();
        // Every reader got the same pyramid: the conversion's `frame`th, so it ran once a frame
        // and its count is one count, not one per reader.
        let seen = frame * 1000 + frame;
        for port in ["a", "b", "c"] {
            assert_eq!(
                take(&mut host, port),
                Some(seen),
                "frame {frame} port {port}"
            );
        }
        assert_eq!(take(&mut host, "w"), Some(frame));
    }
}

#[test]
fn the_plan_explains_the_conversion_once_with_its_three_readers() {
    let explain = explain_plan(&planned());
    let added: Vec<_> = explain
        .added
        .iter()
        .map(|added| {
            let readers: Vec<u32> = added.readers.iter().map(|reader| reader.node).collect();
            (
                added.id.as_str(),
                added.from_node,
                added.from_port.as_str(),
                readers,
            )
        })
        .collect();
    assert_eq!(added, [("once.pyramid", 0, "frame", vec![1, 2, 3])]);
    // Each reader's edge still says it came from the host's frame through the conversion.
    let through: Vec<(u32, u32, &[String])> = explain
        .edges
        .iter()
        .filter(|edge| edge.from_node == 0)
        .map(|edge| (edge.from_node, edge.to_node, edge.conversion.as_slice()))
        .collect();
    let pyramid = ["once.pyramid".to_string()];
    assert_eq!(
        through,
        [
            (0, 1, &pyramid[..]),
            (0, 2, &pyramid[..]),
            (0, 3, &pyramid[..]),
            (0, 4, &[][..]),
        ]
    );
}

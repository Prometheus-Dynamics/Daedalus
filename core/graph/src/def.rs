//! The constant form of a graph.

use daedalus_types::Literal;

/// What a node in the graph is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum NodeKind {
    #[default]
    Node,
    Group,
    HostBridge,
    Control,
}

/// A declared change of rate: `num` outputs for every `den` inputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rate {
    pub num: u32,
    pub den: u32,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NodeDef {
    pub id: &'static str,
    pub kind: NodeKind,
    pub label: Option<&'static str>,
    /// Names a backend to force this node onto it.
    pub backend: Option<&'static str>,
    pub consts: &'static [(&'static str, Literal)],
    pub fire: Option<&'static str>,
    pub reusable: Option<bool>,
    pub rate: Option<Rate>,
    pub shareable: bool,
    pub ui_id: Option<&'static str>,
    /// Port name to type key, for ports whose type the graph chooses.
    pub port_types: &'static [(&'static str, &'static str)],
    pub metadata: &'static [(&'static str, Literal)],
}

impl NodeDef {
    /// A node with every optional field empty.
    pub const fn new(id: &'static str) -> NodeDef {
        NodeDef {
            id,
            kind: NodeKind::Node,
            label: None,
            backend: None,
            consts: &[],
            fire: None,
            reusable: None,
            rate: None,
            shareable: false,
            ui_id: None,
            port_types: &[],
            metadata: &[],
        }
    }
}

/// A port on the node at index `node` in [`GraphDef::nodes`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PortRef {
    pub node: u32,
    pub port: &'static str,
}

/// How an edge queues. The runner defines what each policy does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgePolicy {
    Fifo,
    LatestOnly,
    Bounded {
        capacity: u32,
        overflow: &'static str,
    },
    /// One value, the newest, that lives for the tick it was sent in: a reader that runs later in
    /// that tick gets it, and one that runs on a later tick gets nothing, so an `Option<T>` input
    /// on this edge is `None` unless its value was sent this tick. A reader that could not run
    /// when it was sent (it was waiting on its own work, backing off, held by a full output or
    /// switched out by the host) still gets it in the first tick it can run.
    PerTick,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EdgeDef {
    pub from: PortRef,
    pub to: PortRef,
    pub policy: EdgePolicy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StagePort {
    pub stage: &'static str,
    pub port: &'static str,
}

/// A group of nodes used as one. Data only: the planner expands it. Start one from
/// [`GroupDef::new`] and `..`, so a field added later doesn't need every group written again.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GroupDef {
    pub id: &'static str,
    /// Stage name and node id.
    pub stages: &'static [(&'static str, &'static str)],
    pub connects: &'static [(StagePort, StagePort)],
    pub inputs: &'static [(&'static str, &'static [StagePort])],
    pub outputs: &'static [(&'static str, StagePort)],
    pub consts: &'static [(StagePort, Literal)],
    /// Metadata a stage's node carries: stage name, key and value.
    pub metadata: &'static [(&'static str, &'static str, Literal)],
    /// The policy of the connects into a stage input, by that input. A connect not listed is
    /// `Fifo`; an entry no connect leads into is refused at expansion, so a misspelt one can't
    /// leave its edge `Fifo` unseen.
    pub edge_policies: &'static [(StagePort, EdgePolicy)],
}

impl GroupDef {
    /// A group named `id` with nothing in it.
    pub const fn new(id: &'static str) -> GroupDef {
        GroupDef {
            id,
            stages: &[],
            connects: &[],
            inputs: &[],
            outputs: &[],
            consts: &[],
            metadata: &[],
            edge_policies: &[],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GraphDef<'a> {
    pub nodes: &'a [NodeDef],
    pub edges: &'a [EdgeDef],
    pub groups: &'a [GroupDef],
}

// A whole graph is buildable in a const, on every target this crate builds for.
const _: GraphDef<'static> = GraphDef {
    nodes: &[
        NodeDef::new("demo.source"),
        NodeDef {
            consts: &[("gain", Literal::Float(2.0))],
            ..NodeDef::new("demo.gain")
        },
    ],
    edges: &[EdgeDef {
        from: PortRef {
            node: 0,
            port: "out",
        },
        to: PortRef {
            node: 1,
            port: "in",
        },
        policy: EdgePolicy::Fifo,
    }],
    groups: &[GroupDef {
        stages: &[("a", "demo.source"), ("b", "demo.gain")],
        connects: &[(
            StagePort {
                stage: "a",
                port: "out",
            },
            StagePort {
                stage: "b",
                port: "in",
            },
        )],
        inputs: &[],
        outputs: &[(
            "out",
            StagePort {
                stage: "b",
                port: "out",
            },
        )],
        edge_policies: &[(
            StagePort {
                stage: "b",
                port: "in",
            },
            EdgePolicy::LatestOnly,
        )],
        ..GroupDef::new("demo.pair")
    }],
};

//! A backend, a registry and graphs for the listener tests. A core crate can't use a real
//! backend, so this one is written here, without `unsafe`, on `OwnedStep`.

#![allow(dead_code)] // Each test uses part of this module.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, SpaceKind,
    StepUnit, Transfer, UnitInfo,
};
use daedalus_graph::{
    EdgeDoc, EdgePolicyDoc, GraphBody, GraphDoc, NodeDoc, NodeKindDoc, PortRefDoc,
};
use daedalus_host::listen::{ListenOptions, PluginLoader, ServiceHost};
use daedalus_registry::{OwnedBody, OwnedDecl, OwnedPort, Registry, TypeRef};
use daedalus_runner::{
    INLINE_BYTES, put_inline, put_shared, put_typed, read_inline, skip_cancelled, take_typed,
    write_inline,
};
use daedalus_types::{Scalar, SpaceIndex, TypeIndex, TypeKey};
use daedalus_wire::service::{
    ClientMsg, Op, PluginInfo, Request, ServerMsg, ServiceHello, TypedValue,
};
use daedalus_wire::{DEFAULT_CAP, Schema, WireType, WireValue, read_packet, write_packet};
use std::net::{SocketAddr, TcpStream};

/// Set when the waiting node's body is running.
pub static WAITING: AtomicBool = AtomicBool::new(false);

/// How many bodies of the node `t.probe` are alive: compiled and not yet released.
pub static PROBES: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);

enum Kind {
    Double,
    Boom,
    /// Runs until the run is cancelled.
    Wait,
    /// Copies its input's bytes to its output, whatever the type: the way a backend that
    /// reads a cell's bytes works.
    Echo,
    /// Hands a typed `String` or `Vec<u8>` on, the way a node written as a Rust function does.
    Typed,
}

struct Body {
    kind: Kind,
    node: u32,
    probe: bool,
    /// Where the output's bytes are kept until the runner has taken them.
    out: [u8; INLINE_BYTES],
}

impl Drop for Body {
    fn drop(&mut self) {
        if self.probe {
            PROBES.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl OwnedStep for Body {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        // A number arrives in place, as its bytes, and leaves the same way.
        match self.kind {
            Kind::Echo => return self.echo(slots),
            Kind::Typed => return typed(slots),
            _ => {}
        }
        let Some(input) = slots.cell(0).and_then(|cell| read_inline::<u32>(cell)) else {
            return StepEnd::Skipped;
        };
        match self.kind {
            Kind::Double => {
                let Some(cell) = slots.cell(1) else {
                    return StepEnd::Fault;
                };
                write_inline(&(input * 2), &mut self.out, cell);
                StepEnd::Done
            }
            Kind::Boom => {
                cx.fault(Some(self.node), "boom: the node failed on purpose");
                StepEnd::Fault
            }
            Kind::Echo | Kind::Typed => StepEnd::Fault,
            Kind::Wait => {
                WAITING.store(true, Ordering::SeqCst);
                while !cx.cancelled() {
                    std::thread::sleep(Duration::from_millis(1));
                }
                skip_cancelled(cx)
            }
        }
    }
}

impl Body {
    fn echo(&mut self, slots: &mut Slots) -> StepEnd {
        let Some(bytes) = slots
            .cell(0)
            .and_then(|cell| cell.bytes().map(<[u8]>::to_vec))
        else {
            return StepEnd::Skipped;
        };
        let Some(cell) = slots.cell(1) else {
            return StepEnd::Fault;
        };
        if bytes.len() <= INLINE_BYTES {
            self.out[..bytes.len()].copy_from_slice(&bytes);
            let kept = &self.out[..bytes.len()];
            put_inline(cell, TypeIndex(0), SpaceIndex(0), kept);
        } else {
            put_shared(cell, TypeIndex(0), SpaceIndex(0), Arc::from(bytes));
        }
        StepEnd::Done
    }
}

fn typed(slots: &mut Slots) -> StepEnd {
    let Some(input) = slots.cell(0) else {
        return StepEnd::Fault;
    };
    if let Some(text) = take_typed::<String>(input) {
        let Some(cell) = slots.cell(1) else {
            return StepEnd::Fault;
        };
        put_typed(
            cell,
            TypeIndex(0),
            SpaceIndex(0),
            Arc::new(format!("{text}!")),
        );
        return StepEnd::Done;
    }
    let Some(bytes) = slots.cell(0).and_then(take_typed::<Vec<u8>>) else {
        return StepEnd::Skipped;
    };
    let Some(cell) = slots.cell(1) else {
        return StepEnd::Fault;
    };
    let reversed: Vec<u8> = bytes.iter().rev().copied().collect();
    put_typed(cell, TypeIndex(0), SpaceIndex(0), Arc::new(reversed));
    StepEnd::Done
}

pub struct TestBackend;

impl Backend for TestBackend {
    fn id(&self) -> &str {
        "t"
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![MemorySpace {
            id: "t.host".into(),
            kind: SpaceKind::Host,
            readable: true,
        }]
    }

    fn transfers(&self) -> Vec<Transfer> {
        Vec::new()
    }

    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        offer
            .nodes
            .iter()
            .map(|node| NodeRef {
                node: node.node,
                body: node.body,
            })
            .collect()
    }

    fn compile(&self, region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        let [node] = region.nodes else {
            return Err(Refusal::new(Vec::new(), "one node a region"));
        };
        let id = cx
            .bodies
            .iter()
            .find(|body| body.node == *node)
            .map_or("", |body| body.id);
        let kind = match id {
            "t.double" | "t.blob" | "t.probe" => Kind::Double,
            "t.boom" => Kind::Boom,
            id if id.starts_with("t.echo.") => Kind::Echo,
            id if id.starts_with("t.typed.") => Kind::Typed,
            _ => Kind::Wait,
        };
        let info = UnitInfo {
            covers: region.nodes.to_vec(),
            reads: region.inputs.to_vec(),
            writes: region.outputs.to_vec(),
            absorbed: Vec::new(),
            may_pend: false,
        };
        let probe = id == "t.probe";
        if probe {
            PROBES.fetch_add(1, Ordering::SeqCst);
        }
        let step = Step::boxed(Body {
            kind,
            node: node.0,
            probe,
            out: [0; INLINE_BYTES],
        });
        Ok(Compiled::Steps(vec![StepUnit { step, info }]))
    }
}

/// The scalars the echo node is registered for, as `t.echo.<type key>`.
pub const ECHOED: [Scalar; 9] = [
    Scalar::Unit,
    Scalar::Bool,
    Scalar::I8,
    Scalar::Int,
    Scalar::U64,
    Scalar::F32,
    Scalar::Float,
    Scalar::String,
    Scalar::Bytes,
];

pub fn registry() -> Registry {
    let mut registry = Registry::new();
    let u32_ty = || TypeRef::Key(Scalar::U32.key());
    let blob = || TypeRef::Key(TypeKey::shared("t:blob").unwrap());
    let scalar = |scalar: Scalar| move || TypeRef::Key(scalar.key());
    let nodes: Vec<(String, Box<dyn Fn() -> TypeRef>)> = vec![
        ("t.double".to_string(), Box::new(u32_ty)),
        ("t.boom".to_string(), Box::new(u32_ty)),
        ("t.wait".to_string(), Box::new(u32_ty)),
        ("t.probe".to_string(), Box::new(u32_ty)),
        ("t.blob".to_string(), Box::new(blob)),
        (
            "t.typed.string".to_string(),
            Box::new(scalar(Scalar::String)),
        ),
        ("t.typed.bytes".to_string(), Box::new(scalar(Scalar::Bytes))),
    ];
    let echoes = ECHOED
        .iter()
        .map(|kind| -> (String, Box<dyn Fn() -> TypeRef>) {
            (
                format!("t.echo.{}", kind.key().as_str()),
                Box::new(scalar(*kind)),
            )
        });
    for (id, ty) in nodes.into_iter().chain(echoes) {
        let id = id.as_str();
        registry
            .register_owned(OwnedDecl {
                id: id.to_string(),
                source: "listen test".to_string(),
                inputs: vec![OwnedPort {
                    name: "in".to_string(),
                    ty: ty(),
                    optional: false,
                }],
                outputs: vec![OwnedPort {
                    name: "out".to_string(),
                    ty: ty(),
                    optional: false,
                }],
                body: OwnedBody {
                    backend: "t".to_string(),
                    text: id.to_string(),
                    code: None,
                },
                conversion: None,
            })
            .unwrap();
    }
    registry
}

/// A graph document: a host bridge with ports `in` and `out`, around one node.
pub fn graph(node: &str) -> String {
    graph_typed(node, "u32")
}

pub fn graph_typed(node: &str, key: &str) -> String {
    let mut bridge = NodeDoc::new("host");
    bridge.kind = NodeKindDoc::HostBridge;
    bridge.label = Some("io".to_string());
    for port in ["in", "out"] {
        bridge.port_types.insert(port.to_string(), key.to_string());
    }
    let mut inner = NodeDoc::new(node);
    inner.label = Some("work".to_string());
    let edge = |from: u32, from_port: &str, to: u32, to_port: &str| EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: from_port.to_string(),
        },
        to: PortRefDoc {
            node: to,
            port: to_port.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    };
    let body = GraphBody {
        nodes: vec![bridge, inner],
        edges: vec![edge(0, "in", 1, "in"), edge(1, "out", 0, "out")],
        groups: Vec::new(),
    };
    GraphDoc::new(body).to_json().unwrap()
}

/// A loader that panics on the path `panic`, fails on `bad`, and otherwise reports a plugin
/// named after the path.
pub struct FakeLoader;

impl PluginLoader for FakeLoader {
    fn load(
        &mut self,
        path: &str,
        _: &mut Registry,
        _: &mut Vec<Arc<dyn Backend>>,
    ) -> Result<PluginInfo, String> {
        match path {
            "panic" => panic!("the loader panicked on purpose"),
            "bad" => Err("not a plugin".to_string()),
            path => Ok(PluginInfo {
                name: path.to_string(),
                generation: 1,
                nodes: Vec::new(),
                backends: Vec::new(),
            }),
        }
    }
}

pub fn host(options: ListenOptions) -> Arc<ServiceHost> {
    ServiceHost::builder(registry())
        .backend(Arc::new(TestBackend))
        .plugins(Box::new(FakeLoader))
        .options(options)
        .build()
}

/// A raw connection that has said hello.
pub fn raw(addr: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).expect("connect");
    let hello = ClientMsg::Hello(ServiceHello::client())
        .encode(&Schema::new())
        .unwrap();
    write_packet(&mut stream, &hello, DEFAULT_CAP).unwrap();
    let mut buf = Vec::new();
    read_packet(&mut stream, DEFAULT_CAP, &mut buf).unwrap();
    assert!(matches!(
        ServerMsg::decode(&buf, &Schema::new()),
        Ok(ServerMsg::Hello(_))
    ));
    stream
}

pub fn request(id: u64, op: Op) -> Vec<u8> {
    ClientMsg::Request(Request::new(id, op))
        .encode(&Schema::new())
        .unwrap()
}

/// A host listening on a loopback port, and how to reach it.
pub fn serving(options: ListenOptions) -> (Arc<ServiceHost>, SocketAddr) {
    let host = host(options);
    let (_listener, addr) = host
        .listen_tcp("127.0.0.1:0".parse().unwrap())
        .expect("listen");
    (host, addr)
}

pub fn u32_value(value: u32) -> TypedValue {
    TypedValue::new(WireType::Scalar(Scalar::U32), WireValue::U32(value))
}

/// Waits for `done`, however slow the machine is. Fails only if it never happens.
pub fn until(what: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(180);
    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(2));
    }
}

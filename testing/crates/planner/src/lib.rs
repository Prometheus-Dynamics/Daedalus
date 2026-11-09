//! Building registries and graphs for the planner's tests.

#![forbid(unsafe_code)]

use std::borrow::Cow;

use daedalus_abi::{Slots, Step, StepCx, StepEnd, StepWord};
use daedalus_backend::{
    Backend, CompileCx, Compiled, MemorySpace, NodeRef, Offer, Refusal, Region, SpaceKind,
    Transfer, TransferCost, UnitInfo,
};
use daedalus_graph::{EdgeDoc, EdgePolicyDoc, GraphBody, NodeDoc, PortRefDoc};
use daedalus_planner::assign::ASSIGN_BUDGET;
use daedalus_planner::regions::{Input, RegionError, Regions, build};
use daedalus_planner::solve::type_table::TypeTable;
use daedalus_planner::stitch::{Stitched, stitch};
use daedalus_registry::{
    BodyRecord, ConfigDecl, Conversion, Fire, NoCode, NodeDecl, NodeRecord, PortDecl, Registry,
    SourceAt, TypeRef,
};
use daedalus_types::{Literal, NodeIndex, Scalar, TypeKey};

fn leak_str(text: impl Into<String>) -> &'static str {
    Box::leak(text.into().into_boxed_str())
}

fn leak<T>(items: Vec<T>) -> &'static [T] {
    Box::leak(items.into_boxed_slice())
}

pub fn key(name: &str) -> TypeRef {
    TypeRef::Key(TypeKey::shared(name).unwrap())
}

pub fn u8_ty() -> TypeRef {
    TypeRef::Key(Scalar::U8.key())
}

pub fn u32_ty() -> TypeRef {
    TypeRef::Key(Scalar::U32.key())
}

pub fn f64_ty() -> TypeRef {
    TypeRef::Key(Scalar::Float.key())
}

/// A declaration under construction, and the backends it has bodies for.
pub struct Decl {
    id: &'static str,
    inputs: Vec<PortDecl>,
    outputs: Vec<PortDecl>,
    fire: Fire,
    reusable: bool,
    shareable: bool,
    conversion: Option<Conversion>,
    group: Option<&'static daedalus_graph::GroupDef>,
    config: Vec<ConfigDecl>,
    bodies: Vec<(&'static str, &'static str, &'static [&'static str])>,
}

pub fn decl(id: &str) -> Decl {
    Decl {
        id: leak_str(id),
        inputs: Vec::new(),
        outputs: Vec::new(),
        fire: Fire::All,
        reusable: false,
        shareable: false,
        conversion: None,
        group: None,
        config: Vec::new(),
        bodies: Vec::new(),
    }
}

impl Decl {
    pub fn input(mut self, name: &str, ty: TypeRef) -> Decl {
        self.inputs.push(PortDecl::new(leak_str(name), ty));
        self
    }

    /// An input taken by value.
    pub fn input_move(mut self, name: &str, ty: TypeRef) -> Decl {
        self.inputs.push(PortDecl {
            access: daedalus_value::Access::Move,
            ..PortDecl::new(leak_str(name), ty)
        });
        self
    }

    pub fn input_with(mut self, port: PortDecl) -> Decl {
        self.inputs.push(port);
        self
    }

    pub fn optional(mut self, name: &str, ty: TypeRef) -> Decl {
        self.inputs.push(PortDecl {
            optional: true,
            ..PortDecl::new(leak_str(name), ty)
        });
        self
    }

    pub fn defaulted(mut self, name: &str, ty: TypeRef, value: Literal) -> Decl {
        self.inputs.push(PortDecl {
            default: Some(value),
            ..PortDecl::new(leak_str(name), ty)
        });
        self
    }

    pub fn config(mut self, name: &str, ty: TypeRef) -> Decl {
        self.config.push(ConfigDecl {
            name: leak_str(name),
            ty,
            fields: &[],
            rules: &[],
        });
        self
    }

    pub fn output(mut self, name: &str, ty: TypeRef) -> Decl {
        self.outputs.push(PortDecl::new(leak_str(name), ty));
        self
    }

    pub fn fire(mut self, fire: Fire) -> Decl {
        self.fire = fire;
        self
    }

    pub fn shareable(mut self) -> Decl {
        self.shareable = true;
        self
    }

    pub fn conversion(mut self, cost: u32, lossy: bool) -> Decl {
        self.conversion = Some(Conversion { cost, lossy });
        self
    }

    pub fn group(mut self, group: &'static daedalus_graph::GroupDef) -> Decl {
        self.group = Some(group);
        self
    }

    /// A body whose `text` is the node id, as the sim backend expects.
    pub fn body(mut self, backend: &str) -> Decl {
        self.bodies.push((leak_str(backend), self.id, &[]));
        self
    }

    pub fn body_needing(mut self, backend: &str, needs: &'static [&'static str]) -> Decl {
        self.bodies.push((leak_str(backend), self.id, needs));
        self
    }

    pub fn add(self, registry: &mut Registry) {
        let node: &'static NodeDecl = Box::leak(Box::new(NodeDecl {
            id: self.id,
            at: SourceAt("test"),
            summary: "",
            description: "",
            inputs: leak(self.inputs),
            outputs: leak(self.outputs),
            config: leak(self.config),
            fanin: &[],
            fire: self.fire,
            reusable: self.reusable,
            shareable: self.shareable,
            conversion: self.conversion,
            group: self.group,
        }));
        for (backend, text, needs) in self.bodies {
            let record: &'static NodeRecord = Box::leak(Box::new(NodeRecord {
                decl: node,
                body: BodyRecord {
                    backend,
                    at: SourceAt("test"),
                    needs,
                    state: None,
                    ports: &[],
                    memory: &[],
                    code: &NoCode,
                    text,
                },
            }));
            registry.register(record).unwrap();
        }
    }
}

pub fn node(id: &str) -> NodeDoc {
    NodeDoc::new(id)
}

pub fn edge(from: u32, from_port: &str, to: u32, to_port: &str) -> EdgeDoc {
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

/// A backend that takes whatever it is offered and compiles nothing, for placement tests. A body
/// that needs [`DECLINED`] is not taken.
pub struct Named(pub &'static str);

pub const DECLINED: &str = "t.declined";

impl Backend for Named {
    fn id(&self) -> &str {
        self.0
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![MemorySpace {
            id: Cow::Borrowed("t.shared"),
            kind: SpaceKind::Shared,
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
            .filter(|node| !node.needs.contains(&DECLINED))
            .map(|node| NodeRef {
                node: node.node,
                body: node.body,
            })
            .collect()
    }

    fn compile(&self, region: &Region<'_>, _: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        Ok(blob(region))
    }
}

/// One unit doing all of a region's work.
fn blob(region: &Region<'_>) -> Compiled {
    Compiled::Blob {
        bytes: Vec::new(),
        units: vec![UnitInfo {
            covers: region.nodes.to_vec(),
            reads: region.inputs.to_vec(),
            writes: region.outputs.to_vec(),
            absorbed: Vec::new(),
            may_pend: false,
        }],
    }
}

/// Takes everything, and refuses any compile holding a body in `refuse` (a body's text is its
/// node's id). With `whole`, the refusal names the whole region.
pub struct Refuser {
    pub id: &'static str,
    pub refuse: &'static [&'static str],
    pub whole: bool,
}

impl Backend for Refuser {
    fn id(&self) -> &str {
        self.id
    }

    fn memory(&self) -> Vec<MemorySpace> {
        vec![MemorySpace {
            id: Cow::Borrowed("t.shared"),
            kind: SpaceKind::Shared,
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
        let refused: Vec<NodeIndex> = cx
            .bodies
            .iter()
            .filter(|body| self.refuse.contains(&body.text))
            .map(|body| body.node)
            .collect();
        if refused.is_empty() {
            Ok(blob(region))
        } else if self.whole {
            Err(Refusal::whole_region("refused"))
        } else {
            Err(Refusal::new(refused, "refused"))
        }
    }
}

/// Solves a graph and gathers the types its ports use, the way a planning run does.
pub fn solved(
    registry: &Registry,
    nodes: Vec<NodeDoc>,
    edges: Vec<EdgeDoc>,
) -> (daedalus_planner::solve::Graph, TypeTable) {
    let body = GraphBody {
        nodes,
        edges,
        groups: Vec::new(),
    };
    let graph =
        daedalus_planner::solve(&body, registry, &daedalus_planner::Config::default()).unwrap();
    let mut types = TypeTable::default();
    for node in &graph.nodes {
        for port in node.inputs.iter().chain(&node.outputs) {
            types.add(&port.ty);
        }
    }
    (graph, types)
}

/// Runs regions and stitching, with the highest priority backend first.
pub fn stitched(
    registry: &Registry,
    nodes: Vec<NodeDoc>,
    edges: Vec<EdgeDoc>,
    backends: &[&dyn Backend],
) -> Result<(Regions, Stitched), RegionError> {
    let (graph, types) = solved(registry, nodes, edges);
    let regions = build(&Input {
        graph: &graph,
        registry,
        backends,
        types: &types,
        budget: ASSIGN_BUDGET,
        merge_allowed: true,
    })?;
    let stitched = stitch(&regions, &types);
    Ok((regions, stitched))
}

/// A step that does nothing.
struct Nop;

impl StepWord for Nop {
    fn run(_: usize, _: &mut Slots, _: &mut StepCx) -> StepEnd {
        StepEnd::Done
    }
}

fn make_nop(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::of::<Nop>(0))
}

/// A backend whose memory and transfers a test spells out.
#[derive(Default)]
pub struct Mem {
    pub id: &'static str,
    /// (id, kind, readable), home first.
    pub spaces: Vec<(&'static str, SpaceKind, bool)>,
    /// (id, from, to, cost).
    pub moves: Vec<(&'static str, &'static str, &'static str, TransferCost)>,
    pub may_pend: bool,
}

impl Backend for Mem {
    fn id(&self) -> &str {
        self.id
    }

    fn memory(&self) -> Vec<MemorySpace> {
        self.spaces
            .iter()
            .map(|&(id, kind, readable)| MemorySpace {
                id: Cow::Borrowed(id),
                kind,
                readable,
            })
            .collect()
    }

    fn transfers(&self) -> Vec<Transfer> {
        self.moves
            .iter()
            .map(|&(id, from, to, cost)| Transfer {
                id,
                key: None,
                from: Cow::Borrowed(from),
                to: Cow::Borrowed(to),
                cost,
                make: make_nop,
            })
            .collect()
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

    fn compile(&self, region: &Region<'_>, _: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        let mut compiled = blob(region);
        if let Compiled::Blob { units, .. } = &mut compiled {
            units[0].may_pend = self.may_pend;
        }
        Ok(compiled)
    }
}

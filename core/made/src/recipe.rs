//! The request one region was compiled from: what the planner told its backend, owned and with no
//! live parts. Compiling a recipe again, on a machine with the same backends and registry, asks the
//! same question, so the backend answers with the same units.
//!
//! The live parts are not stored. A body's code and text come from the registry by backend name,
//! the memory spaces and offers come from the backends, and a room lent by another backend is
//! asked of that backend when the region compiles.

use std::borrow::Cow;
use std::collections::BTreeMap;

use daedalus_backend::{
    Backend, BodyIndex, CompileCx, Compiled, ConstOf, HostBinding, Incoming, InnerEdge, LentRoom,
    NodeBody, OutputSharing, PortRef, RateId, Refusal, Region,
};
use daedalus_graph::{EdgePolicyDoc, Lit};
use daedalus_registry::Registry;
use daedalus_types::{BackendIndex, NodeIndex, TypeExpr};

/// A value that moves into the region's memory before one of its inputs reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecipeIncoming {
    pub input: PortRef,
    pub from: String,
    pub to: String,
    pub transfer: String,
}

/// A constant a node's port holds.
#[derive(Clone, Debug, PartialEq)]
pub struct RecipeConst {
    pub node: u32,
    pub port: String,
    pub value: Lit,
}

/// One node's body in the region: the registry entry it names, and the backend whose record runs.
#[derive(Clone, Debug, PartialEq)]
pub struct RecipeBody {
    pub node: u32,
    pub id: String,
    /// The backend that wrote the record: the key `Entry::body` looks it up by.
    pub backend: String,
    pub inputs: u16,
    pub outputs: u16,
    pub merges: bool,
    pub fire_all: bool,
    pub label: Option<String>,
    pub metadata: BTreeMap<String, Lit>,
}

/// An edge between two nodes of the region.
#[derive(Clone, Debug, PartialEq)]
pub struct RecipeEdge {
    pub from: PortRef,
    pub to: PortRef,
    pub queued: bool,
    pub policy: EdgePolicyDoc,
}

/// A port of the region that faces the host bridge.
#[derive(Clone, Debug, PartialEq)]
pub struct RecipeHost {
    pub port: PortRef,
    pub name: String,
    pub policy: EdgePolicyDoc,
}

/// A room another backend lends for one output. The offering backend is asked for it at compile.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecipeLent {
    pub port: PortRef,
    pub space: String,
    /// The backend that offers `space`, as the planner chose it.
    pub offering: String,
    /// The consumers on the offering backend, by node id.
    pub readers: Vec<String>,
}

/// The planner's choices for one region, and what the backend is asked for them.
#[derive(Clone, Debug, PartialEq)]
pub struct RegionRecipe {
    /// The backend the region runs on.
    pub backend: String,
    pub nodes: Vec<u32>,
    pub inputs: Vec<PortRef>,
    pub outputs: Vec<PortRef>,
    pub rate: RateId,
    pub incoming: Vec<RecipeIncoming>,
    /// The memory space each output is written in.
    pub placed: Vec<String>,
    /// The memory space each input is read from.
    pub read_in: Vec<String>,
    pub consts: Vec<RecipeConst>,
    pub bodies: Vec<RecipeBody>,
    pub edges: Vec<RecipeEdge>,
    pub host_inputs: Vec<RecipeHost>,
    pub host_outputs: Vec<RecipeHost>,
    pub sharing: Vec<OutputSharing>,
    pub lent: Vec<RecipeLent>,
    pub fifo_capacity: u32,
    pub merge_allowed: bool,
    pub freeze_params: bool,
}

impl RegionRecipe {
    /// Asks the region's backend to compile the region. `backends` is in priority order, the order
    /// the planner used: the backend's index is its place there. `types` is the plan's type table.
    pub fn compile(
        &self,
        backends: &[&dyn Backend],
        registry: &Registry,
        types: &[TypeExpr],
    ) -> Result<Compiled, Refusal> {
        let Some(rank) = backends.iter().position(|b| b.id() == self.backend) else {
            return Err(Refusal::whole_region(&format!(
                "backend `{}` is not present",
                self.backend
            )));
        };
        let backend = backends[rank];
        let nodes: Vec<NodeIndex> = self.nodes.iter().map(|&node| NodeIndex(node)).collect();
        let consts: Vec<ConstOf<'_>> = self
            .consts
            .iter()
            .map(|of| ConstOf {
                node: NodeIndex(of.node),
                port: &of.port,
                value: &of.value,
            })
            .collect();
        let mut bodies = Vec::with_capacity(self.bodies.len());
        for body in &self.bodies {
            let Some(entry) = registry.entry(&body.id) else {
                return Err(Refusal::whole_region(&format!(
                    "node `{}` is not in the registry",
                    body.id
                )));
            };
            let Some(at) = entry.bodies.iter().position(|r| r.backend == body.backend) else {
                return Err(Refusal::whole_region(&format!(
                    "node `{}` has no body for backend `{}`",
                    body.id, body.backend
                )));
            };
            let record = entry.bodies[at];
            bodies.push(NodeBody {
                node: NodeIndex(body.node),
                id: &body.id,
                body: BodyIndex(at as u32),
                inputs: body.inputs,
                outputs: body.outputs,
                merges: body.merges,
                fire_all: body.fire_all,
                metrics: entry.decl.metrics.len() as u16,
                label: body.label.as_deref(),
                metadata: &body.metadata,
                code: record.code,
                text: record.text,
            });
        }
        let edges: Vec<InnerEdge<'_>> = self
            .edges
            .iter()
            .map(|edge| InnerEdge {
                from: edge.from,
                to: edge.to,
                queued: edge.queued,
                policy: &edge.policy,
            })
            .collect();
        let host_inputs: Vec<HostBinding<'_>> = host_bindings(&self.host_inputs);
        let host_outputs: Vec<HostBinding<'_>> = host_bindings(&self.host_outputs);
        let incoming: Vec<Incoming<'_>> = self
            .incoming
            .iter()
            .map(|of| Incoming {
                input: of.input,
                from: &of.from,
                to: &of.to,
                transfer: &of.transfer,
            })
            .collect();
        let placed: Vec<&str> = self.placed.iter().map(String::as_str).collect();
        let read_in: Vec<&str> = self.read_in.iter().map(String::as_str).collect();
        let mut lent: Vec<LentRoom> = Vec::with_capacity(self.lent.len());
        for request in &self.lent {
            let Some(offering) = backends.iter().find(|b| b.id() == request.offering) else {
                return Err(Refusal::whole_region(&format!(
                    "backend `{}`, which lends a room, is not present",
                    request.offering
                )));
            };
            let readers: Vec<&str> = request.readers.iter().map(String::as_str).collect();
            if let Some(room) = offering.bind_offer(&request.space, &readers) {
                lent.push(LentRoom {
                    port: request.port,
                    room,
                });
            }
        }
        let spaces = backend.memory();
        let offers: Vec<Cow<'static, str>> = backends
            .iter()
            .filter(|other| other.id() != backend.id())
            .flat_map(|other| other.offers())
            .collect();
        let offered: Vec<&str> = offers.iter().map(|id| id.as_ref()).collect();
        let region = Region {
            backend: BackendIndex(rank as u8),
            nodes: &nodes,
            inputs: &self.inputs,
            outputs: &self.outputs,
            incoming: &incoming,
            placed: &placed,
            read_in: &read_in,
            rate: self.rate,
        };
        backend.compile(
            &region,
            &CompileCx {
                types,
                consts: &consts,
                spaces: &spaces,
                bodies: &bodies,
                edges: &edges,
                host_inputs: &host_inputs,
                host_outputs: &host_outputs,
                fifo_capacity: self.fifo_capacity,
                merge_allowed: self.merge_allowed,
                freeze_params: self.freeze_params,
                sharing: &self.sharing,
                lent: &lent,
                offered: &offered,
            },
        )
    }

    /// The backends this region needs by id: its own, and each one that lends it a room.
    pub fn needs(&self) -> impl Iterator<Item = &str> {
        std::iter::once(self.backend.as_str())
            .chain(self.lent.iter().map(|lent| lent.offering.as_str()))
    }
}

fn host_bindings(ports: &[RecipeHost]) -> Vec<HostBinding<'_>> {
    ports
        .iter()
        .map(|host| HostBinding {
            port: host.port,
            name: &host.name,
            policy: &host.policy,
        })
        .collect()
}

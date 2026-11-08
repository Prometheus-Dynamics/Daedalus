//! Node groups: a plugin exports several nodes as one node that the planner expands into its
//! stages at plan time.
//!
//! A group is an ordinary node declaration carrying an embedded graph
//! ([`EMBEDDED_GRAPH_KEY`]). [`NodeGroup`] builds that declaration from stage node ids, the
//! edges between them, exposed inputs and outputs and config defaults, so a plugin does not
//! write the metadata by hand. The stages stay separate nodes at run time: each keeps its own
//! telemetry, and the group id ([`GROUP_ID_KEY`]) rolls them up.

use alloc::collections::{BTreeMap, BTreeSet};
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use daedalus_core::metadata::{EMBEDDED_GRAPH_KEY, EMBEDDED_HOST_KEY, GROUP_ID_KEY};
use daedalus_data::model::Value;
use daedalus_registry::capability::NodeDecl;
use daedalus_registry::ids::NodeId;

use super::{PluginError, PluginRegistry, PluginResult};
use crate::graph_builder::{GraphBuildError, GraphCtx, graph_to_json};
use crate::handles::{NodeHandle, PortHandle};

/// Metadata key on a group declaration: the stage node ids, as a `Value::List` of strings.
/// [`PluginRegistry::graph_requirements`] reads it, so a group's stage plugins are required by
/// any graph that places the group.
pub const GROUP_STAGES_KEY: &str = "daedalus.group.stages";

/// The host-bridge label every group's embedded graph uses.
const HOST_LABEL: &str = "host";

/// One port of one stage: the stage alias in the group and the port name on that stage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagePort {
    pub stage: String,
    pub port: String,
}

impl StagePort {
    pub fn new(stage: impl Into<String>, port: impl Into<String>) -> Self {
        Self {
            stage: stage.into(),
            port: port.into(),
        }
    }
}

/// Builder for a node group. See the module documentation.
///
/// ```ignore
/// let group = NodeGroup::new("acme:detect_track")
///     .label("Detect and track")
///     .stage("detect", "acme:detect")
///     .stage("track", "acme:track")
///     .connect(StagePort::new("detect", "boxes"), StagePort::new("track", "boxes"))
///     .input("frame", [StagePort::new("detect", "frame")])
///     .input("threshold", [StagePort::new("detect", "threshold")])
///     .output("tracks", StagePort::new("track", "tracks"));
/// registry.register_node_group(group)?;
/// ```
#[derive(Clone, Debug)]
pub struct NodeGroup {
    id: String,
    label: Option<String>,
    stages: Vec<(String, String)>,
    edges: Vec<(StagePort, StagePort)>,
    consts: Vec<(StagePort, Value)>,
    inputs: Vec<(String, Vec<StagePort>)>,
    outputs: Vec<(String, StagePort)>,
}

impl NodeGroup {
    /// A group with canonical node id `id`.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            label: None,
            stages: Vec::new(),
            edges: Vec::new(),
            consts: Vec::new(),
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }

    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Adds stage `node_id` (a node already registered on the registry) under `alias`.
    pub fn stage(mut self, alias: impl Into<String>, node_id: impl Into<String>) -> Self {
        self.stages.push((alias.into(), node_id.into()));
        self
    }

    /// Wires stage output `from` to stage input `to`.
    pub fn connect(mut self, from: StagePort, to: StagePort) -> Self {
        self.edges.push((from, to));
        self
    }

    /// Exposes group input `name`, which feeds every listed stage input. The group port takes
    /// the stage input's type; the targets must agree on it.
    pub fn input(
        mut self,
        name: impl Into<String>,
        targets: impl IntoIterator<Item = StagePort>,
    ) -> Self {
        self.inputs
            .push((name.into(), targets.into_iter().collect()));
        self
    }

    /// Exposes stage output `from` as group output `name`.
    pub fn output(mut self, name: impl Into<String>, from: StagePort) -> Self {
        self.outputs.push((name.into(), from));
        self
    }

    /// Sets a constant on a stage input. On an exposed input this is also the group port's
    /// default, so the config a user does not set keeps the stage's value.
    pub fn const_input(mut self, port: StagePort, value: Value) -> Self {
        self.consts.push((port, value));
        self
    }

    /// The group's node declaration: ports, the union of the stages' feature flags, and the
    /// embedded graph. Checks the stages, ports and types against `registry`.
    pub fn declaration(&self, registry: &PluginRegistry) -> PluginResult<NodeDecl> {
        let caps = registry.combined_transport_capabilities()?;
        let mut decls: BTreeMap<&str, NodeDecl> = BTreeMap::new();
        for (alias, node_id) in &self.stages {
            let decl = caps
                .nodes()
                .get(&NodeId::new(node_id.as_str()))
                .cloned()
                .ok_or_else(|| {
                    invalid(format!(
                        "node group `{}`: stage `{alias}` node `{node_id}` is not registered",
                        self.id
                    ))
                })?;
            if decls.insert(alias.as_str(), decl).is_some() {
                return Err(invalid(format!(
                    "node group `{}`: duplicate stage alias `{alias}`",
                    self.id
                )));
            }
        }

        let graph = self.embedded_graph(&caps)?;
        let json = graph_to_json(&graph).map_err(invalid)?;

        let mut decl = NodeDecl::new(self.id.clone())
            .label(self.label.clone().unwrap_or_else(|| self.id.clone()));
        for (name, targets) in &self.inputs {
            let mut port = None;
            for target in targets {
                let src = stage_port(&decls, &self.id, target, true)?;
                match &port {
                    None => port = Some(src),
                    Some(first) if first.type_key != src.type_key => {
                        return Err(invalid(format!(
                            "node group `{}`: input `{name}` feeds ports of different types",
                            self.id
                        )));
                    }
                    Some(_) => {}
                }
            }
            let Some(mut port) = port else {
                return Err(invalid(format!(
                    "node group `{}`: input `{name}` feeds no stage port",
                    self.id
                )));
            };
            port.name = name.clone();
            let default = self
                .consts
                .iter()
                .find(|(port, _)| targets.contains(port))
                .map(|(_, value)| value.clone());
            if let Some(value) = default {
                port = port.const_value(value);
            }
            decl = decl.input(port);
        }
        for (name, from) in &self.outputs {
            let mut port = stage_port(&decls, &self.id, from, false)?;
            port.name = name.clone();
            decl = decl.output(port);
        }

        let features: BTreeSet<&str> = decls
            .values()
            .flat_map(|stage| stage.feature_flags.iter().map(String::as_str))
            .collect();
        for feature in features {
            decl = decl.feature_flag(feature);
        }
        let stage_ids = self
            .stages
            .iter()
            .map(|(_, node_id)| Value::String(node_id.clone().into()))
            .collect();
        Ok(decl
            .metadata(EMBEDDED_GRAPH_KEY, Value::String(json.into()))
            .metadata(EMBEDDED_HOST_KEY, Value::String(HOST_LABEL.into()))
            .metadata(GROUP_ID_KEY, Value::String(self.id.clone().into()))
            .metadata(GROUP_STAGES_KEY, Value::List(stage_ids)))
    }

    /// The embedded graph: the stages, their edges, the host bridge and the constants.
    fn embedded_graph(
        &self,
        caps: &daedalus_registry::capability::CapabilityRegistry,
    ) -> PluginResult<daedalus_planner::Graph> {
        let input_names: Vec<&str> = self.inputs.iter().map(|(name, _)| name.as_str()).collect();
        let output_names: Vec<&str> = self.outputs.iter().map(|(name, _)| name.as_str()).collect();
        let mut ctx = GraphCtx::new(caps.clone(), &input_names, &output_names);
        let mut handles: BTreeMap<&str, NodeHandle> = BTreeMap::new();
        for (alias, node_id) in &self.stages {
            let handle = ctx
                .try_node_as(node_id, alias)
                .map_err(|err| invalid(format!("node group `{}`: {err}", self.id)))?;
            handles.insert(alias.as_str(), handle);
        }
        let port = |sp: &StagePort, input: bool| -> PluginResult<PortHandle> {
            let handle = handles.get(sp.stage.as_str()).ok_or_else(|| {
                invalid(format!(
                    "node group `{}`: unknown stage `{}`",
                    self.id, sp.stage
                ))
            })?;
            Ok(if input {
                handle.input(sp.port.as_str())
            } else {
                handle.output(sp.port.as_str())
            })
        };
        let wrap = |err: GraphBuildError| invalid(err.to_string());
        for (from, to) in &self.edges {
            ctx.try_connect(&port(from, false)?, &port(to, true)?)
                .map_err(wrap)?;
        }
        for (name, targets) in &self.inputs {
            for target in targets {
                ctx.try_connect(&ctx.input(name), &port(target, true)?)
                    .map_err(wrap)?;
            }
        }
        for (target, value) in &self.consts {
            ctx.try_const_input(&port(target, true)?, value.clone())
                .map_err(wrap)?;
        }
        for (name, from) in &self.outputs {
            ctx.try_bind_output(name, &port(from, false)?)
                .map_err(wrap)?;
        }
        ctx.try_build().map_err(wrap)
    }
}

/// The declared port `sp` of its stage: an input when `input`, else an output.
fn stage_port(
    decls: &BTreeMap<&str, NodeDecl>,
    group: &str,
    sp: &StagePort,
    input: bool,
) -> PluginResult<daedalus_registry::capability::PortDecl> {
    let stage = decls.get(sp.stage.as_str()).ok_or_else(|| {
        invalid(format!(
            "node group `{group}`: unknown stage `{}`",
            sp.stage
        ))
    })?;
    let ports = if input { &stage.inputs } else { &stage.outputs };
    ports
        .iter()
        .find(|port| port.name == sp.port)
        .cloned()
        .ok_or_else(|| {
            invalid(format!(
                "node group `{group}`: stage `{}` has no port `{}`",
                sp.stage, sp.port
            ))
        })
}

fn invalid(message: impl ToString) -> PluginError {
    PluginError::Install {
        message: message.to_string(),
    }
}

impl PluginRegistry {
    /// Registers a [`NodeGroup`] as a node: it lists and places like any other node, and the
    /// planner expands it into its stages.
    pub fn register_node_group(&mut self, group: NodeGroup) -> PluginResult<()> {
        let decl = group.declaration(self)?;
        self.register_node_decl(decl)
    }
}

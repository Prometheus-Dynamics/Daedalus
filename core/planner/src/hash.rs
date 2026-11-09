//! The canonical bytes behind a plan's hash and its cache key.
//!
//! Everything is written field by field in a fixed order. Each document type is destructured with
//! no `..`, so a new field can't be added without deciding here whether it counts.

use daedalus_backend::Backend;
use daedalus_graph::{
    ControlDoc, ControlOp, EdgeDoc, EdgePolicyDoc, GraphBody, GroupDoc, HostInputPolicy, Lit,
    NodeDoc, NodeKindDoc, RateDoc, SyncGroupDoc, SyncPolicy,
};
use daedalus_registry::Registry;
use daedalus_types::fnv1a64;

use crate::api::{PlanError, Planned, by_priority};
use crate::config::{Config, DEFAULT_FIFO_CAPACITY};
use crate::form::Form;

#[derive(Default)]
struct Canon(Vec<u8>);

impl Canon {
    fn byte(&mut self, value: u8) {
        self.0.push(value);
    }

    fn flag(&mut self, value: bool) {
        self.byte(u8::from(value));
    }

    fn word(&mut self, value: u64) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn len(&mut self, value: usize) {
        self.word(value as u64);
    }

    fn text(&mut self, value: &str) {
        self.len(value.len());
        self.0.extend_from_slice(value.as_bytes());
    }

    fn texts<S: AsRef<str>>(&mut self, values: &[S]) {
        self.len(values.len());
        for value in values {
            self.text(value.as_ref());
        }
    }

    fn maybe_text(&mut self, value: Option<&str>) {
        self.flag(value.is_some());
        if let Some(value) = value {
            self.text(value);
        }
    }

    fn lit(&mut self, value: &Lit) {
        match value {
            Lit::Unit => self.byte(0),
            Lit::Bool(value) => {
                self.byte(1);
                self.flag(*value);
            }
            Lit::Int(value) => {
                self.byte(2);
                self.word(*value as u64);
            }
            Lit::Float(value) => {
                self.byte(3);
                self.word(value.to_bits());
            }
            Lit::Str(value) => {
                self.byte(4);
                self.text(value);
            }
            Lit::Bytes(value) => {
                self.byte(5);
                self.len(value.len());
                self.0.extend_from_slice(value);
            }
            Lit::List(items) => {
                self.byte(6);
                self.len(items.len());
                items.iter().for_each(|item| self.lit(item));
            }
            Lit::Struct(fields) => {
                self.byte(7);
                self.len(fields.len());
                for field in fields {
                    self.text(&field.name);
                    self.lit(&field.value);
                }
            }
            Lit::Enum { name, payload } => {
                self.byte(8);
                self.text(name);
                self.flag(payload.is_some());
                if let Some(payload) = payload {
                    self.lit(payload);
                }
            }
        }
    }

    fn lits<'a>(&mut self, count: usize, entries: impl Iterator<Item = (&'a String, &'a Lit)>) {
        self.len(count);
        for (name, value) in entries {
            self.text(name);
            self.lit(value);
        }
    }

    fn sync_group(&mut self, group: &SyncGroupDoc) {
        let SyncGroupDoc {
            name,
            policy,
            ports,
        } = group;
        self.text(name);
        self.byte(match policy {
            SyncPolicy::AllReady => 0,
            SyncPolicy::Latest => 1,
            SyncPolicy::ZipByTag => 2,
        });
        self.texts(ports);
    }

    fn node(&mut self, node: &NodeDoc) {
        let NodeDoc {
            id,
            kind,
            label,
            backend,
            consts,
            fire,
            reusable,
            rate,
            shareable,
            ui_id,
            port_types,
            host_inputs,
            sync_groups,
            inputs,
            outputs,
            metadata,
            control,
        } = node;
        self.text(id);
        self.byte(match kind {
            NodeKindDoc::Node => 0,
            NodeKindDoc::Group => 1,
            NodeKindDoc::HostBridge => 2,
            NodeKindDoc::Control => 3,
        });
        self.maybe_text(label.as_deref());
        self.maybe_text(backend.as_deref());
        self.lits(consts.len(), consts.iter());
        self.maybe_text(fire.as_deref());
        self.byte(reusable.map_or(2, u8::from));
        self.flag(rate.is_some());
        if let Some(RateDoc { num, den }) = rate {
            self.word(u64::from(*num));
            self.word(u64::from(*den));
        }
        self.flag(*shareable);
        self.maybe_text(ui_id.as_deref());
        self.len(port_types.len());
        for (name, ty) in port_types {
            self.text(name);
            self.text(ty);
        }
        self.len(host_inputs.len());
        for (name, policy) in host_inputs {
            self.text(name);
            self.byte(match policy {
                HostInputPolicy::Consume => 0,
                HostInputPolicy::Held => 1,
                HostInputPolicy::Shared => 2,
            });
        }
        self.len(sync_groups.len());
        sync_groups.iter().for_each(|group| self.sync_group(group));
        self.texts(inputs);
        self.texts(outputs);
        self.lits(metadata.len(), metadata.iter());
        self.flag(control.is_some());
        if let Some(ControlDoc { op, max, bodies }) = control {
            self.byte(match op {
                ControlOp::If => 0,
                ControlOp::ForEach => 1,
                ControlOp::Until => 2,
            });
            self.word(max.map_or(u64::MAX, u64::from));
            self.len(bodies.len());
            for (name, body) in bodies {
                self.text(name);
                let bytes = graph_bytes(body);
                self.len(bytes.len());
                self.0.extend_from_slice(&bytes);
            }
        }
    }

    fn edge(&mut self, edge: &EdgeDoc) {
        let EdgeDoc { from, to, policy } = edge;
        self.word(u64::from(from.node));
        self.text(&from.port);
        self.word(u64::from(to.node));
        self.text(&to.port);
        match policy {
            EdgePolicyDoc::Fifo => self.byte(0),
            EdgePolicyDoc::LatestOnly => self.byte(1),
            EdgePolicyDoc::Bounded { capacity, overflow } => {
                self.byte(2);
                self.word(u64::from(*capacity));
                self.text(overflow);
            }
        }
    }

    fn group(&mut self, group: &GroupDoc) {
        let GroupDoc {
            id,
            stages,
            connects,
            inputs,
            outputs,
            consts,
        } = group;
        self.text(id);
        self.len(stages.len());
        for (stage, node) in stages {
            self.text(stage);
            self.text(node);
        }
        self.len(connects.len());
        for (from, to) in connects {
            for at in [from, to] {
                self.text(&at.stage);
                self.text(&at.port);
            }
        }
        self.len(inputs.len());
        for (name, targets) in inputs {
            self.text(name);
            self.len(targets.len());
            for at in targets {
                self.text(&at.stage);
                self.text(&at.port);
            }
        }
        self.len(outputs.len());
        for (name, at) in outputs {
            self.text(name);
            self.text(&at.stage);
            self.text(&at.port);
        }
        self.len(consts.len());
        for item in consts {
            self.text(&item.at.stage);
            self.text(&item.at.port);
            self.lit(&item.value);
        }
    }

    fn config(&mut self, config: &Config) {
        // Lints only produce diagnostics, which don't change a plan. Priority shows up as the
        // order of the backends, which is the only thing it decides.
        let Config {
            priority: _,
            strict_ports,
            lints: _,
            rewrites,
            assignment_budget,
            form,
            merge,
            fifo_capacity,
            freeze_params,
        } = config;
        self.flag(*strict_ports);
        self.word(*assignment_budget);
        if *fifo_capacity != DEFAULT_FIFO_CAPACITY {
            self.word(u64::from(*fifo_capacity));
        }
        if !merge {
            self.byte(0xff);
        }
        if *freeze_params {
            self.byte(0xfe);
        }
        self.byte(match form {
            Form::Steps => 0,
            Form::Blob => 1,
            Form::Source => 2,
        });
        self.len(rewrites.len());
        for rewrite in rewrites {
            self.text(rewrite.id());
            self.word(u64::from(rewrite.version()));
        }
    }
}

/// The canonical bytes of a graph as given.
pub fn graph_bytes(graph: &GraphBody) -> Vec<u8> {
    let GraphBody {
        nodes,
        edges,
        groups,
    } = graph;
    let mut out = Canon::default();
    out.len(nodes.len());
    nodes.iter().for_each(|node| out.node(node));
    out.len(edges.len());
    edges.iter().for_each(|edge| out.edge(edge));
    out.len(groups.len());
    groups.iter().for_each(|group| out.group(group));
    out.0
}

/// The canonical bytes of everything else a plan depends on: the config, what each backend
/// offers, in the order they'll be tried, and what the registry declares.
pub fn settings_bytes(config: &Config, registry: &Registry, backends: &[&dyn Backend]) -> Vec<u8> {
    let mut out = Canon::default();
    out.config(config);
    let ordered = by_priority(backends, &config.priority);
    out.len(ordered.len());
    for backend in ordered {
        out.text(backend.id());
        out.word(backend.version());
        let memory = backend.memory();
        out.len(memory.len());
        for space in memory {
            out.text(&format!("{space:?}"));
        }
        let transfers = backend.transfers();
        out.len(transfers.len());
        for transfer in transfers {
            let shown = (
                transfer.id,
                &transfer.key,
                &transfer.from,
                &transfer.to,
                transfer.cost,
            );
            out.text(&format!("{shown:?}"));
        }
    }
    // Only ever compared within one process: the cache isn't stored.
    for entry in registry.entries() {
        out.text(&format!("{:?}", entry.decl));
        let mut bodies: Vec<String> = entry
            .bodies
            .iter()
            .map(|body| format!("{body:?}"))
            .collect();
        bodies.sort_unstable();
        out.texts(&bodies);
    }
    for transfer in registry.transfers() {
        out.text(&format!(
            "{:?}",
            (
                transfer.id,
                &transfer.key,
                &transfer.output,
                transfer.from,
                transfer.to,
                transfer.class
            )
        ));
    }
    out.0
}

/// The hash of a finished plan: its blob, the consts it was solved with, the backends, the
/// transfers it uses, and the config.
pub fn plan_hash(
    planned: &Planned,
    config: &Config,
    backends: &[&dyn Backend],
) -> Result<u64, PlanError> {
    let mut out = Canon::default();
    let blob = planned.blob()?;
    out.len(blob.len());
    out.0.extend_from_slice(&blob);
    for node in &planned.graph.nodes {
        out.lits(node.consts.len(), node.consts.iter());
    }
    for backend in by_priority(backends, &config.priority) {
        out.text(backend.id());
        out.word(backend.version());
    }
    let routing = &planned.regions.routing;
    out.len(routing.table.len());
    for entry in &routing.table {
        let shown = (&entry.id, &entry.key, &entry.from, &entry.to, entry.cost);
        out.text(&format!("{shown:?}"));
    }
    out.len(planned.stitched.transfers.len());
    for used in &planned.stitched.transfers {
        out.len(used.table);
        out.text(&used.id);
    }
    out.config(config);
    Ok(fnv1a64(&out.0))
}

//! Where each boundary value lives and how it reaches its readers.

use std::collections::BTreeMap;

use daedalus_backend::{
    Backend, Incoming, MemorySpace, PortRef, SpaceKind, Transfer, TransferCost,
};
use daedalus_graph::NodeKindDoc;
use daedalus_registry::{Registry, TransferClass, TransferRecord};
use daedalus_types::{NodeIndex, PortIndex, TypeKey};

use super::StitchError;
use crate::regions::Formed;
use crate::solve::convert::key_of;
use crate::solve::{Graph, Port};

/// Paths longer than this aren't considered.
const MAX_HOPS: usize = 6;

/// Who can make a transfer's step.
#[derive(Clone, Copy, Debug)]
pub enum MoveOwner {
    /// Index into the backend's own `transfers()`.
    Backend { backend: u8, index: u32 },
    /// A `#[transfer]`. Whoever loads the plan binds its body.
    Record(&'static TransferRecord),
}

#[derive(Clone, Debug)]
pub struct MoveEntry {
    pub id: String,
    pub key: Option<TypeKey>,
    pub from: usize,
    pub to: usize,
    pub cost: TransferCost,
    pub owner: MoveOwner,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Source {
    Node {
        node: u32,
        port: u16,
    },
    /// A host bridge's output.
    Host {
        node: u32,
        port: String,
    },
}

#[derive(Clone, Debug)]
pub struct Value {
    pub source: Source,
    pub key: Option<TypeKey>,
    /// The value's type, by the producing port.
    pub ty: daedalus_registry::TypeRef,
    /// Space index: where the value is written.
    pub home: usize,
}

/// How one reader gets a value.
#[derive(Clone, Debug)]
pub struct Reader {
    pub value: u32,
    pub space: usize,
    /// Indexes into `Routing::table`.
    pub path: Vec<usize>,
}

#[derive(Clone, Debug)]
pub struct HostOutput {
    pub node: u32,
    pub port: String,
    pub reader: Reader,
}

/// Every decision about memory, made before compile so a backend can absorb a move.
#[derive(Clone, Debug, Default)]
pub struct Routing {
    pub spaces: Vec<String>,
    pub table: Vec<MoveEntry>,
    pub values: Vec<Value>,
    pub readers: BTreeMap<PortRef, Reader>,
    pub host_outputs: Vec<HostOutput>,
    /// For each region, the inputs a one-step move reaches, in `Region::incoming` order.
    pub incoming: Vec<Vec<PortRef>>,
}

impl Routing {
    pub(crate) fn incoming_of(&self, region: usize) -> Vec<Incoming<'_>> {
        self.incoming[region]
            .iter()
            .map(|input| {
                let reader = &self.readers[input];
                let entry = &self.table[reader.path[0]];
                Incoming {
                    input: *input,
                    from: &self.spaces[entry.from],
                    to: &self.spaces[entry.to],
                    transfer: &entry.id,
                }
            })
            .collect()
    }

    fn space(&mut self, id: &str) -> usize {
        match self.spaces.iter().position(|known| known == id) {
            Some(at) => at,
            None => {
                self.spaces.push(id.to_string());
                self.spaces.len() - 1
            }
        }
    }
}

fn class_cost(class: TransferClass) -> TransferCost {
    match class {
        TransferClass::Handoff => TransferCost::Handoff,
        TransferClass::Import => TransferCost::Import,
        TransferClass::Copy => TransferCost::Copy,
    }
}

fn add_entry(routing: &mut Routing, transfer: &Transfer, backend: u8, index: u32) {
    let (from, to) = (routing.space(&transfer.from), routing.space(&transfer.to));
    routing.table.push(MoveEntry {
        id: transfer.id.to_string(),
        key: transfer.key.clone(),
        from,
        to,
        cost: transfer.cost,
        owner: MoveOwner::Backend { backend, index },
    });
}

fn build_table(routing: &mut Routing, backends: &[&dyn Backend], registry: &Registry) {
    for (rank, backend) in backends.iter().enumerate() {
        for (index, transfer) in backend.transfers().iter().enumerate() {
            add_entry(routing, transfer, rank as u8, index as u32);
        }
    }
    for record in registry.transfers() {
        let (from, to) = (routing.space(record.from), routing.space(record.to));
        routing.table.push(MoveEntry {
            id: record.id.to_string(),
            key: Some(record.key.clone()),
            from,
            to,
            cost: class_cost(record.class),
            owner: MoveOwner::Record(record),
        });
    }
}

type PathKey = (TransferCost, usize, Vec<String>, Vec<String>);

/// The moves usable for this type: for a given pair of spaces, a typed one hides any-type ones.
fn usable(table: &[MoveEntry], key: Option<&TypeKey>) -> Vec<usize> {
    let typed = |entry: &MoveEntry| entry.key.is_some() && entry.key.as_ref() == key;
    (0..table.len())
        .filter(|&at| {
            let entry = &table[at];
            typed(entry)
                || (entry.key.is_none()
                    && !table.iter().any(|other| {
                        other.from == entry.from && other.to == entry.to && typed(other)
                    }))
        })
        .collect()
}

struct Finder<'a> {
    table: &'a [MoveEntry],
    spaces: &'a [String],
    moves: Vec<usize>,
    targets: &'a [usize],
    best: Option<(PathKey, Vec<usize>)>,
}

impl Finder<'_> {
    fn key_of(&self, path: &[usize], start: usize) -> PathKey {
        let cost = path
            .iter()
            .map(|&at| self.table[at].cost)
            .max()
            .unwrap_or(TransferCost::Handoff);
        let mut route = vec![self.spaces[start].clone()];
        route.extend(
            path.iter()
                .map(|&at| self.spaces[self.table[at].to].clone()),
        );
        let ids = path.iter().map(|&at| self.table[at].id.clone()).collect();
        (cost, path.len(), route, ids)
    }

    fn walk(&mut self, start: usize, at: usize, path: &mut Vec<usize>, seen: &mut Vec<usize>) {
        if self.targets.contains(&at) && !path.is_empty() {
            let key = self.key_of(path, start);
            if self.best.as_ref().is_none_or(|(known, _)| key < *known) {
                self.best = Some((key, path.clone()));
            }
            return;
        }
        if path.len() == MAX_HOPS {
            return;
        }
        for next in self.moves.clone() {
            let to = self.table[next].to;
            if self.table[next].from == at && !seen.contains(&to) {
                path.push(next);
                seen.push(to);
                self.walk(start, to, path, seen);
                seen.pop();
                path.pop();
            }
        }
    }
}

/// The best path from `from` to any of `targets`.
fn find_path(
    routing: &Routing,
    key: Option<&TypeKey>,
    from: usize,
    targets: &[usize],
) -> Option<(usize, Vec<usize>)> {
    let mut finder = Finder {
        table: &routing.table,
        spaces: &routing.spaces,
        moves: usable(&routing.table, key),
        targets,
        best: None,
    };
    finder.walk(from, from, &mut Vec::new(), &mut vec![from]);
    let (_, path) = finder.best?;
    let last = *path.last()?;
    Some((routing.table[last].to, path))
}

fn port_of(ports: &[Port], name: &str) -> Option<u16> {
    ports
        .iter()
        .position(|port| port.name == name)
        .map(|at| at as u16)
}

/// One backend's memory, by space index.
struct BackendSpaces {
    list: Vec<(MemorySpace, usize)>,
}

impl BackendSpaces {
    fn home(&self) -> Option<usize> {
        self.list.first().map(|(_, at)| *at)
    }

    fn readable(&self) -> Vec<usize> {
        self.list
            .iter()
            .filter(|(s, _)| s.readable)
            .map(|(_, at)| *at)
            .collect()
    }

    fn host_kind(&self) -> Option<usize> {
        self.list
            .iter()
            .find(|(s, _)| s.kind == SpaceKind::Host)
            .map(|(_, at)| *at)
    }
}

struct Router<'a> {
    graph: &'a Graph,
    formed: &'a [Formed],
    backends: Vec<BackendSpaces>,
    names: Vec<String>,
    region_of: Vec<Option<usize>>,
    values: BTreeMap<(Source, usize), u32>,
    routing: Routing,
}

impl Router<'_> {
    fn value(&mut self, source: Source, port: &Port, home: usize) -> u32 {
        let routing = &mut self.routing;
        *self
            .values
            .entry((source.clone(), home))
            .or_insert_with(|| {
                routing.values.push(Value {
                    source,
                    key: key_of(&port.ty),
                    ty: port.ty.clone(),
                    home,
                });
                routing.values.len() as u32 - 1
            })
    }

    fn deliver(&self, value: u32, targets: &[usize], port: &str) -> Result<Reader, StitchError> {
        let info = &self.routing.values[value as usize];
        if targets.contains(&info.home) {
            return Ok(Reader {
                value,
                space: info.home,
                path: Vec::new(),
            });
        }
        match find_path(&self.routing, info.key.as_ref(), info.home, targets) {
            Some((space, path)) => Ok(Reader { value, space, path }),
            None => Err(StitchError::NoTransfer {
                port: port.to_string(),
                from_space: self.routing.spaces[info.home].clone(),
                to_space: targets
                    .first()
                    .map_or_else(String::new, |&at| self.routing.spaces[at].clone()),
            }),
        }
    }

    fn memory_of(&self, region: usize) -> Result<&BackendSpaces, StitchError> {
        let rank = usize::from(self.formed[region].backend.0);
        let spaces = &self.backends[rank];
        if spaces.list.is_empty() {
            return Err(StitchError::NoMemory {
                backend: self.names[rank].clone(),
            });
        }
        Ok(spaces)
    }

    /// Gives an edge inside a region a value in the region's own memory. It only takes a slot if
    /// the backend's units name its ports, as they do when a region runs as separate steps.
    fn inside_regions(&mut self) -> Result<(), StitchError> {
        let graph = self.graph;
        for edge in &graph.edges {
            let (from, to) = (edge.from.node as usize, edge.to.node as usize);
            let (Some(region), true) = (
                self.region_of[to],
                self.region_of[to] == self.region_of[from],
            ) else {
                continue;
            };
            let (producer, consumer) = (&graph.nodes[from], &graph.nodes[to]);
            let (Some(out), Some(input)) = (
                port_of(&producer.outputs, &edge.from.port),
                port_of(&consumer.inputs, &edge.to.port),
            ) else {
                continue;
            };
            let Some(home) = self.memory_of(region)?.home() else {
                continue;
            };
            let source = Source::Node {
                node: edge.from.node,
                port: out,
            };
            let value = self.value(source, &producer.outputs[out as usize], home);
            let port = PortRef {
                node: NodeIndex(edge.to.node),
                port: PortIndex(input),
            };
            self.routing.readers.insert(
                port,
                Reader {
                    value,
                    space: home,
                    path: Vec::new(),
                },
            );
        }
        Ok(())
    }

    fn region_input(&mut self, region: usize, input: PortRef) -> Result<(), StitchError> {
        let graph = self.graph;
        let node = &graph.nodes[input.node.0 as usize];
        let port = &node.inputs[input.port.0 as usize];
        let Some(edge) = graph
            .edges
            .iter()
            .find(|e| e.to.node == input.node.0 && e.to.port == port.name)
        else {
            return Ok(());
        };
        let producer = &graph.nodes[edge.from.node as usize];
        let Some(out) = port_of(&producer.outputs, &edge.from.port) else {
            return Ok(());
        };
        let produced = &producer.outputs[out as usize];
        let consumer = self.memory_of(region)?;
        let targets = consumer.readable();
        let (source, home) = if producer.doc.kind == NodeKindDoc::HostBridge {
            let home = consumer
                .host_kind()
                .or(targets.first().copied())
                .or(consumer.home());
            let source = Source::Host {
                node: edge.from.node,
                port: edge.from.port.clone(),
            };
            (source, home)
        } else {
            let from = self.region_of[edge.from.node as usize];
            let home = match from {
                Some(from) => self.memory_of(from)?.home(),
                None => None,
            };
            (
                Source::Node {
                    node: edge.from.node,
                    port: out,
                },
                home,
            )
        };
        let Some(home) = home else {
            return Ok(());
        };
        let value = self.value(source, produced, home);
        let name = format!("{}.{}", node.doc.id, port.name);
        let reader = self.deliver(value, &targets, &name)?;
        if reader.path.len() == 1 {
            self.routing.incoming[region].push(input);
        }
        self.routing.readers.insert(input, reader);
        Ok(())
    }

    fn host_output(&mut self, region: usize, output: PortRef) -> Result<(), StitchError> {
        let graph = self.graph;
        let producer = &graph.nodes[output.node.0 as usize];
        let port = &producer.outputs[output.port.0 as usize];
        let memory = self.memory_of(region)?;
        let (Some(home), host) = (memory.home(), memory.host_kind()) else {
            return Ok(());
        };
        let targets = vec![host.unwrap_or(home)];
        let source = Source::Node {
            node: output.node.0,
            port: output.port.0,
        };
        let value = self.value(source, port, home);
        for edge in graph.edges.iter().filter(|e| {
            e.from.node == output.node.0
                && e.from.port == port.name
                && graph.nodes[e.to.node as usize].doc.kind == NodeKindDoc::HostBridge
        }) {
            let name = format!("{}.{}", producer.doc.id, port.name);
            let reader = self.deliver(value, &targets, &name)?;
            self.routing.host_outputs.push(HostOutput {
                node: edge.to.node,
                port: edge.to.port.clone(),
                reader,
            });
        }
        Ok(())
    }
}

/// Decides where every boundary value lives and how it moves.
pub fn route(
    graph: &Graph,
    formed: &[Formed],
    backends: &[&dyn Backend],
    registry: &Registry,
) -> Result<Routing, StitchError> {
    let mut routing = Routing {
        incoming: vec![Vec::new(); formed.len()],
        ..Routing::default()
    };
    let memory: Vec<BackendSpaces> = backends
        .iter()
        .map(|backend| BackendSpaces {
            list: backend
                .memory()
                .into_iter()
                .map(|space| {
                    let at = routing.space(&space.id);
                    (space, at)
                })
                .collect(),
        })
        .collect();
    build_table(&mut routing, backends, registry);
    let mut region_of = vec![None; graph.nodes.len()];
    for (region, one) in formed.iter().enumerate() {
        for &node in &one.nodes {
            region_of[node as usize] = Some(region);
        }
    }
    let mut router = Router {
        graph,
        formed,
        backends: memory,
        names: backends.iter().map(|b| b.id().to_string()).collect(),
        region_of,
        values: BTreeMap::new(),
        routing,
    };
    for (region, one) in formed.iter().enumerate() {
        for input in &one.inputs {
            router.region_input(region, *input)?;
        }
    }
    router.inside_regions()?;
    for (region, one) in formed.iter().enumerate() {
        for output in &one.outputs {
            router.host_output(region, *output)?;
        }
    }
    Ok(router.routing)
}

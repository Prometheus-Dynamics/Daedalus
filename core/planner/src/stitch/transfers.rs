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

    /// For each of a region's outputs, the space its value is written in. Empty when none of
    /// them has a value, which is when nothing reads them.
    pub(crate) fn placed_of(&self, outputs: &[PortRef]) -> Vec<&str> {
        let home = |output: &PortRef| {
            let source = Source::Node {
                node: output.node.0,
                port: output.port.0,
            };
            self.values
                .iter()
                .find(|value| value.source == source)
                .map(|value| self.spaces[value.home].as_str())
        };
        let placed: Vec<Option<&str>> = outputs.iter().map(home).collect();
        // An output nothing reads is written wherever the others are, or in the first space.
        let Some(known) = placed.iter().flatten().next().copied() else {
            return Vec::new();
        };
        placed
            .into_iter()
            .map(|space| space.unwrap_or(known))
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

/// What reaching a value costs a reader: its dearest move, or none, and how many moves.
type Reach = (Option<TransferCost>, usize);

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
    /// The other spaces an output can be written in, by index, in the backend's order.
    placements: Vec<usize>,
}

impl BackendSpaces {
    fn home(&self) -> Option<usize> {
        self.list.first().map(|(_, at)| *at)
    }

    /// Every space an output of this backend can be written in: its first, then the others.
    fn writable(&self) -> Vec<usize> {
        let mut spaces: Vec<usize> = self.home().into_iter().collect();
        for &at in &self.placements {
            if !spaces.contains(&at) {
                spaces.push(at);
            }
        }
        spaces
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
    registry: &'a Registry,
    formed: &'a [Formed],
    backends: Vec<BackendSpaces>,
    names: Vec<String>,
    region_of: Vec<Option<usize>>,
    values: BTreeMap<(Source, usize), u32>,
    /// Where each node output is written, once decided.
    homes: BTreeMap<(u32, u16), Option<usize>>,
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

    /// What it costs a reader that takes its input from one of `targets` to get a value written
    /// in `home`: nothing when it is already there, the dearest move of the best path
    /// otherwise, and `None` when there is no way.
    fn reach(&self, key: Option<&TypeKey>, home: usize, targets: &[usize]) -> Option<Reach> {
        if targets.contains(&home) {
            return Some((None, 0));
        }
        let (_, path) = find_path(&self.routing, key, home, targets)?;
        let dearest = path.iter().map(|&at| self.routing.table[at].cost).max();
        Some((dearest, path.len()))
    }

    /// Where the output `port` of `node` is written: the space of its backend that costs its
    /// readers least to read from. The backend's first space wins a tie, so a backend with
    /// nowhere else to write, or readers that are all its own, is placed as before.
    fn home_of(&mut self, node: u32, port: u16) -> Result<Option<usize>, StitchError> {
        if let Some(known) = self.homes.get(&(node, port)) {
            return Ok(*known);
        }
        let home = self.choose_home(node, port)?;
        self.homes.insert((node, port), home);
        Ok(home)
    }

    /// The space the body of `node` says its port `port` is in (`memory(port = "space")` on the
    /// node), or `None` when it says nothing and the backend's own spaces apply.
    fn declared(&self, node: u32, port: &str) -> Result<Option<usize>, StitchError> {
        let Some(region) = self.region_of[node as usize] else {
            return Ok(None);
        };
        let backend = &self.names[self.formed[region].backend.0 as usize];
        let id = &self.graph.nodes[node as usize].doc.id;
        let space = self.registry.entry(id).and_then(|entry| {
            let body = entry.bodies.iter().find(|body| body.backend == *backend)?;
            let (_, space) = body.memory.iter().find(|(name, _)| *name == port)?;
            Some(*space)
        });
        let Some(space) = space else {
            return Ok(None);
        };
        // A space a transfer only names is not enough: some backend has to supply it.
        let supplied = self
            .backends
            .iter()
            .flat_map(|backend| &backend.list)
            .find(|(known, _)| known.id == space)
            .map(|(_, at)| *at);
        match supplied {
            Some(at) => Ok(Some(at)),
            None => Err(StitchError::UnknownSpace {
                port: format!("{id}.{port}"),
                space: space.to_string(),
            }),
        }
    }

    fn choose_home(&self, node: u32, port: u16) -> Result<Option<usize>, StitchError> {
        let Some(region) = self.region_of[node as usize] else {
            return Ok(None);
        };
        let name = &self.graph.nodes[node as usize].outputs[port as usize].name;
        if let Some(declared) = self.declared(node, name)? {
            return Ok(Some(declared));
        }
        let memory = self.memory_of(region)?;
        let candidates = memory.writable();
        if candidates.len() < 2 {
            return Ok(memory.home());
        }
        let graph = self.graph;
        let produced = &graph.nodes[node as usize].outputs[port as usize];
        let key = key_of(&produced.ty);
        // Each reader, as the spaces it can take the value from.
        let mut readers: Vec<Vec<usize>> = Vec::new();
        let leaving = graph
            .edges
            .iter()
            .filter(|edge| edge.from.node == node && edge.from.port == produced.name);
        for edge in leaving {
            let consumer = &graph.nodes[edge.to.node as usize];
            if consumer.doc.kind == NodeKindDoc::HostBridge {
                readers.extend(memory.host_kind().map(|host| vec![host]));
            } else if let Some(other) = self.region_of[edge.to.node as usize] {
                readers.push(match self.declared(edge.to.node, &edge.to.port)? {
                    Some(declared) => vec![declared],
                    None => self.memory_of(other)?.readable(),
                });
            }
        }
        let cost = |home: usize| -> Option<(Option<TransferCost>, usize)> {
            let mut worst = (None, 0);
            for targets in &readers {
                let one = self.reach(key.as_ref(), home, targets)?;
                worst = (worst.0.max(one.0), worst.1 + one.1);
            }
            Some(worst)
        };
        let best = candidates
            .iter()
            .enumerate()
            .filter_map(|(order, &home)| Some((cost(home)?, order, home)))
            .min();
        Ok(best.map(|(_, _, home)| home).or(memory.home()))
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
                from_port: match &info.source {
                    Source::Node { node, port } => {
                        let from = &self.graph.nodes[*node as usize];
                        format!("{}.{}", from.doc.id, from.outputs[*port as usize].name)
                    }
                    Source::Host { node, port } => {
                        format!("{}.{port}", self.graph.nodes[*node as usize].doc.id)
                    }
                },
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
            let _ = region;
            let Some(home) = self.home_of(edge.from.node, out)? else {
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
        let targets = match self.declared(input.node.0, &port.name)? {
            Some(declared) => vec![declared],
            None => consumer.readable(),
        };
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
            let home = self.home_of(edge.from.node, out)?;
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
        let (Some(first), host) = (memory.home(), memory.host_kind()) else {
            return Ok(());
        };
        let targets = vec![host.unwrap_or(first)];
        let Some(home) = self.home_of(output.node.0, output.port.0)? else {
            return Ok(());
        };
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
        .map(|backend| {
            let list: Vec<(MemorySpace, usize)> = backend
                .memory()
                .into_iter()
                .map(|space| {
                    let at = routing.space(&space.id);
                    (space, at)
                })
                .collect();
            // A placement has to be one of the backend's own spaces.
            let placements = backend
                .placements()
                .iter()
                .filter_map(|id| list.iter().find(|(space, _)| space.id == *id))
                .map(|(_, at)| *at)
                .collect();
            BackendSpaces { list, placements }
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
        registry,
        formed,
        backends: memory,
        names: backends.iter().map(|b| b.id().to_string()).collect(),
        region_of,
        values: BTreeMap::new(),
        homes: BTreeMap::new(),
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

#[cfg(test)]
mod tests {
    use daedalus_backend::TransferCost;

    use super::{MoveEntry, MoveOwner, Routing, find_path};

    fn entry(id: &str, from: usize, to: usize, cost: TransferCost) -> MoveEntry {
        MoveEntry {
            id: id.into(),
            key: None,
            from,
            to,
            cost,
            owner: MoveOwner::Backend {
                backend: 0,
                index: 0,
            },
        }
    }

    /// Device memory reaches the host two ways: a readback into host-mapped memory, which the
    /// host reads uncached, or a readback into dma-heap memory, which it reads through its cache.
    /// Each then maps to the host. The planner takes the heap route, by cost.
    #[test]
    fn the_cached_readback_route_beats_the_mapped_one() {
        let routing = Routing {
            spaces: vec![
                "wgpu.buffer".into(),
                "wgpu.mapped".into(),
                "wgpu.dmaheap".into(),
                "cpu.host".into(),
            ],
            table: vec![
                entry("wgpu.readback", 0, 1, TransferCost::Copy),
                entry("wgpu.readback.heap", 0, 2, TransferCost::CopyCached),
                entry("wgpu.map", 1, 3, TransferCost::Import),
                entry("wgpu.map.dmaheap", 2, 3, TransferCost::Import),
            ],
            ..Routing::default()
        };
        let (_, path) = find_path(&routing, None, 0, &[3]).expect("a route");
        let ids: Vec<&str> = path
            .iter()
            .map(|&at| routing.table[at].id.as_str())
            .collect();
        assert_eq!(ids, ["wgpu.readback.heap", "wgpu.map.dmaheap"]);
    }

    /// Without the heap move, the mapped route is the only one.
    #[test]
    fn without_the_heap_move_the_mapped_route_is_taken() {
        let routing = Routing {
            spaces: vec![
                "wgpu.buffer".into(),
                "wgpu.mapped".into(),
                "cpu.host".into(),
            ],
            table: vec![
                entry("wgpu.readback", 0, 1, TransferCost::Copy),
                entry("wgpu.map", 1, 2, TransferCost::Import),
            ],
            ..Routing::default()
        };
        let (_, path) = find_path(&routing, None, 0, &[2]).expect("a route");
        assert_eq!(path.len(), 2);
    }
}

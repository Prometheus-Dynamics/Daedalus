//! [`ExecutionDomain`]: several separately compiled [`HostGraph`]s ticked together on one thread,
//! with one graph's host outputs fanned out to other graphs' host inputs, so a shared upstream
//! (a camera's mask and pyramid) runs once per frame for every downstream graph (AprilTag,
//! ArUco, ML) instead of once per graph.
//!
//! - **One tick** ([`ExecutionDomain::tick`]) visits the graphs in link order (upstreams first,
//!   ties in the order they were added): a graph with pending host input ticks once, then each of
//!   its linked outputs is taken and fed to every linked input. Feeding is an `Arc` clone of the
//!   payload (no copy, no allocation per tick); the downstream graph's own planned adapters
//!   apply when its input type differs. Every fed graph shares the payload, so nodes read it by
//!   reference; a graph whose nodes take a linked (or multiply routed) input by value declares
//!   it shared (`GraphBuilder::shared_input`) to get a planned copy.
//! - **Backpressure** per link ([`LinkMode`]): latest-only by default (a downstream that has not
//!   consumed the previous value gets it replaced), every value in order, or held context.
//! - **Failure isolation:** a failing graph never stops the others. Its error is kept
//!   ([`ExecutionDomain::last_error`]), its linked outputs of that tick are dropped, and every
//!   graph downstream of it is skipped for the tick with its fed inputs cleared, so no graph sees
//!   a frame without the shared results computed from it. Unrelated graphs run as usual.
//! - **Runtime changes** between ticks: add, remove, link and unlink graphs without recompiling
//!   any of them.
//! - **Structural sharing** ([`ExecutionDomain::load_shared`], `plugins`): load several graphs
//!   and the domain finds the node subgraphs they compute identically (nodes marked
//!   [`NODE_SHAREABLE_META_KEY`](daedalus_runtime::NODE_SHAREABLE_META_KEY)) and runs them once.
//!
//! Domain inputs ([`ExecutionDomain::route_input`]) fan one push out to several graphs (the
//! camera frame to the upstream and to detectors that also read raw pixels).

use crate::prelude::*;
use core::time::Duration;

use daedalus_core::platform::Clock;
use daedalus_runtime::executor::NodeHandler;
use daedalus_runtime::handles::PortId;
use daedalus_runtime::host_bridge::HostBridgeHandle;
use daedalus_transport::{FeedOutcome, Payload, TypeKey};

use crate::error::EngineError;
use crate::host_graph::HostGraph;

mod explain;
mod stats;
#[cfg(feature = "plugins")]
mod structural;

pub use explain::{
    DomainExplanation, DomainGraphExplanation, DomainInputExplanation, DomainLinkExplanation,
    DomainSharedNode,
};
pub use stats::{DomainGraphStats, DomainOverhead, DomainStats, DomainTick};
#[cfg(feature = "plugins")]
pub use structural::{SHARED_UPSTREAM_GRAPH, is_shareable};

/// How a link hands an upstream output to a downstream input.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum LinkMode {
    /// Only the newest value of the tick is forwarded, and the downstream input is made
    /// latest-only: a value the downstream has not consumed yet is replaced. Live streams.
    #[default]
    Latest,
    /// Every value, in order; the downstream input's own pressure policy applies.
    All,
    /// The newest value, kept by the downstream as held context (`HostGraph::set_held_input`):
    /// seen every tick until replaced, never a trigger by itself.
    Held,
}

impl LinkMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Latest => "latest",
            Self::All => "all",
            Self::Held => "held",
        }
    }
}

/// A downstream input fed by a source port.
struct Target {
    member: usize,
    port: PortId,
    host: HostBridgeHandle,
    mode: LinkMode,
    /// Upstream output and downstream input type keys when the plans resolved them.
    types: (Option<TypeKey>, Option<TypeKey>),
}

/// A host-side reader of a source port's newest value (a tap or a structural alias).
struct Tap {
    graph: String,
    port: String,
    latest: Option<Payload>,
}

/// One linked host output of a member.
struct Source {
    port: PortId,
    targets: Vec<Target>,
    taps: Vec<Tap>,
}

struct Member<H: NodeHandler> {
    name: String,
    graph: HostGraph<H>,
    sources: Vec<Source>,
    /// Non-held inputs fed by links or routes: cleared when the graph is skipped.
    fed: Vec<PortId>,
    blocked: bool,
    stats: DomainGraphStats,
    last_error: Option<EngineError>,
}

/// A domain input and the graph inputs it feeds.
struct Route {
    name: PortId,
    targets: Vec<(usize, PortId, HostBridgeHandle)>,
}

/// Graphs ticked together on one thread, linked host output to host input. See the module docs.
pub struct ExecutionDomain<H: NodeHandler> {
    members: Vec<Member<H>>,
    order: Vec<usize>,
    routes: Vec<Route>,
    clock: Clock,
    ticks: u64,
    wall_time: Duration,
    overhead_window: Option<usize>,
    shared_nodes: Vec<DomainSharedNode>,
}

impl<H: NodeHandler> Default for ExecutionDomain<H> {
    fn default() -> Self {
        Self {
            members: Vec::new(),
            order: Vec::new(),
            routes: Vec::new(),
            clock: Clock::default(),
            ticks: 0,
            wall_time: Duration::ZERO,
            overhead_window: None,
            shared_nodes: Vec::new(),
        }
    }
}

fn config(message: impl Into<String>) -> EngineError {
    EngineError::Config(message.into())
}

impl<H: NodeHandler + Send + Sync + 'static> ExecutionDomain<H> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Time domain ticks with `clock` (graphs time their own ticks with their executor clock).
    pub fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// Add `graph` under `name`. It runs after the graphs it is linked from; until linked it is
    /// independent and ticks whenever it has pending input.
    pub fn add_graph(
        &mut self,
        name: impl Into<String>,
        mut graph: HostGraph<H>,
    ) -> Result<(), EngineError> {
        let name = name.into();
        if self.index(&name).is_some() {
            return Err(config(format!("domain already has a graph named '{name}'")));
        }
        if let Some(window) = self.overhead_window {
            graph.enable_frame_overhead(window);
        }
        self.members.push(Member {
            stats: DomainGraphStats {
                name: name.clone(),
                nodes: graph
                    .runtime_plan()
                    .nodes
                    .iter()
                    .filter(|node| !daedalus_planner::is_host_bridge_metadata(&node.metadata))
                    .count(),
                ..DomainGraphStats::default()
            },
            name,
            graph,
            sources: Vec::new(),
            fed: Vec::new(),
            blocked: false,
            last_error: None,
        });
        self.refresh()
    }

    /// Remove graph `name` with its links, routes and taps, and hand it back. Graphs it fed keep
    /// running on their other inputs.
    pub fn remove_graph(&mut self, name: &str) -> Result<HostGraph<H>, EngineError> {
        let index = self.required(name)?;
        let member = self.members.remove(index);
        let shift = |at: &mut usize| *at -= usize::from(*at > index);
        for other in &mut self.members {
            for source in &mut other.sources {
                source.targets.retain(|target| target.member != index);
                source.targets.iter_mut().for_each(|t| shift(&mut t.member));
                source.taps.retain(|tap| tap.graph != name);
            }
            other
                .sources
                .retain(|source| !source.targets.is_empty() || !source.taps.is_empty());
        }
        for route in &mut self.routes {
            route.targets.retain(|target| target.0 != index);
            route.targets.iter_mut().for_each(|t| shift(&mut t.0));
        }
        self.routes.retain(|route| !route.targets.is_empty());
        self.refresh()?;
        Ok(member.graph)
    }

    /// Feed host output `from_port` of graph `from` to host input `to_port` of graph `to` on
    /// every tick (see [`LinkMode`]). Fails for unknown graphs or ports, or a link cycle.
    pub fn link(
        &mut self,
        from: &str,
        from_port: impl Into<PortId>,
        to: &str,
        to_port: impl Into<PortId>,
        mode: LinkMode,
    ) -> Result<(), EngineError> {
        let (from_port, to_port) = (from_port.into(), to_port.into());
        let (source, member) = (self.required(from)?, self.required(to)?);
        if source == member {
            return Err(config(format!("graph '{from}' cannot feed itself")));
        }
        let from_type = self.port_type(source, &from_port, false)?;
        let to_type = self.port_type(member, &to_port, true)?;
        let host = self.members[member].graph.host().clone();
        let source_entry = self.source_mut(source, &from_port);
        if source_entry
            .targets
            .iter()
            .any(|t| t.member == member && t.port == to_port)
        {
            return Err(config(format!(
                "'{from}.{from_port}' already feeds '{to}.{to_port}'"
            )));
        }
        source_entry.targets.push(Target {
            member,
            port: to_port.clone(),
            host,
            mode,
            types: (from_type, to_type),
        });
        if let Err(error) = self.refresh() {
            self.unlink(from, from_port.as_str(), to, to_port.as_str());
            return Err(error);
        }
        let graph = &self.members[member].graph;
        match mode {
            LinkMode::Latest => graph
                .set_latest_input(to_port)
                .map_err(|error| config(error.to_string()))?,
            LinkMode::Held => graph.set_held_input(to_port),
            LinkMode::All => {}
        }
        Ok(())
    }

    /// Remove a link; `false` when there was none.
    pub fn unlink(&mut self, from: &str, from_port: &str, to: &str, to_port: &str) -> bool {
        let (Some(source), Some(member)) = (self.index(from), self.index(to)) else {
            return false;
        };
        let sources = &mut self.members[source].sources;
        let Some(at) = sources.iter().position(|s| s.port.as_str() == from_port) else {
            return false;
        };
        let before = sources[at].targets.len();
        sources[at]
            .targets
            .retain(|t| !(t.member == member && t.port.as_str() == to_port));
        let removed = sources[at].targets.len() != before;
        if sources[at].targets.is_empty() && sources[at].taps.is_empty() {
            sources.remove(at);
        }
        let _ = self.refresh();
        removed
    }

    /// Route domain input `input` to host input `port` of graph `graph`: each
    /// [`Self::push_payload`] to `input` feeds every routed graph input (an `Arc` clone each).
    pub fn route_input(
        &mut self,
        input: impl Into<PortId>,
        graph: &str,
        port: impl Into<PortId>,
    ) -> Result<(), EngineError> {
        let (input, port) = (input.into(), port.into());
        let member = self.required(graph)?;
        self.port_type(member, &port, true)?;
        let host = self.members[member].graph.host().clone();
        let route = match self.routes.iter().position(|route| route.name == input) {
            Some(at) => &mut self.routes[at],
            None => {
                self.routes.push(Route {
                    name: input,
                    targets: Vec::new(),
                });
                self.routes.last_mut().expect("just pushed")
            }
        };
        if !route.targets.iter().any(|t| t.0 == member && t.1 == port) {
            route.targets.push((member, port, host));
        }
        self.refresh()
    }

    /// Keep the newest value of graph `graph`'s linked or unlinked host output `port` each tick
    /// for [`Self::take_payload`] (the domain takes the port's values from the graph).
    pub fn tap(&mut self, graph: &str, port: impl Into<PortId>) -> Result<(), EngineError> {
        let port = port.into();
        let member = self.required(graph)?;
        self.port_type(member, &port, false)?;
        self.add_tap(member, port, graph.to_string(), None)
    }

    fn add_tap(
        &mut self,
        member: usize,
        port: PortId,
        graph: String,
        reader_port: Option<String>,
    ) -> Result<(), EngineError> {
        let reader_port = reader_port.unwrap_or_else(|| port.as_str().to_string());
        let source = self.source_mut(member, &port);
        if !source
            .taps
            .iter()
            .any(|tap| tap.graph == graph && tap.port == reader_port)
        {
            source.taps.push(Tap {
                graph,
                port: reader_port,
                latest: None,
            });
        }
        Ok(())
    }

    /// Push `payload` to every graph input routed from domain input `input`; returns how many
    /// accepted it (dropped values count as not accepted). A payload a graph's type check refuses
    /// is an error, after the other graphs were fed.
    pub fn push_payload(
        &self,
        input: impl AsRef<str>,
        payload: Payload,
    ) -> Result<usize, EngineError> {
        let input = input.as_ref();
        let route = self
            .routes
            .iter()
            .find(|route| route.name.as_str() == input)
            .ok_or_else(|| config(format!("domain has no input '{input}'")))?;
        let mut accepted = 0;
        let mut rejected = None;
        let last = route.targets.len().saturating_sub(1);
        let mut payload = Some(payload);
        for (at, (_, port, host)) in route.targets.iter().enumerate() {
            let value = if at == last {
                payload.take()
            } else {
                payload.clone()
            };
            let Some(value) = value else { break };
            match host.feed_payload(port.clone(), value) {
                FeedOutcome::Accepted { .. } | FeedOutcome::Replaced { .. } => accepted += 1,
                FeedOutcome::Rejected(error) => rejected = Some(error),
                _ => {}
            }
        }
        match rejected {
            Some(error) => Err((*error).into()),
            None => Ok(accepted),
        }
    }

    /// [`Self::push_payload`] of `value` under the key the first routed graph's registry gives
    /// `T`.
    pub fn push<T: Send + Sync + 'static>(
        &self,
        input: impl AsRef<str>,
        value: T,
    ) -> Result<usize, EngineError> {
        let input = input.as_ref();
        let graph = self
            .routes
            .iter()
            .find(|route| route.name.as_str() == input)
            .and_then(|route| route.targets.first())
            .map(|target| &self.members[target.0].graph)
            .ok_or_else(|| config(format!("domain has no input '{input}'")))?;
        let payload = Payload::owned(graph.type_index().key_of::<T>()?, value).stamp(&self.clock);
        self.push_payload(input, payload)
    }

    /// Whether any graph has pending host input.
    pub fn has_pending_input(&self) -> bool {
        self.members
            .iter()
            .any(|member| member.graph.host().has_pending_inbound())
    }

    /// One domain tick: each graph with pending input ticks once, in link order, and its linked
    /// outputs are fed downstream right after. Failures are isolated (see the module docs) and
    /// counted in the returned summary; the errors stay on the graphs
    /// ([`Self::last_error`]). Allocation-free at steady state.
    pub fn tick(&mut self) -> DomainTick {
        let start = self.clock.now();
        for member in &mut self.members {
            member.blocked = false;
        }
        let mut tick = DomainTick::default();
        for position in 0..self.order.len() {
            let index = self.order[position];
            let member = &mut self.members[index];
            if member.blocked {
                for port in &member.fed {
                    member.graph.clear_input(port);
                }
                member.stats.skipped += 1;
                tick.skipped += 1;
                self.block_downstream(index);
                continue;
            }
            if !member.graph.host().has_pending_inbound() {
                tick.idle += 1;
                continue;
            }
            let clock = member.graph.runner.executor.clock().clone();
            let started = clock.now();
            let result = member.graph.tick();
            let elapsed = clock.elapsed(started);
            match result {
                Ok(_) => {
                    member.stats.record_run(elapsed);
                    member.forward();
                    tick.ran += 1;
                }
                Err(error) => {
                    member.stats.failures += 1;
                    member.last_error = Some(error);
                    member.drop_linked_outputs();
                    tick.failed += 1;
                    self.block_downstream(index);
                }
            }
        }
        tick.duration = self.clock.elapsed(start);
        self.ticks += 1;
        self.wall_time += tick.duration;
        tick
    }

    /// [`Self::tick`] when any graph has pending input.
    pub fn tick_if_ready(&mut self) -> Option<DomainTick> {
        self.has_pending_input().then(|| self.tick())
    }

    fn block_downstream(&mut self, index: usize) {
        for source in 0..self.members[index].sources.len() {
            for target in 0..self.members[index].sources[source].targets.len() {
                let member = self.members[index].sources[source].targets[target].member;
                self.members[member].blocked = true;
            }
        }
    }

    /// Take a value of graph `graph`'s host output `port`: the newest tapped value when the port
    /// is tapped or served by a shared upstream (structural sharing), else the graph's queue.
    pub fn take_payload(&mut self, graph: &str, port: &str) -> Option<Payload> {
        for member in &mut self.members {
            for source in &mut member.sources {
                if let Some(tap) = source
                    .taps
                    .iter_mut()
                    .find(|tap| tap.graph == graph && tap.port == port)
                {
                    return tap.latest.take();
                }
            }
        }
        self.graph(graph)?.take_payload(port)
    }

    /// The graph named `name`.
    pub fn graph(&self, name: &str) -> Option<&HostGraph<H>> {
        Some(&self.members[self.index(name)?].graph)
    }

    pub fn graph_mut(&mut self, name: &str) -> Option<&mut HostGraph<H>> {
        let index = self.index(name)?;
        Some(&mut self.members[index].graph)
    }

    /// Graph names in tick order.
    pub fn tick_order(&self) -> impl Iterator<Item = &str> {
        self.order.iter().map(|&i| self.members[i].name.as_str())
    }

    /// The error of graph `name`'s most recent failed tick (kept until it is cleared).
    pub fn last_error(&self, name: &str) -> Option<&EngineError> {
        self.members[self.index(name)?].last_error.as_ref()
    }

    /// Drop graph `name`'s kept error.
    pub fn clear_error(&mut self, name: &str) -> Option<EngineError> {
        let index = self.index(name)?;
        self.members[index].last_error.take()
    }

    fn index(&self, name: &str) -> Option<usize> {
        self.members.iter().position(|member| member.name == name)
    }

    fn required(&self, name: &str) -> Result<usize, EngineError> {
        self.index(name)
            .ok_or_else(|| config(format!("domain has no graph named '{name}'")))
    }

    fn source_mut(&mut self, member: usize, port: &PortId) -> &mut Source {
        let sources = &mut self.members[member].sources;
        let at = match sources.iter().position(|s| &s.port == port) {
            Some(at) => at,
            None => {
                sources.push(Source {
                    port: port.clone(),
                    targets: Vec::new(),
                    taps: Vec::new(),
                });
                sources.len() - 1
            }
        };
        &mut sources[at]
    }

    /// The type key of host input (`input`) or output `port` of a member; an error when the
    /// graph has no such port.
    fn port_type(
        &self,
        member: usize,
        port: &PortId,
        input: bool,
    ) -> Result<Option<TypeKey>, EngineError> {
        let graph = &self.members[member].graph;
        let ports = if input {
            graph.host_inputs()
        } else {
            graph.host_outputs()
        };
        let found = ports.into_iter().find(|p| p.name() == port.as_str());
        match found {
            Some(descriptor) => Ok(descriptor.type_key),
            None => Err(config(format!(
                "graph '{}' has no host {} '{port}'",
                self.members[member].name,
                if input { "input" } else { "output" }
            ))),
        }
    }

    /// Recompute the tick order, consumers and fed inputs after a change; a link cycle is an
    /// error (the order is left as it was).
    fn refresh(&mut self) -> Result<(), EngineError> {
        let count = self.members.len();
        let mut upstreams = vec![0usize; count];
        for member in &self.members {
            let mut seen = Vec::new();
            for target in member.sources.iter().flat_map(|s| &s.targets) {
                if !seen.contains(&target.member) {
                    seen.push(target.member);
                    upstreams[target.member] += 1;
                }
            }
        }
        let mut order = Vec::with_capacity(count);
        let mut placed = vec![false; count];
        while order.len() < count {
            let Some(next) = (0..count).find(|&i| !placed[i] && upstreams[i] == 0) else {
                return Err(config("graph links form a cycle"));
            };
            placed[next] = true;
            order.push(next);
            let mut seen = Vec::new();
            for target in self.members[next].sources.iter().flat_map(|s| &s.targets) {
                if !seen.contains(&target.member) {
                    seen.push(target.member);
                    upstreams[target.member] -= 1;
                }
            }
        }
        self.order = order;
        let mut fed: Vec<Vec<PortId>> = vec![Vec::new(); count];
        for member in &mut self.members {
            let mut consumers = Vec::new();
            for target in member.sources.iter().flat_map(|s| &s.targets) {
                if !consumers.contains(&target.member) {
                    consumers.push(target.member);
                }
                if target.mode != LinkMode::Held && !fed[target.member].contains(&target.port) {
                    fed[target.member].push(target.port.clone());
                }
            }
            member.stats.consumers = consumers.len();
        }
        for route in &self.routes {
            for (member, port, _) in &route.targets {
                if !fed[*member].contains(port) {
                    fed[*member].push(port.clone());
                }
            }
        }
        for (member, fed) in self.members.iter_mut().zip(fed) {
            member.fed = fed;
        }
        Ok(())
    }
}

impl<H: NodeHandler + Send + Sync + 'static> Member<H> {
    /// Take each linked output's values and feed them downstream (see [`LinkMode`]).
    fn forward(&mut self) {
        for source in &mut self.sources {
            let mut last = None;
            while let Some(payload) = self.graph.take_payload(&source.port) {
                for target in source.targets.iter().filter(|t| t.mode == LinkMode::All) {
                    feed(&mut self.stats, target, payload.clone());
                }
                last = Some(payload);
            }
            let Some(payload) = last else { continue };
            for target in source.targets.iter().filter(|t| t.mode != LinkMode::All) {
                feed(&mut self.stats, target, payload.clone());
            }
            for tap in &mut source.taps {
                tap.latest = Some(payload.clone());
            }
        }
    }

    fn drop_linked_outputs(&mut self) {
        for source in &self.sources {
            while self.graph.take_payload(&source.port).is_some() {}
        }
    }
}

fn feed(stats: &mut DomainGraphStats, target: &Target, payload: Payload) {
    if let FeedOutcome::Rejected(_) = target.host.feed_payload(target.port.clone(), payload) {
        stats.forward_rejected += 1;
    }
}

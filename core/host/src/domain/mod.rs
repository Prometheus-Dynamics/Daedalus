//! Several graphs ticked on one thread, with some outputs linked to other graphs' inputs. A graph
//! with something to do ticks once, upstream first, and what it produced goes to every reader as a
//! shared handle, never a copy.

#![cfg(feature = "std")]

mod explain;
mod link;
mod stats;

use std::string::String;
use std::sync::Arc;
use std::time::Instant;
use std::vec::Vec;

use daedalus_planner::Planned;
use daedalus_runner::Forward;
use daedalus_value::Value;

use crate::{Dir, FeedOutcome, LoadedHost, ReloadError, ReloadReport};

pub use explain::{DomainExplanation, DomainGraph};
pub use link::{DomainError, Link};
pub use stats::{DomainGraphStats, DomainStats, DomainTick};

struct Graph {
    name: String,
    host: LoadedHost,
    stats: DomainGraphStats,
}

/// A set of graphs and the links between them.
#[derive(Default)]
pub struct Domain {
    graphs: Vec<Graph>,
    links: Vec<Link>,
    /// Graph indexes, upstream first, ties in the order added.
    order: Vec<usize>,
    stats: DomainStats,
}

impl Domain {
    pub fn new() -> Domain {
        Domain::default()
    }

    fn index_of(&self, name: &str) -> Result<usize, DomainError> {
        self.graphs
            .iter()
            .position(|graph| graph.name == name)
            .ok_or_else(|| DomainError::NoGraph {
                graph: String::from(name),
            })
    }

    /// Adds a graph, downstream of nothing until it is linked.
    pub fn add(&mut self, name: &str, host: LoadedHost) -> Result<(), DomainError> {
        if self.index_of(name).is_ok() {
            return Err(DomainError::DuplicateGraph {
                graph: String::from(name),
            });
        }
        self.graphs.push(Graph {
            name: String::from(name),
            stats: DomainGraphStats::named(name, host.node_keys().count()),
            host,
        });
        self.reorder();
        Ok(())
    }

    /// Removes a graph and every link touching it, and hands the graph back.
    pub fn remove(&mut self, name: &str) -> Result<LoadedHost, DomainError> {
        let index = self.index_of(name)?;
        self.links
            .retain(|link| link.from_graph != name && link.to_graph != name);
        let graph = self.graphs.remove(index);
        self.reorder();
        Ok(graph.host)
    }

    pub fn graph(&mut self, name: &str) -> Option<&mut LoadedHost> {
        let index = self.index_of(name).ok()?;
        Some(&mut self.graphs[index].host)
    }

    /// The graphs in the order they tick.
    pub fn tick_order(&self) -> impl Iterator<Item = &str> {
        self.order.iter().map(|&at| self.graphs[at].name.as_str())
    }

    /// Links an output of one graph to an input of another, after the structural checks.
    pub fn link(
        &mut self,
        from_graph: &str,
        from_port: &str,
        to_graph: &str,
        to_port: &str,
    ) -> Result<(), DomainError> {
        let link = Link {
            from_graph: String::from(from_graph),
            from_port: String::from(from_port),
            to_graph: String::from(to_graph),
            to_port: String::from(to_port),
        };
        self.check_link(&link)?;
        self.links.push(link);
        self.reorder();
        Ok(())
    }

    /// Hands a value to an input.
    pub fn push(
        &mut self,
        graph: &str,
        port: &str,
        value: Value,
    ) -> Result<FeedOutcome, DomainError> {
        let index = self.index_of(graph)?;
        self.graphs[index]
            .host
            .host()
            .feed(port, value)
            .map_err(DomainError::Host)
    }

    /// Hands a typed value to an input.
    pub fn push_typed<T: Send + Sync + 'static>(
        &mut self,
        graph: &str,
        port: &str,
        value: Arc<T>,
    ) -> Result<FeedOutcome, DomainError> {
        let index = self.index_of(graph)?;
        self.graphs[index]
            .host
            .host()
            .feed_typed(port, value)
            .map_err(DomainError::Host)
    }

    /// Takes a value from an output that no link reads.
    pub fn take(&mut self, graph: &str, port: &str) -> Result<Option<Value>, DomainError> {
        let index = self.index_of(graph)?;
        self.graphs[index]
            .host
            .host()
            .take_value(port)
            .map_err(DomainError::Host)
    }

    /// Ticks once every graph with something to do, upstream first. A graph that fails doesn't
    /// stop the others.
    pub fn tick(&mut self) -> DomainTick {
        let started = Instant::now();
        let mut tick = DomainTick::default();
        let mut step_time = 0;
        for position in 0..self.order.len() {
            let at = self.order[position];
            if !self.has_work(at) {
                tick.idle += 1;
                continue;
            }
            let began = Instant::now();
            let result = self.graphs[at].host.step();
            let spent = began.elapsed().as_nanos() as u64;
            step_time += spent;
            let consumers = self.consumers_of(at);
            self.graphs[at]
                .stats
                .record(spent, consumers, result.is_ok());
            match result {
                Ok(_) => tick.ran += 1,
                Err(_) => tick.failed += 1,
            }
            self.forward(at, &mut tick);
        }
        let total = started.elapsed().as_nanos() as u64;
        tick.duration_ns = total;
        self.stats.record(total, step_time);
        tick
    }

    /// True when an input has a queued value, or the graph has no inputs to wait for.
    fn has_work(&mut self, at: usize) -> bool {
        let mut inputs = 0;
        let mut pending = false;
        for port in self.graphs[at].host.host().port_infos() {
            if port.dir == Dir::Input {
                inputs += 1;
                pending |= !port.held && port.queued > 0;
            }
        }
        inputs == 0 || pending
    }

    fn consumers_of(&self, at: usize) -> usize {
        let name = &self.graphs[at].name;
        let mut readers: Vec<&str> = self
            .links
            .iter()
            .filter(|link| link.from_graph == *name)
            .map(|link| link.to_graph.as_str())
            .collect();
        readers.sort_unstable();
        readers.dedup();
        readers.len()
    }

    /// Moves everything graph `at` produced on its linked ports to the readers.
    fn forward(&mut self, at: usize, tick: &mut DomainTick) {
        let name = self.graphs[at].name.clone();
        let mut sources: Vec<String> = self
            .links
            .iter()
            .filter(|link| link.from_graph == name)
            .map(|link| link.from_port.clone())
            .collect();
        sources.sort_unstable();
        sources.dedup();
        for port in sources {
            while let Ok(Some(value)) = self.graphs[at].host.host().take_forward(&port) {
                self.deliver(&name, &port, value, tick);
            }
        }
    }

    fn deliver(&mut self, graph: &str, port: &str, value: Forward, tick: &mut DomainTick) {
        let targets: Vec<(usize, String)> = self
            .links
            .iter()
            .filter(|link| link.from_graph == graph && link.from_port == port)
            .filter_map(|link| {
                let reader = self.graphs.iter().position(|g| g.name == link.to_graph)?;
                Some((reader, link.to_port.clone()))
            })
            .collect();
        let mut value = Some(value);
        for (position, (reader, to_port)) in targets.iter().enumerate() {
            let last = position + 1 == targets.len();
            let sent = if last {
                value.take()
            } else {
                value.as_ref().map(Forward::share)
            };
            let Some(sent) = sent else { continue };
            let host = self.graphs[*reader].host.host();
            if host.feed_forward(to_port, sent).is_err() {
                tick.rejected += 1;
                self.stats.forward_rejected += 1;
            }
        }
    }

    /// Replaces one graph in place. Links and every other graph are kept. A plan that lacks a
    /// linked port is refused with nothing changed.
    pub fn reload(&mut self, name: &str, planned: Planned) -> Result<ReloadReport, DomainError> {
        let index = self.index_of(name)?;
        self.check_reload(name, index, &planned)?;
        let report = self.graphs[index].host.reload(planned);
        let report: Result<ReloadReport, ReloadError> = report;
        report.map_err(DomainError::Reload)
    }
}

impl core::fmt::Debug for Domain {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Domain")
            .field("links", &self.links)
            .finish()
    }
}

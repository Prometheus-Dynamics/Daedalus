//! The structural rules for links: both ends exist, an output feeds an input of the same type, an
//! input has one source, and the graphs stay acyclic.

#![cfg(feature = "std")]

use std::fmt;
use std::string::String;
use std::vec::Vec;

use daedalus_planner::Planned;

use super::Domain;
use crate::load::type_text;
use crate::{Dir, HostError, ReloadError};

/// One output feeding one input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Link {
    pub from_graph: String,
    pub from_port: String,
    pub to_graph: String,
    pub to_port: String,
}

/// Why a domain operation was refused. Nothing changed.
#[derive(Debug)]
#[non_exhaustive]
pub enum DomainError {
    NoGraph {
        graph: String,
    },
    DuplicateGraph {
        graph: String,
    },
    NoPort {
        graph: String,
        port: String,
    },
    /// The source isn't an output, or the target isn't an input.
    WrongDirection {
        graph: String,
        port: String,
    },
    TypeMismatch {
        link: String,
    },
    /// The input already has a source.
    AlreadyLinked {
        graph: String,
        port: String,
    },
    /// The link would make a graph its own upstream.
    Cycle {
        link: String,
    },
    /// A reload would remove or change a port a link uses.
    ReloadBreaksLink {
        graph: String,
        port: String,
    },
    Host(HostError),
    Reload(ReloadError),
}

impl fmt::Display for DomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DomainError::NoGraph { graph } => write!(f, "no graph `{graph}`"),
            DomainError::DuplicateGraph { graph } => write!(f, "graph `{graph}` already exists"),
            DomainError::NoPort { graph, port } => write!(f, "no port `{graph}.{port}`"),
            DomainError::WrongDirection { graph, port } => {
                write!(f, "port `{graph}.{port}` goes the wrong way for this link")
            }
            DomainError::TypeMismatch { link } => write!(f, "the ends of {link} differ in type"),
            DomainError::AlreadyLinked { graph, port } => {
                write!(f, "input `{graph}.{port}` already has a source")
            }
            DomainError::Cycle { link } => write!(f, "{link} would make a cycle"),
            DomainError::ReloadBreaksLink { graph, port } => {
                write!(f, "the plan for `{graph}` breaks the link on `{port}`")
            }
            DomainError::Host(error) => write!(f, "{error:?}"),
            DomainError::Reload(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for DomainError {}

impl Link {
    pub(super) fn describe(&self) -> String {
        std::format!(
            "link {}.{} -> {}.{}",
            self.from_graph,
            self.from_port,
            self.to_graph,
            self.to_port
        )
    }
}

impl Domain {
    /// The direction and type key of a port, or why there is none.
    fn port(&mut self, graph: &str, port: &str) -> Result<(Dir, String), DomainError> {
        let index = self.index_of(graph)?;
        let found = self.graphs[index]
            .host
            .host()
            .port_infos()
            .find(|info| info.name == port)
            .map(|info| (info.dir, String::from(info.type_key)));
        found.ok_or_else(|| DomainError::NoPort {
            graph: String::from(graph),
            port: String::from(port),
        })
    }

    pub(super) fn check_link(&mut self, link: &Link) -> Result<(), DomainError> {
        let (from_dir, from_ty) = self.port(&link.from_graph, &link.from_port)?;
        let (to_dir, to_ty) = self.port(&link.to_graph, &link.to_port)?;
        if from_dir != Dir::Output {
            return Err(DomainError::WrongDirection {
                graph: link.from_graph.clone(),
                port: link.from_port.clone(),
            });
        }
        if to_dir != Dir::Input {
            return Err(DomainError::WrongDirection {
                graph: link.to_graph.clone(),
                port: link.to_port.clone(),
            });
        }
        if from_ty != to_ty {
            return Err(DomainError::TypeMismatch {
                link: link.describe(),
            });
        }
        if self
            .links
            .iter()
            .any(|known| known.to_graph == link.to_graph && known.to_port == link.to_port)
        {
            return Err(DomainError::AlreadyLinked {
                graph: link.to_graph.clone(),
                port: link.to_port.clone(),
            });
        }
        if self.reaches(&link.to_graph, &link.from_graph) {
            return Err(DomainError::Cycle {
                link: link.describe(),
            });
        }
        Ok(())
    }

    /// Whether `to` is downstream of `from`, or the same graph.
    fn reaches(&self, from: &str, to: &str) -> bool {
        if from == to {
            return true;
        }
        self.links
            .iter()
            .filter(|link| link.from_graph == from)
            .any(|link| self.reaches(&link.to_graph, to))
    }

    /// Upstream first, and among graphs that are free to go, the one added first.
    pub(super) fn reorder(&mut self) {
        let mut placed = std::vec![false; self.graphs.len()];
        let mut order = Vec::with_capacity(self.graphs.len());
        while order.len() < self.graphs.len() {
            let next = (0..self.graphs.len()).find(|&at| {
                !placed[at]
                    && self.links.iter().all(|link| {
                        link.to_graph != self.graphs[at].name
                            || self
                                .graphs
                                .iter()
                                .position(|graph| graph.name == link.from_graph)
                                .is_some_and(|from| placed[from])
                    })
            });
            let Some(next) = next else { break };
            placed[next] = true;
            order.push(next);
        }
        self.order = order;
    }

    /// Refuses a plan whose ports no longer fit the links on this graph.
    pub(super) fn check_reload(
        &mut self,
        name: &str,
        index: usize,
        planned: &Planned,
    ) -> Result<(), DomainError> {
        let links: Vec<(String, Dir)> = self
            .links
            .iter()
            .filter_map(|link| {
                if link.from_graph == name {
                    Some((link.from_port.clone(), Dir::Output))
                } else if link.to_graph == name {
                    Some((link.to_port.clone(), Dir::Input))
                } else {
                    None
                }
            })
            .collect();
        let stitched = &planned.stitched;
        for (port, dir) in links {
            let list = match dir {
                Dir::Input => &stitched.host_inputs,
                Dir::Output => &stitched.host_outputs,
            };
            let offered = list.iter().find(|candidate| candidate.port == port);
            let same_type = offered.is_some_and(|candidate| {
                let ty = stitched.slots[candidate.slot as usize].ty;
                let key = ty
                    .and_then(|ty| planned.types.exprs().get(ty as usize))
                    .map(type_text);
                let running = self.graphs[index]
                    .host
                    .host()
                    .port_infos()
                    .find(|info| info.name == port)
                    .map(|info| String::from(info.type_key));
                key == running
            });
            if !same_type {
                return Err(DomainError::ReloadBreaksLink {
                    graph: String::from(name),
                    port,
                });
            }
        }
        Ok(())
    }
}

//! Works out how fast each node runs relative to its inputs, and checks that joins agree.

use std::fmt;

use daedalus_graph::{NodeKindDoc, RateDoc};

use super::{Node, Work};
use crate::pipeline::SolveError;
use crate::solve::ports::{Direction, PortError};

/// A positive rational in lowest terms.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Rate {
    pub num: u64,
    pub den: u64,
}

fn gcd(mut a: u128, mut b: u128) -> u128 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

impl Rate {
    pub const ONE: Rate = Rate { num: 1, den: 1 };

    /// `None` when either part is zero.
    pub fn new(num: u64, den: u64) -> Option<Rate> {
        if num == 0 || den == 0 {
            return None;
        }
        let divisor = gcd(num.into(), den.into()) as u64;
        Some(Rate {
            num: num / divisor,
            den: den / divisor,
        })
    }

    /// `None` when the result doesn't fit.
    pub fn times(self, other: Rate) -> Option<Rate> {
        let num = u128::from(self.num) * u128::from(other.num);
        let den = u128::from(self.den) * u128::from(other.den);
        let divisor = gcd(num, den);
        Some(Rate {
            num: u64::try_from(num / divisor).ok()?,
            den: u64::try_from(den / divisor).ok()?,
        })
    }
}

impl fmt::Display for Rate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.num, self.den)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RateError {
    RateMismatch {
        node: String,
        first: Rate,
        second: Rate,
    },
    SyncGroupRate {
        node: String,
        group: String,
        first: Rate,
        second: Rate,
    },
    /// A declared rate has a zero in it.
    RateInvalid { node: String },
    /// A chain of ratios no longer fits.
    RateOverflow { node: String },
}

impl fmt::Display for RateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RateError::RateMismatch {
                node,
                first,
                second,
            } => {
                write!(
                    f,
                    "node `{node}` has inputs at different rates: {first} and {second}"
                )
            }
            RateError::SyncGroupRate {
                node,
                group,
                first,
                second,
            } => {
                write!(
                    f,
                    "node `{node}` sync group `{group}` has rates {first} and {second}"
                )
            }
            RateError::RateInvalid { node } => write!(f, "node `{node}` declares a zero rate"),
            RateError::RateOverflow { node } => write!(f, "node `{node}`: the rate is too large"),
        }
    }
}

impl std::error::Error for RateError {}

fn declared(doc: &RateDoc) -> Option<Rate> {
    Rate::new(doc.num.into(), doc.den.into())
}

/// Checks each sync group. `true` if one of them already reported this node.
fn check_groups(node: &Node, incoming: &[(&str, Rate)], errors: &mut Vec<SolveError>) -> bool {
    let mut reported = false;
    for group in &node.doc.sync_groups {
        let mut first = None;
        for port in &group.ports {
            let Some(&(_, rate)) = incoming.iter().find(|(name, _)| name == port) else {
                if !node.inputs.iter().any(|known| known.name == *port) {
                    errors.push(SolveError::Ports(PortError::PortMissing {
                        node: node.doc.id.clone(),
                        direction: Direction::Input,
                        port: port.clone(),
                    }));
                }
                continue;
            };
            match first {
                None => first = Some(rate),
                Some(first) if first != rate => {
                    errors.push(SolveError::Rates(RateError::SyncGroupRate {
                        node: node.doc.id.clone(),
                        group: group.name.clone(),
                        first,
                        second: rate,
                    }));
                    reported = true;
                    break;
                }
                Some(_) => {}
            }
        }
    }
    reported
}

fn rate_error(error: RateError) -> SolveError {
    SolveError::Rates(error)
}

pub fn run(work: &mut Work<'_>) {
    if work.graph.order.len() != work.graph.nodes.len() {
        return; // A cycle was already reported.
    }
    let order = std::mem::take(&mut work.graph.order);
    let mut errors = Vec::new();
    for &index in &order {
        let incoming: Vec<(&str, Rate)> = work
            .graph
            .edges
            .iter()
            .filter(|edge| edge.to.node == index)
            .map(|edge| {
                let source = &work.graph.nodes[edge.from.node as usize];
                (edge.to.port.as_str(), source.rate_out)
            })
            .collect();
        let node = &work.graph.nodes[index as usize];
        let mut rate_in = incoming.first().map_or(Rate::ONE, |&(_, rate)| rate);
        if !check_groups(node, &incoming, &mut errors)
            && let Some(&(_, other)) = incoming.iter().find(|&&(_, rate)| rate != rate_in)
        {
            errors.push(rate_error(RateError::RateMismatch {
                node: node.doc.id.clone(),
                first: rate_in,
                second: other,
            }));
        }
        let id = node.doc.id.clone();
        let mut rate_out = rate_in;
        if let Some(doc) = &node.doc.rate {
            match declared(doc) {
                None => errors.push(rate_error(RateError::RateInvalid { node: id })),
                Some(rate) if node.doc.kind == NodeKindDoc::HostBridge => {
                    rate_in = rate;
                    rate_out = rate;
                }
                Some(ratio) => match rate_in.times(ratio) {
                    Some(rate) => rate_out = rate,
                    None => errors.push(rate_error(RateError::RateOverflow { node: id })),
                },
            }
        }
        let node = &mut work.graph.nodes[index as usize];
        node.rate_in = rate_in;
        node.rate_out = rate_out;
    }
    work.graph.edge_rates = work
        .graph
        .edges
        .iter()
        .map(|edge| work.graph.nodes[edge.from.node as usize].rate_out)
        .collect();
    work.graph.order = order;
    work.errors.extend(errors);
}

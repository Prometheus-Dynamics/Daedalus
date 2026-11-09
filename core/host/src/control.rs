//! Switching a node on or off by hand, between ticks. Holding `&mut LoadedHost` is what keeps a
//! tick from running meanwhile. Automatic turning off stays a policy on the runner.

#![cfg(feature = "std")]

use std::fmt;
use std::string::String;
use std::vec::Vec;

use crate::load::LoadedHost;

/// Why a node couldn't be switched. Nothing changed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlError {
    NoSuchNode {
        node: String,
    },
    /// The node runs inside a merged step, so it can't be switched alone. Run unmerged first.
    Merged {
        node: String,
        covers: Vec<String>,
    },
}

impl fmt::Display for ControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ControlError::NoSuchNode { node } => write!(f, "no node `{node}`"),
            ControlError::Merged { node, covers } => {
                write!(f, "`{node}` runs merged with {covers:?}")
            }
        }
    }
}

impl std::error::Error for ControlError {}

impl LoadedHost {
    fn step_of(&self, node: &str) -> Result<u32, ControlError> {
        let found = self.covers.iter().position(|covers| covers.contains(&node));
        let Some(step) = found else {
            return Err(ControlError::NoSuchNode {
                node: String::from(node),
            });
        };
        if self.covers[step].len() > 1 {
            return Err(ControlError::Merged {
                node: String::from(node),
                covers: self.covers[step].iter().map(|n| String::from(*n)).collect(),
            });
        }
        Ok(step as u32)
    }

    /// Turns a node on or off. It stays that way across reloads while it exists.
    pub fn set_node_on(&mut self, node: &str, on: bool) -> Result<(), ControlError> {
        let step = self.step_of(node)?;
        let result = if on {
            self.host.runner.switch_on(step)
        } else {
            self.host.runner.switch_off(step)
        };
        debug_assert!(result.is_ok(), "the step came from this plan");
        if on {
            self.off_nodes.remove(node);
        } else {
            self.off_nodes.insert(String::from(node));
        }
        Ok(())
    }

    /// Whether a node is on. `None` when the graph has no such node.
    pub fn node_is_on(&self, node: &str) -> Option<bool> {
        self.covers
            .iter()
            .any(|covers| covers.contains(&node))
            .then(|| !self.off_nodes.contains(node))
    }

    /// Switches the nodes that were off back off, after a reload built a new runner.
    pub(crate) fn reapply_off_nodes(&mut self) {
        let nodes: Vec<String> = self.off_nodes.iter().cloned().collect();
        for node in nodes {
            if self.set_node_on(&node, false).is_err() {
                self.off_nodes.remove(&node);
            }
        }
    }
}

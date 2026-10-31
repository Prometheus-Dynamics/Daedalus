//! CPU→GPU→CPU chains in a plan explanation.

use crate::prelude::*;
use alloc::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};

use super::{RuntimeEdgeExplanation, RuntimePlanExplanation};

/// A payload uploaded to the device on `upload_edge`, carried through `nodes` on device-resident
/// edges (no transfer), and downloaded again on `download_edge`. A GPU pipeline usually wants
/// none: the work between the two transfers either belongs on the CPU, or the download belongs
/// at the end of a longer device chain. One edge that uploads and downloads within its own
/// adapter path is reported with the same edge twice and no nodes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceRoundTrip {
    pub upload_edge: usize,
    pub download_edge: usize,
    /// Node indices between the transfers, in path order.
    pub nodes: Vec<usize>,
    pub node_ids: Vec<String>,
}

fn transfers(edge: &RuntimeEdgeExplanation) -> (u64, u64) {
    edge.transport
        .as_ref()
        .map_or((0, 0), |transport| transport.device_transfers())
}

impl RuntimePlanExplanation {
    /// Every CPU→GPU→CPU chain across nodes ([`DeviceRoundTrip`]), so a test can assert that a
    /// GPU pipeline has none. Only transfers on edges are seen: an upload or readback inside a
    /// node's handler is invisible to the plan.
    pub fn device_round_trips(&self) -> Vec<DeviceRoundTrip> {
        let id_of = |node: usize| {
            self.nodes
                .iter()
                .find(|n| n.index == node)
                .map_or_else(String::new, |n| n.id.clone())
        };
        let mut outgoing: BTreeMap<usize, Vec<&RuntimeEdgeExplanation>> = BTreeMap::new();
        for edge in &self.edges {
            outgoing.entry(edge.from_node).or_default().push(edge);
        }
        let mut trips = Vec::new();
        for upload in &self.edges {
            let (up, down) = transfers(upload);
            if up == 0 {
                continue;
            }
            if down > 0 {
                trips.push(DeviceRoundTrip {
                    upload_edge: upload.index,
                    download_edge: upload.index,
                    nodes: Vec::new(),
                    node_ids: Vec::new(),
                });
                continue;
            }
            // Breadth-first over device-resident edges from the upload's consumer; `parent`
            // records the path back for each reached node.
            let mut parent: BTreeMap<usize, Option<usize>> =
                BTreeMap::from([(upload.to_node, None)]);
            let mut queue = VecDeque::from([upload.to_node]);
            while let Some(node) = queue.pop_front() {
                for edge in outgoing.get(&node).into_iter().flatten() {
                    let (up, down) = transfers(edge);
                    if down > 0 {
                        let mut nodes = vec![node];
                        let mut at = node;
                        while let Some(Some(prev)) = parent.get(&at) {
                            nodes.push(*prev);
                            at = *prev;
                        }
                        nodes.reverse();
                        trips.push(DeviceRoundTrip {
                            upload_edge: upload.index,
                            download_edge: edge.index,
                            node_ids: nodes.iter().map(|&n| id_of(n)).collect(),
                            nodes,
                        });
                    } else if up == 0 && !parent.contains_key(&edge.to_node) {
                        parent.insert(edge.to_node, Some(node));
                        queue.push_back(edge.to_node);
                    }
                }
            }
        }
        trips
    }
}

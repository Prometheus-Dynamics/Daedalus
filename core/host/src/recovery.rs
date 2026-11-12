//! What a step does when its body faults, read from the graph's node metadata when a plan loads.
//! The keys are documented in `docs/host.md`. Parsing happens at load, never on the tick path.

use std::collections::BTreeMap;
use std::string::String;

use daedalus_graph::Lit;
use daedalus_planner::Planned;
use daedalus_runner::Recovery;

use crate::load::LoadError;

/// What a fault does: `"fail"` (the default), `"skip"` or `"restart"`. Use
/// [`RETRIES_KEY`] for a retry.
pub const RECOVERY_KEY: &str = "daedalus.node.recovery";

/// How many times the body runs again in the same tick after a fault, an integer from 1 up. The
/// fault stands after the last retry, and the failure rule applies as for any fault.
pub const RETRIES_KEY: &str = "daedalus.node.retries";

fn refuse(node: &str, reason: &str) -> LoadError {
    LoadError::Recovery {
        node: String::from(node),
        reason: String::from(reason),
    }
}

/// The recovery a node's metadata asks for, or `None` when it says nothing.
pub(crate) fn node_recovery(
    node: &str,
    metadata: &BTreeMap<String, Lit>,
) -> Result<Option<Recovery>, LoadError> {
    match (metadata.get(RECOVERY_KEY), metadata.get(RETRIES_KEY)) {
        (None, None) => Ok(None),
        (Some(_), Some(_)) => Err(refuse(
            node,
            "sets both daedalus.node.recovery and daedalus.node.retries; set one of them",
        )),
        (None, Some(Lit::Int(count))) => match u32::try_from(*count) {
            Ok(max) if max >= 1 => Ok(Some(Recovery::Retry { max })),
            _ => Err(refuse(
                node,
                "daedalus.node.retries must be an integer from 1 to 4294967295",
            )),
        },
        (None, Some(_)) => Err(refuse(
            node,
            "daedalus.node.retries must be an integer from 1 to 4294967295",
        )),
        (Some(Lit::Str(text)), None) => match text.as_str() {
            "fail" => Ok(Some(Recovery::Fail)),
            "skip" => Ok(Some(Recovery::SkipTick)),
            "restart" => Ok(Some(Recovery::Restart)),
            _ => Err(refuse(
                node,
                "daedalus.node.recovery must be \"fail\", \"skip\" or \"restart\"",
            )),
        },
        (Some(_), None) => Err(refuse(
            node,
            "daedalus.node.recovery must be \"fail\", \"skip\" or \"restart\"",
        )),
    }
}

/// The recovery of one step, from the nodes it covers. Nodes that say nothing don't count. Nodes
/// merged into one step that ask for different things are refused: the step can have only one
/// recovery, so the graph has to keep them apart with `daedalus.node.fusion` set to false.
pub(crate) fn step_recovery(planned: &Planned, covers: &[u32]) -> Result<Recovery, LoadError> {
    let mut chosen: Option<(Recovery, &str)> = None;
    for &node in covers {
        let doc = &planned.graph.nodes[node as usize].doc;
        let name = doc.label.as_deref().unwrap_or(doc.id.as_str());
        let Some(found) = node_recovery(name, &doc.metadata)? else {
            continue;
        };
        match chosen {
            None => chosen = Some((found, name)),
            Some((first, _)) if first == found => {}
            Some((_, first)) => {
                return Err(refuse(
                    name,
                    &std::format!(
                        "is merged with `{first}`, which asks for a different recovery; set \
                         daedalus.node.fusion to false on one of them"
                    ),
                ));
            }
        }
    }
    Ok(chosen.map_or(Recovery::Fail, |(recovery, _)| recovery))
}

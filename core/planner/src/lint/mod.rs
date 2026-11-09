//! Things that are legal but probably wrong. Off unless the config turns them on.

mod rules;

use daedalus_registry::Registry;

use crate::diagnostic::Diagnostic;
use crate::solve::Graph;

pub const UNCONNECTED_REQUIRED_INPUT: &str = "unconnected_required_input";
pub const UNUSED_OUTPUT: &str = "unused_output";
pub const EXCLUSIVE_ON_FANOUT: &str = "exclusive_on_fanout";
pub const FIRE_ALL_MAY_NOT_PRODUCE: &str = "fire_all_may_not_produce";

/// Runs every rule over a solved graph.
pub fn lint(graph: &Graph, registry: &Registry) -> Vec<Diagnostic> {
    let mut out = Vec::new();
    rules::unconnected_required_inputs(graph, &mut out);
    rules::unused_outputs(graph, &mut out);
    rules::exclusive_on_fanout(graph, &mut out);
    rules::fire_all_may_not_produce(graph, registry, &mut out);
    out
}

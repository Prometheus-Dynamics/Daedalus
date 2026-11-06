//! Nodes registered at runtime, as a data-driven catalog builds them: a `NodeDecl`, a handler
//! through `handlers.try_on`, and each port's stable codec through `declare_stable_port` (what
//! the node macros record for a `#[node]`). The facade's `dylib_stable_generated` test runs them
//! statically and through the stable path.

use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::registry::capability::{NodeDecl, PortDecl};
use daedalus::runtime::NodeError;
use daedalus::runtime::plugins::{PluginInstallContext, PluginResult, StablePortCodec};
use daedalus::transport::{Local, Payload};

use super::locals::{TRACKER_KEY, Tracker};
use super::pods::{BOXES_KEY, BoxPod};

/// The key of one `BoxPod`.
pub const BOX_KEY: &str = "stable_abi:box";
const INT_KEY: &str = "typeexpr:{\"Scalar\":\"Int\"}";

fn int(name: &str) -> PortDecl {
    PortDecl::new(name, INT_KEY).schema(TypeExpr::Scalar(ValueType::Int))
}

fn invalid(what: &str) -> NodeError {
    NodeError::InvalidInput(format!("expected {what}"))
}

/// `stable_abi:gen_boxes`: the first box of a `Vec<BoxPod>` and the count; and
/// `stable_abi:gen_tracker`: a `Local<Tracker>`'s id, and the same object passed on.
pub fn install_generated(registry: &mut PluginInstallContext<'_>) -> PluginResult<()> {
    let failed = |err: daedalus::runtime::handler_registry::HandlerRegistryError| {
        daedalus::runtime::plugins::PluginError::Install {
            message: err.to_string(),
        }
    };
    registry.register_node_decl(
        NodeDecl::new("stable_abi:gen_boxes")
            .input(PortDecl::new("boxes", BOXES_KEY))
            .output(PortDecl::new("first", BOX_KEY))
            .output(int("count")),
    )?;
    registry
        .handlers
        .try_on("stable_abi:gen_boxes", |_node, _ctx, io| {
            let boxes = io
                .get_typed_ref::<Vec<BoxPod>>("boxes")
                .ok_or_else(|| invalid("a Vec<BoxPod>"))?;
            let (first, count) = (boxes.first().copied().unwrap_or_default(), boxes.len());
            io.push_payload("first", Payload::owned(BOX_KEY, first));
            io.push_payload("count", Payload::owned(INT_KEY, count as i64));
            Ok(())
        })
        .map_err(failed)?;
    let pods = StablePortCodec::pod::<Vec<BoxPod>>();
    registry.declare_stable_port("stable_abi:gen_boxes", "boxes", &pods)?;
    let pod = StablePortCodec::pod::<BoxPod>();
    registry.declare_stable_port("stable_abi:gen_boxes", "first", &pod)?;
    let count = StablePortCodec::value_of::<i64>();
    registry.declare_stable_port("stable_abi:gen_boxes", "count", &count)?;

    registry.register_node_decl(
        NodeDecl::new("stable_abi:gen_tracker")
            .input(PortDecl::new("obj", TRACKER_KEY))
            .output(PortDecl::new("obj", TRACKER_KEY))
            .output(int("id")),
    )?;
    registry
        .handlers
        .try_on("stable_abi:gen_tracker", |_node, _ctx, io| {
            let obj = io
                .get_typed_ref::<Local<Tracker>>("obj")
                .ok_or_else(|| invalid("a Local<Tracker>"))?
                .clone();
            io.push_payload("id", Payload::owned(INT_KEY, obj.id));
            io.push_payload("obj", Payload::owned(TRACKER_KEY, obj));
            Ok(())
        })
        .map_err(failed)?;
    let local = StablePortCodec::local::<Tracker>();
    // One declaration covers the input and the output: both are the port `obj`.
    registry.declare_stable_port("stable_abi:gen_tracker", "obj", &local)?;
    Ok(())
}

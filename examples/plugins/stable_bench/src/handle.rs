//! The plugin-local handle prototype: `Detections` crosses as an opaque foreign handle, never
//! converted to a `Value`.
//!
//! The handle is `daedalus:detections_view`, a foreign interface whose vtable hands back the
//! owner's own pointer. The producer shares its `Detections` in an `Arc`
//! (`Payload::shared_with`), retypes it to the interface with `provide_foreign`, and the stable
//! boundary wraps it as a `ForeignHandle` whose retain and release are this build's own
//! functions (`ForeignOwner::from_arc`). The host passes the handle on without reading its
//! data. The consumer reads the object through the vtable's `object` accessor, which returns the
//! data pointer (this build's own `Detections`), and casts it: that is sound only because the
//! handle came from this build, which the consumer checks by the interface, not by the owner.
//! A handle from some other build of this plugin would need its own vtable check.

use std::ffi::c_void;
use std::sync::Arc;

use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::macros::node;
use daedalus::runtime::{ExecutionContext, NodeError, NodeIo, RuntimeNode};
use daedalus::transport::{ForeignRef, Payload, ProvideForeign, Residency, foreign_interface};

use super::detections::Detections;

foreign_interface! {
    /// `Detections` read in place through its owning build (the stable-bench handle prototype).
    pub interface DetectionsInterface("stable_bench:detections_view", version = 1);
    /// Accessors of `stable_bench:detections_view` v1.
    pub struct DetectionsVTable {
        /// The owner's own `Detections` pointer, given back from the data pointer.
        pub object: unsafe extern "C" fn(data: *const c_void) -> *const c_void,
    }
}

/// The port type of a `Detections` handle (the interface key, as `provide_foreign` retypes it).
pub const DETECTIONS_HANDLE_KEY: &str = "stable_bench:detections_view";

unsafe extern "C" fn object(data: *const c_void) -> *const c_void {
    data
}

// Safety: `object` returns the data pointer unchanged and does not unwind; the data pointer is
// the `Detections` the handle was built from.
unsafe impl ProvideForeign<DetectionsInterface> for Detections {
    fn vtable() -> &'static DetectionsVTable {
        static VTABLE: DetectionsVTable = DetectionsVTable { object };
        &VTABLE
    }
}

/// A payload holding `det` as a `Detections` handle: shared, retyped to the interface.
pub fn handle_payload(det: Arc<Detections>) -> Result<Payload, NodeError> {
    Payload::shared_with(DETECTIONS_HANDLE_KEY, det, Residency::Cpu, None, None)
        .provide_foreign::<Detections, DetectionsInterface>()
        .map_err(|_| NodeError::InvalidInput("a Detections payload cannot be retyped".into()))
}

/// Reads the handle's `Detections` in place.
pub fn view_detections<'a>(view: &ForeignRef<'a, DetectionsInterface>) -> &'a Detections {
    let object = unsafe { (view.vtable().object)(view.data()) };
    // Safety: the handle was built by `handle_payload` in this build, so its data pointer is
    // the live `Detections` the owner keeps alive for the handle's lifetime `'a`.
    unsafe { &*object.cast::<Detections>() }
}

/// Makes `tags` tags and hands them on as a plugin-local handle.
#[node(
    id = "handle_source",
    inputs(port(name = "tags", ty = TypeExpr::Scalar(ValueType::Int))),
    outputs(port(name = "det", type_key = "stable_bench:detections_view"))
)]
pub fn handle_source(
    _node: &RuntimeNode,
    _ctx: &ExecutionContext,
    io: &mut NodeIo,
) -> Result<(), NodeError> {
    let tags = io
        .get_typed::<i64>("tags")
        .ok_or_else(|| NodeError::InvalidInput("missing `tags`".into()))?;
    let det = Arc::new(Detections::sample(tags.max(0) as usize));
    io.push_payload("det", handle_payload(det)?);
    Ok(())
}

/// Reads a `Detections` handle in place and returns the same checksum `value_sink` does.
#[node(
    id = "handle_sink",
    inputs(port(name = "det", type_key = "stable_bench:detections_view")),
    outputs(port(name = "out", ty = TypeExpr::Scalar(ValueType::Int)))
)]
pub fn handle_sink(
    _node: &RuntimeNode,
    _ctx: &ExecutionContext,
    io: &mut NodeIo,
) -> Result<(), NodeError> {
    let checksum = {
        let view: ForeignRef<'_, DetectionsInterface> = io.get_foreign("det")?;
        view_detections(&view).checksum()
    };
    io.push_to("out", checksum)?;
    Ok(())
}

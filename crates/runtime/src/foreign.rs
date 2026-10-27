//! Node-side access to foreign-interface ports: inputs (`FrameView<'_>`, `ForeignRef<'_, I>`,
//! `SharedFrame`) and frame outputs (`SharedFrame`).

use daedalus_data::model::TypeExpr;
use daedalus_transport::{ForeignInterface, ForeignView, SharedFrame};

use crate::NodeError;
use crate::handles::PortId;
use crate::io::NodeIo;

impl NodeIo {
    /// Read input `port` as the foreign view `V`: borrowed (`FrameView<'_>`) or owned
    /// (`SharedFrame`, a retain of the frame).
    ///
    /// The payload must be foreign-readable ([`Payload::foreign_borrow`]): an owner value the
    /// planner's provider adapter retyped (borrowed in place, no allocation) or a carried foreign
    /// handle, for the same interface key, version and vtable layout as this build's
    /// `V::Interface`.
    ///
    /// [`Payload::foreign_borrow`]: daedalus_transport::Payload::foreign_borrow
    pub fn get_foreign<'a, V: ForeignView<'a>>(&'a self, port: &str) -> Result<V, NodeError> {
        let key = <V::Interface as ForeignInterface>::KEY;
        let payload = self
            .get_payload(port)
            .ok_or_else(|| NodeError::InvalidInput(format!("missing {port}")))?;
        V::from_payload(payload)
            .ok_or_else(|| {
                NodeError::InvalidInput(format!(
                    "input `{port}` expects foreign interface `{key}`, found a `{}` payload that \
                     is not foreign-readable (is a provider for `{key}` registered for that \
                     type?)",
                    payload.type_key()
                ))
            })?
            .map_err(|mismatch| NodeError::InvalidInput(format!("input `{port}`: {mismatch}")))
    }

    /// Push `frame` to output `port` as a `daedalus:frame` payload (what a `SharedFrame` node
    /// output does): no copy, one allocation for the payload storage.
    pub fn push_frame(&mut self, port: impl Into<PortId>, frame: SharedFrame) {
        let payload = frame.into_payload().stamp(self.clock());
        self.push_payload(port, payload);
    }
}

/// Macro support: the port schema of a foreign view input, `Opaque(<interface key>)`.
#[doc(hidden)]
pub fn view_type_expr<'a, V: ForeignView<'a>>() -> TypeExpr {
    TypeExpr::opaque(<V::Interface as ForeignInterface>::KEY)
}

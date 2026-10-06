//! Node-side access to foreign-interface inputs (`FrameView<'_>`, `ForeignRef<'_, I>`).

use daedalus_data::model::TypeExpr;
use daedalus_transport::{ForeignInterface, ForeignView};

use crate::NodeError;
use crate::io::NodeIo;

impl NodeIo {
    /// Borrow input `port` as the foreign view `V`.
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
        let borrow = payload.foreign_borrow().ok_or_else(|| {
            NodeError::InvalidInput(format!(
                "input `{port}` expects foreign interface `{key}`, found a `{}` payload that is \
                 not foreign-readable (is a provider for `{key}` registered for that type?)",
                payload.type_key()
            ))
        })?;
        V::from_borrow(borrow)
            .map_err(|mismatch| NodeError::InvalidInput(format!("input `{port}`: {mismatch}")))
    }
}

/// Macro support: the port schema of a foreign view input, `Opaque(<interface key>)`.
#[doc(hidden)]
pub fn view_type_expr<'a, V: ForeignView<'a>>() -> TypeExpr {
    TypeExpr::opaque(<V::Interface as ForeignInterface>::KEY)
}

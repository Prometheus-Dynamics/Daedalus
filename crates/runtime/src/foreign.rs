//! Node-side access to foreign-interface inputs (`FrameView<'_>`, `ForeignRef<'_, I>`).

use daedalus_data::model::TypeExpr;
use daedalus_transport::{ForeignInterface, ForeignView};

use crate::NodeError;
use crate::io::NodeIo;

impl NodeIo {
    /// Borrow input `port` as the foreign view `V`.
    ///
    /// The payload must carry a foreign handle (the planner inserts the owner's provider adapter)
    /// for the same interface key, version and vtable layout as this build's `V::Interface`.
    pub fn get_foreign<'a, V: ForeignView<'a>>(&'a self, port: &str) -> Result<V, NodeError> {
        let key = <V::Interface as ForeignInterface>::KEY;
        let payload = self
            .get_payload(port)
            .ok_or_else(|| NodeError::InvalidInput(format!("missing {port}")))?;
        let handle = payload.foreign_handle().ok_or_else(|| {
            NodeError::InvalidInput(format!(
                "input `{port}` expects foreign interface `{key}`, found a `{}` payload without a \
                 foreign handle (is a provider for `{key}` registered for that type?)",
                payload.type_key()
            ))
        })?;
        V::from_handle(handle)
            .map_err(|mismatch| NodeError::InvalidInput(format!("input `{port}`: {mismatch}")))
    }
}

/// Macro support: the port schema of a foreign view input, `Opaque(<interface key>)`.
#[doc(hidden)]
pub fn view_type_expr<'a, V: ForeignView<'a>>() -> TypeExpr {
    TypeExpr::opaque(<V::Interface as ForeignInterface>::KEY)
}

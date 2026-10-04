use alloc::string::ToString;
use core::fmt;

crate::define_text_id!(
    TypeKey,
    "Stable graph/plugin-facing type identity.\n\n`TypeKey` is the transport identity that \
     survives manifests, plugins, and FFI boundaries. Native Rust `TypeId` can still be used as \
     a fast path, but it is not stable enough to be the graph type identity."
);

crate::define_text_id!(
    LayoutHash,
    "Deterministic ABI/layout identity for types that may cross a Rust dynamic plugin boundary."
);

impl LayoutHash {
    /// Runtime-local fallback layout identity for plain Rust values.
    ///
    /// This is intentionally tied to the concrete Rust type name plus basic ABI facts. Dynamic
    /// plugin boundaries that already have schema/field metadata should prefer
    /// [`Self::for_schema`] so same-name, same-size layout changes are still rejected.
    pub fn for_type<T: 'static>() -> Self {
        Self::new(format!(
            "rust-type-v1:{}:{}:{}",
            core::any::type_name::<T>(),
            core::mem::size_of::<T>(),
            core::mem::align_of::<T>()
        ))
    }

    /// Stable schema-derived layout identity for boundary-crossing payload contracts.
    pub fn for_schema<T: 'static>(schema: impl fmt::Display) -> Self {
        Self::new(format!(
            "rust-schema-v1:{}:{}:{}:{:016x}",
            core::any::type_name::<T>(),
            core::mem::size_of::<T>(),
            core::mem::align_of::<T>(),
            daedalus_core::stable_id::fnv1a64(schema.to_string().as_bytes())
        ))
    }
}

crate::define_text_id!(
    Layout,
    "Layout identity or constraint for payloads with meaningful memory/device layout."
);
crate::define_text_id!(SourceId, "Source id recorded in payload lineage.");
crate::define_text_id!(AdapterId, "Stable adapter identifier.");

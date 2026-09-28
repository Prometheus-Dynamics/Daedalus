use std::fmt;

crate::define_text_id!(
    TypeKey,
    "Stable graph/plugin-facing type identity.\n\n`TypeKey` is the transport identity that \
     survives manifests, plugins, and FFI boundaries. Native Rust `TypeId` can still be used as \
     a fast path, but it is not stable enough to be the graph type identity."
);

impl TypeKey {
    pub fn opaque(key: impl Into<String>) -> Self {
        Self::new(key)
    }
}

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
            std::any::type_name::<T>(),
            std::mem::size_of::<T>(),
            std::mem::align_of::<T>()
        ))
    }

    /// Stable schema-derived layout identity for boundary-crossing payload contracts.
    pub fn for_schema<T: 'static>(schema: impl fmt::Display) -> Self {
        Self::new(format!(
            "rust-schema-v1:{}:{}:{}:{:016x}",
            std::any::type_name::<T>(),
            std::mem::size_of::<T>(),
            std::mem::align_of::<T>(),
            stable_hash64(schema.to_string().as_bytes())
        ))
    }
}

/// 64-bit FNV-1a, identical to `daedalus_core::stable_id::fnv1a64`; duplicated because this crate
/// deliberately depends on nothing but serde and thiserror.
fn stable_hash64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

crate::define_text_id!(
    Layout,
    "Layout identity or constraint for payloads with meaningful memory/device layout."
);
crate::define_text_id!(SourceId, "Source id recorded in payload lineage.");
crate::define_text_id!(AdapterId, "Stable adapter identifier.");

//! Per-port codecs for the stable dynamic plugin path (`daedalus::dylib`): how a node port's
//! Rust type converts from and to a [`Value`], recorded by the node macros while a registry
//! extracts a dynamic plugin (see [`PluginRegistry::record_stable_codecs`]).
//!
//! Codecs are keyed by node port rather than by type key: structural keys
//! (`typeexpr:List(Int)`) and ports with a `ty = ...` override name no single Rust type, while a
//! port always has exactly the type its handler fetches.

use super::*;
use crate::const_coerce::CoerceFn;
use alloc::sync::Arc;
use core::any::TypeId;
use daedalus_data::model::Value;

/// Converts a port value (the payload's `dyn Any`) to a [`Value`].
pub type StableEncodeFn = Arc<dyn Fn(&(dyn Any + Send + Sync)) -> Option<Value> + Send + Sync>;
/// Builds a payload of the port's Rust type, under the given key, from a [`Value`].
pub type StableDecodeFn = Arc<dyn Fn(TypeKey, &Value) -> Option<Payload> + Send + Sync>;

/// The Rust type behind a node port and its [`Value`] conversions.
#[derive(Clone)]
pub struct StableCodec {
    pub type_id: TypeId,
    pub rust_type: &'static str,
    /// `T: ToValue` (derived with `DaedalusToValue`, or a builtin).
    pub encode: Option<StableEncodeFn>,
    /// Builtin conversions, then `DaedalusTypeExpr::from_value`, then serde.
    pub decode: StableDecodeFn,
}

impl core::fmt::Debug for StableCodec {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StableCodec")
            .field("rust_type", &self.rust_type)
            .field("encode", &self.encode.is_some())
            .finish_non_exhaustive()
    }
}

/// Codecs by `(node id, port name)`.
pub(super) type StableCodecMap = HashMap<(String, String), StableCodec>;

impl PluginRegistry {
    /// Record node port codecs ([`Self::stable_codec`]) from now on. Dynamic plugins enable it in
    /// the registry their stable `invoke` entry point runs nodes from; other registries skip it.
    pub fn record_stable_codecs(&mut self) {
        self.stable_codecs.get_or_insert_with(HashMap::new);
    }

    /// The codec of `node`'s port `port`, when [`Self::record_stable_codecs`] is on and the node
    /// macros know the port's Rust type.
    pub fn stable_codec(&self, node: &str, port: &str) -> Option<&StableCodec> {
        self.stable_codecs
            .as_ref()?
            .get(&(node.to_string(), port.to_string()))
    }

    /// Macro support: node `node`'s port `port` carries `T`. A no-op unless
    /// [`Self::record_stable_codecs`] is on.
    #[doc(hidden)]
    pub fn register_stable_port<T>(
        &mut self,
        node: &str,
        port: &str,
        schema: Option<CoerceFn<T>>,
        serde: Option<CoerceFn<T>>,
        to_value: Option<fn(&T) -> Value>,
    ) where
        T: Send + Sync + 'static,
    {
        let Some(codecs) = &mut self.stable_codecs else {
            return;
        };
        let encode = to_value.map(|to_value| -> StableEncodeFn {
            Arc::new(move |any| any.downcast_ref::<T>().map(to_value))
        });
        let decode: StableDecodeFn = Arc::new(move |key, value| {
            let typed = daedalus_data::typing::coerce_builtin_const_value::<T>(value)
                .or_else(|| schema.and_then(|coerce| coerce(value)))
                .or_else(|| serde.and_then(|coerce| coerce(value)))?;
            Some(Payload::owned(key, typed))
        });
        codecs.insert(
            (node.to_string(), port.to_string()),
            StableCodec {
                type_id: TypeId::of::<T>(),
                rust_type: core::any::type_name::<T>(),
                encode,
                decode,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codecs_are_recorded_only_when_enabled() {
        let mut registry = PluginRegistry::new();
        registry.register_stable_port::<i32>("n", "a", None, None, None);
        assert!(registry.stable_codec("n", "a").is_none());

        registry.record_stable_codecs();
        registry.register_stable_port::<i32>("n", "a", None, None, Some(|v| Value::Int(*v as i64)));
        let codec = registry.stable_codec("n", "a").expect("recorded");
        assert_eq!(codec.type_id, TypeId::of::<i32>());
        let payload = (codec.decode)(TypeKey::new("i32"), &Value::Int(7)).expect("decodes");
        assert_eq!(payload.get_ref::<i32>(), Some(&7));
        let encode = codec.encode.as_ref().expect("encoder");
        assert_eq!(encode(&7_i32), Some(Value::Int(7)));
        assert!((codec.decode)(TypeKey::new("i32"), &Value::Int(1 << 40)).is_none());
    }
}

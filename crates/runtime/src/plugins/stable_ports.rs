//! Declaring how a port of a node registered at runtime crosses the stable plugin path: the
//! codec the node macros record for a `#[node]` port, for nodes built without the macros (a
//! catalog that registers [`NodeDecl`]s and handlers from data, through
//! [`PluginRegistry::register_node_decl`] and `handlers.try_on`).

use super::*;
use crate::const_coerce::{CoerceFn, deserialize_value};
use crate::portable::{Arc, arc_dyn};
use daedalus_data::model::Value;
use daedalus_transport::{ForeignInterface, Local, LocalPort, PodCodec, PodPort, Reusable};

type Declare = Arc<dyn Fn(&mut PluginRegistry, &str, &str) -> PluginResult<()> + Send + Sync>;

/// How one port's Rust type crosses the stable plugin path (see
/// [`PluginRegistry::declare_stable_port`]).
#[derive(Clone)]
pub struct StablePortCodec {
    kind: &'static str,
    declare: Declare,
}

impl core::fmt::Debug for StablePortCodec {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StablePortCodec")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl StablePortCodec {
    /// The port carries `P`, a `#[derive(StablePod)]` struct `T` or a `Vec<T>`: it crosses as a
    /// typed byte view.
    pub fn pod<P: PodPort + Reusable>() -> Self {
        Self {
            kind: "pod",
            declare: arc_dyn!(|registry: &mut PluginRegistry, node: &str, port: &str| {
                registry.register_stable_port::<P>(
                    node,
                    port,
                    None,
                    None,
                    None,
                    Some(PodCodec::of::<P>()),
                    None,
                );
                Ok(())
            }),
        }
    }

    /// The port carries a [`Local<T>`]: it crosses as this build's opaque handle and comes back
    /// as the same object.
    pub fn local<T: Send + Sync + 'static>() -> Self {
        Self {
            kind: "local",
            declare: arc_dyn!(|registry: &mut PluginRegistry, node: &str, port: &str| {
                registry.register_stable_port::<Local<T>>(
                    node,
                    port,
                    None,
                    None,
                    None,
                    None,
                    Some(Local::<T>::codec()),
                );
                Ok(())
            }),
        }
    }

    /// The port carries `T`, crossing as a [`Value`]: `to_value` encodes an output, `from_value`
    /// decodes an input (builtin scalars convert without either).
    pub fn value<T: Send + Sync + 'static>(
        to_value: Option<fn(&T) -> Value>,
        from_value: Option<CoerceFn<T>>,
    ) -> Self {
        Self {
            kind: "value",
            declare: arc_dyn!(
                move |registry: &mut PluginRegistry, node: &str, port: &str| {
                    registry.register_stable_port::<T>(
                        node, port, from_value, None, to_value, None, None,
                    );
                    Ok(())
                }
            ),
        }
    }

    /// [`Self::value`] through `T`'s `ToValue` and serde `Deserialize`.
    pub fn value_of<T>() -> Self
    where
        T: ToValue + DeserializeOwned + Send + Sync + 'static,
    {
        Self::value::<T>(Some(T::to_value), Some(deserialize_value::<T>))
    }

    /// The port carries a payload read through foreign interface `I` (a frame, say): it crosses
    /// as a borrowed handle, which the handler reads with `Payload::foreign_borrow`. Records the
    /// interface, so the plugin's schema lists it.
    pub fn foreign<I: ForeignInterface>() -> Self {
        Self {
            kind: "foreign",
            declare: arc_dyn!(|registry: &mut PluginRegistry, _node: &str, _port: &str| {
                registry.register_foreign_interface::<I>()
            }),
        }
    }
}

impl PluginRegistry {
    /// Declare how port `port` of node `node` (the node's full id) crosses the stable plugin path,
    /// for a node registered without the node macros. A `#[node]` records this itself. Outside a
    /// dynamic plugin's stable registry only a [`StablePortCodec::foreign`] port has an effect.
    pub fn declare_stable_port(
        &mut self,
        node: &str,
        port: &str,
        codec: &StablePortCodec,
    ) -> PluginResult<()> {
        self.ensure_open()?;
        (codec.declare)(self, node, port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::any::TypeId;

    #[test]
    fn declared_ports_record_their_codecs() {
        let mut registry = PluginRegistry::new();
        registry.record_stable_codecs();
        registry
            .declare_stable_port("gen:node", "a", &StablePortCodec::local::<String>())
            .unwrap();
        registry
            .declare_stable_port("gen:node", "b", &StablePortCodec::value_of::<i64>())
            .unwrap();
        let a = registry.stable_codec("gen:node", "a").expect("a");
        assert_eq!(a.type_id, TypeId::of::<Local<String>>());
        assert!(a.local.is_some());
        let b = registry.stable_codec("gen:node", "b").expect("b");
        assert_eq!(b.type_id, TypeId::of::<i64>());
        assert!(b.encode.is_some());
        assert!(registry.stable_codec("gen:node", "c").is_none());
    }
}

//! Per-node caches of values decoded from graph constants.
//!
//! A const input is the same payload on every tick until a patch replaces it, so generated
//! handlers decode it once and keep the result in the node's state slot
//! ([`crate::state::StateStore::take_node_state`]): [`ConfigCache`] for `#[derive(NodeConfig)]`
//! structs, [`DecodedInputs`] for `&T` parameters fed a `Value`. A value is decoded again when
//! any of its input payloads changes (a patched constant, or a new value on a connected edge).

use std::any::{Any, TypeId};

use daedalus_data::model::Value;
use daedalus_transport::Payload;

use crate::NodeError;
use crate::config::{NodeConfig, log_config_changes};
use crate::io::NodeIo;

/// A node's decoded, sanitized and validated config, with the payloads it was built from.
pub struct ConfigCache<C> {
    sources: Vec<Option<Payload>>,
    value: Option<C>,
}

impl<C> Default for ConfigCache<C> {
    fn default() -> Self {
        Self {
            sources: Vec::new(),
            value: None,
        }
    }
}

impl<C: NodeConfig> ConfigCache<C> {
    /// The config built from `io`'s config ports, decoded (`from_io`, `sanitize`, `validate`)
    /// only when one of their payloads differs from the last call's.
    pub fn get(&mut self, io: &NodeIo, node_id: &str) -> Result<&C, NodeError> {
        let names = C::port_names();
        let unchanged = self.value.is_some()
            && self.sources.len() == names.len()
            && names.iter().zip(&self.sources).all(|(name, cached)| {
                match (io.get_payload(name), cached) {
                    (Some(now), Some(cached)) => now.shares_storage(cached),
                    (now, cached) => now.is_none() && cached.is_none(),
                }
            });
        if !unchanged {
            self.value = None;
            let invalid =
                |err: crate::config::ConfigError| NodeError::InvalidInput(err.to_string());
            let sanitized = C::from_io(io)?.sanitize().map_err(invalid)?;
            if !sanitized.changes.is_empty() {
                log_config_changes(node_id, &sanitized.changes);
            }
            sanitized.value.validate().map_err(invalid)?;
            self.sources.clear();
            self.sources
                .extend(names.iter().map(|name| io.get_payload(name).cloned()));
            self.value = Some(sanitized.value);
        }
        Ok(self.value.as_ref().expect("config decoded above"))
    }
}

/// Values decoded from `Value` inputs for `&T` parameters, by port.
#[derive(Default)]
pub struct DecodedInputs {
    entries: Vec<DecodedInput>,
}

struct DecodedInput {
    port: &'static str,
    source: Payload,
    value: Box<dyn Any + Send + Sync>,
}

impl DecodedInputs {
    /// Decode the `Value` on `port` into `T`, unless the cached value came from the same
    /// payload. A port without a `Value` (missing, or a typed payload the handler borrows
    /// directly) or one that does not convert drops its entry.
    pub fn refresh<T: Send + Sync + 'static>(&mut self, io: &NodeIo, port: &'static str) {
        let idx = self.entries.iter().position(|entry| entry.port == port);
        let value = io
            .get_payload(port)
            .filter(|_| TypeId::of::<T>() != TypeId::of::<Value>())
            .and_then(|payload| Some((payload, payload.get_ref::<Value>()?)));
        let Some((payload, value)) = value else {
            if let Some(idx) = idx {
                self.entries.swap_remove(idx);
            }
            return;
        };
        if let Some(idx) = idx {
            let entry = &self.entries[idx];
            if entry.source.shares_storage(payload) && entry.value.is::<T>() {
                return;
            }
            self.entries.swap_remove(idx);
        }
        if let Some(decoded) = io.coerce_value::<T>(value) {
            self.entries.push(DecodedInput {
                port,
                source: payload.clone(),
                value: Box::new(decoded),
            });
        }
    }

    /// The value [`Self::refresh`] decoded for `port`.
    pub fn get<T: 'static>(&self, port: &str) -> Option<&T> {
        let entry = self.entries.iter().find(|entry| entry.port == port)?;
        entry.value.downcast_ref()
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::config::{ConfigError, Sanitized};
    use crate::executor::CorrelatedPayload;
    use crate::handles::PortId;

    fn io_with(port: &'static str, payload: &Payload) -> NodeIo {
        let input = (
            PortId::from_static(port),
            CorrelatedPayload::from_edge(payload.clone()),
        );
        NodeIo::from_inputs([input])
    }

    static DECODES: AtomicUsize = AtomicUsize::new(0);

    #[derive(Clone, Debug, PartialEq)]
    struct Cfg {
        label: String,
    }

    impl NodeConfig for Cfg {
        fn ports(
            _: &daedalus_data::typing::TypeRegistry,
        ) -> Vec<daedalus_registry::capability::PortDecl> {
            Vec::new()
        }
        fn port_names() -> &'static [&'static str] {
            &["label"]
        }
        fn from_io(io: &NodeIo) -> Result<Self, NodeError> {
            DECODES.fetch_add(1, Ordering::SeqCst);
            let label = io.get_typed::<String>("label");
            let missing = || NodeError::InvalidInput("missing label".into());
            Ok(Self {
                label: label.ok_or_else(missing)?,
            })
        }
        fn sanitize(self) -> Result<Sanitized<Self>, ConfigError> {
            Ok(Sanitized {
                value: self,
                changes: Vec::new(),
            })
        }
        fn validate(&self) -> Result<(), ConfigError> {
            Ok(())
        }
    }

    #[test]
    fn config_is_decoded_again_only_when_an_input_changes() {
        let text = |s: &'static str| Payload::owned("value", Value::String(Cow::Borrowed(s)));
        let (first, second) = (text("a"), text("b"));
        let mut cache = ConfigCache::<Cfg>::default();
        let label = |cache: &mut ConfigCache<Cfg>, io: &NodeIo| {
            cache.get(io, "node").map(|cfg| cfg.label.clone())
        };
        let decodes = || DECODES.load(Ordering::SeqCst);

        assert_eq!(label(&mut cache, &io_with("label", &first)).unwrap(), "a");
        assert_eq!(label(&mut cache, &io_with("label", &first)).unwrap(), "a");
        assert_eq!(decodes(), 1, "same payload: decoded once");
        assert_eq!(label(&mut cache, &io_with("label", &second)).unwrap(), "b");
        assert_eq!(decodes(), 2);
        assert!(label(&mut cache, &NodeIo::empty()).is_err());
    }

    #[test]
    fn decoded_inputs_follow_the_payload() {
        let text = |s: &'static str| Payload::owned("value", Value::String(Cow::Borrowed(s)));
        let (first, second) = (text("a"), text("b"));
        let mut decoded = DecodedInputs::default();

        decoded.refresh::<String>(&io_with("label", &first), "label");
        let cached: *const String = decoded.get::<String>("label").expect("decoded");
        decoded.refresh::<String>(&io_with("label", &first.clone()), "label");
        let again: *const String = decoded.get::<String>("label").expect("cached");
        assert_eq!(cached, again, "same payload: not decoded again");

        decoded.refresh::<String>(&io_with("label", &second), "label");
        assert_eq!(
            decoded.get::<String>("label").map(String::as_str),
            Some("b")
        );

        decoded.refresh::<String>(&NodeIo::empty(), "label");
        assert_eq!(decoded.get::<String>("label"), None);
    }
}

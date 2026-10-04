//! Per-node caches of values decoded from graph constants.
//!
//! A const input is the same payload on every tick until a patch replaces it, so generated
//! handlers decode it once and keep the result in the node's state slot
//! ([`crate::state::StateStore::take_node_state`]): [`ConfigCache`] for `#[derive(NodeConfig)]`
//! structs, [`DecodedInputs`] for `&T`, owned `T` and `Option<T>` parameters fed a `Value`
//! (owned ones get a clone of the decoded value). A value is decoded again when any of its input
//! payloads changes (a patched constant, or a new value on a connected edge).

use crate::prelude::*;
use core::any::{Any, TypeId};

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

/// Values decoded from `Value` inputs for `&T`, owned `T` and `Option<T>` parameters, by port.
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
    /// payload, and return whether `port` has a decoded value. A port without a `Value`
    /// (missing, or a typed payload the handler borrows directly) or one that does not convert
    /// drops its entry.
    pub fn refresh<T: Send + Sync + 'static>(&mut self, io: &NodeIo, port: &'static str) -> bool {
        let idx = self.entries.iter().position(|entry| entry.port == port);
        let value = io
            .get_payload(port)
            .filter(|_| TypeId::of::<T>() != TypeId::of::<Value>())
            .and_then(|payload| Some((payload, payload.get_ref::<Value>()?)));
        let Some((payload, value)) = value else {
            if let Some(idx) = idx {
                self.entries.swap_remove(idx);
            }
            return false;
        };
        if let Some(idx) = idx {
            let entry = &self.entries[idx];
            if entry.source.shares_storage(payload) && entry.value.is::<T>() {
                return true;
            }
            self.entries.swap_remove(idx);
        }
        let Some(decoded) = io.coerce_value::<T>(value) else {
            return false;
        };
        self.entries.push(DecodedInput {
            port,
            source: payload.clone(),
            value: Box::new(decoded),
        });
        true
    }

    /// The owned `T` on `port`: a `Value` is decoded once per payload and handed out through
    /// `clone` (`Some(T::clone)` when `T: Clone`); a typed payload, or any input without a
    /// cloner, is taken as [`NodeIo::take_owned`] takes it.
    pub fn take_owned<T: Send + Sync + 'static>(
        &mut self,
        io: &mut NodeIo,
        port: &'static str,
        clone: Option<fn(&T) -> T>,
    ) -> Option<T> {
        match clone {
            Some(clone) if self.refresh::<T>(io, port) => {
                io.take_input_payload(port);
                self.get::<T>(port).map(clone)
            }
            _ => io.take_owned::<T>(port),
        }
    }

    /// [`NodeIo::get_typed`] for `Option<T>` parameters, cloning a decoded `Value` instead of
    /// decoding it on every call.
    pub fn get_typed<T: Clone + Send + Sync + 'static>(
        &mut self,
        io: &NodeIo,
        port: &'static str,
    ) -> Option<T> {
        if self.refresh::<T>(io, port) {
            return self.get::<T>(port).cloned();
        }
        io.get_typed::<T>(port)
    }

    /// The value [`Self::refresh`] decoded for `port`.
    pub fn get<T: 'static>(&self, port: &str) -> Option<&T> {
        let entry = self.entries.iter().find(|entry| entry.port == port)?;
        entry.value.downcast_ref()
    }
}

#[cfg(test)]
mod tests {
    use alloc::borrow::Cow;
    use core::sync::atomic::{AtomicUsize, Ordering};

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

    #[test]
    fn owned_inputs_clone_the_decoded_value() {
        static LABEL_DECODES: AtomicUsize = AtomicUsize::new(0);
        #[derive(Clone, Debug, PartialEq)]
        struct Label(String);
        let coercers = crate::io::new_const_coercer_map();
        let coerce: crate::io::ConstCoercer = Box::new(|value| {
            LABEL_DECODES.fetch_add(1, Ordering::SeqCst);
            let text = value.as_str()?;
            Some(Box::new(Label(text.to_string())) as Box<dyn Any + Send + Sync>)
        });
        coercers
            .write()
            .insert(core::any::type_name::<Label>(), coerce);
        let constant = Payload::owned("value", Value::String(Cow::Borrowed("a")));
        let mut decoded = DecodedInputs::default();
        let mut take = |payload: Payload, clone: Option<fn(&Label) -> Label>| {
            let input = (
                PortId::from_static("label"),
                CorrelatedPayload::from_edge(payload),
            );
            let mut io = NodeIo::from_inputs([input]).with_const_coercers(Some(coercers.clone()));
            let value = decoded.take_owned::<Label>(&mut io, "label", clone);
            assert!(io.get_payload("label").is_none(), "the input is taken");
            value.map(|label| label.0)
        };
        let decodes = || LABEL_DECODES.load(Ordering::SeqCst);

        assert_eq!(
            take(constant.clone(), Some(Label::clone)).as_deref(),
            Some("a")
        );
        assert_eq!(
            take(constant.clone(), Some(Label::clone)).as_deref(),
            Some("a")
        );
        assert_eq!(decodes(), 1, "same constant: decoded once, then cloned");
        assert_eq!(take(constant.clone(), None).as_deref(), Some("a"));
        assert_eq!(decodes(), 2, "without a cloner: decoded per call");
        let typed = Payload::owned("label", Label("t".into()));
        assert_eq!(take(typed, Some(Label::clone)).as_deref(), Some("t"));
        assert_eq!(decodes(), 2, "typed payloads move");
    }
}

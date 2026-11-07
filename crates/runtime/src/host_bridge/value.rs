//! Pushing a `Value` into a host input port, converted to the port's declared type.
//!
//! A host often holds structured data as a `Value` (a parsed config, a JSON document) and has no
//! Rust type for it. The graph declares each host input's type (`input_as`, or the type the planner
//! resolved for it), and [`HostBridgeHandle::push_value`] builds the payload of that type from the
//! `Value` through the registry's conversions, the way a graph constant becomes a typed input:
//!
//! - a key with a payload codec (`register_payload_codec`, a `from_value` conversion, also a
//!   stable-ABI plugin's codec) builds the payload with it;
//! - a key whose Rust type has a const coercer validates the `Value` with it at push time, so a
//!   wrong shape is refused there, and pushes the `Value` itself: node reads coerce it through the
//!   same coercer ([`crate::io::NodeIo::take_owned`]), which is how constants reach typed inputs;
//! - a key the registry names no Rust type for (builtin or structural) takes the `Value` as is.
//!
//! A key registered for a Rust type with neither conversion is refused with
//! [`TypeKeyError::ValueUnconvertible`], and a port with no declared type with
//! [`TypeKeyError::UntypedPort`].

use crate::prelude::*;

use daedalus_data::model::Value;
use daedalus_transport::{FeedOutcome, Payload, TypeKey, TypeKeyError};

use crate::handles::PortId;
use crate::type_index::ValueRoute;

use super::{Direction, HostBridgeBuffers, HostBridgeHandle, PortKey, enqueue_locked, io_timing};

impl HostBridgeHandle {
    /// Feed `value` into inbound `port`, converted to the type the graph declares for the port
    /// (see the module docs). The payload's key is the port's declared key, so the type check
    /// that [`Self::feed_payload`] applies runs on the converted payload. A value the conversion
    /// rejects, or a port with no declared type or no conversion for its type, is
    /// [`FeedOutcome::Rejected`] with the [`TypeKeyError`] naming the port and the fix.
    pub fn push_value(&self, port: impl Into<PortId>, value: Value) -> FeedOutcome {
        let port = port.into();
        let _scope = io_timing::host_alloc_scope();
        if self.shared.io_timing.enabled() {
            return io_timing::timed(&self.shared.io_timing.push_ns, || {
                self.push_value_locked(port, value)
            });
        }
        self.push_value_locked(port, value)
    }

    #[inline(always)]
    fn push_value_locked(&self, port: PortId, value: Value) -> FeedOutcome {
        let mut guard = self.shared.buffers.lock();
        let (payload, checked) = match value_payload(&guard, &port, value) {
            Ok(built) => built,
            Err(error) => return FeedOutcome::Rejected(Box::new(error)),
        };
        let payload = payload.stamp(&guard.clock);
        if checked && let Err(error) = guard.types.check_payload(&payload) {
            return FeedOutcome::Rejected(Box::new(error));
        }
        let (outcome, queued) = enqueue_locked(
            &mut guard,
            Direction::Inbound,
            self.alias.as_str(),
            PortKey::Id(port),
            payload,
        );
        if queued {
            self.wake_after_feed(guard);
        }
        outcome
    }

    /// Declare the type key of inbound `port`, as the graph's planner resolved it. A graph
    /// attaching its bridge calls this for each host input, so [`Self::push_value`] knows what to
    /// build.
    pub(crate) fn declare_input_key(&self, port: impl Into<PortId>, key: TypeKey) {
        let mut guard = self.shared.buffers.lock();
        guard.inbound.port(port.into()).type_key = Some(key);
    }
}

/// The payload of `value` pushed to `port`, and whether it still needs the type check: a
/// `Value` the const coercer accepted is the payload itself, which holds a `Value` under a key
/// whose Rust type is something else, so the check is skipped for it.
fn value_payload(
    buffers: &HostBridgeBuffers,
    port: &PortId,
    value: Value,
) -> Result<(Payload, bool), TypeKeyError> {
    let Some(key) = buffers
        .inbound
        .get(port.as_str())
        .and_then(|state| state.type_key.clone())
    else {
        return Err(TypeKeyError::UntypedPort {
            port: port.as_str().to_string(),
        });
    };
    let rejected = |reason: String| TypeKeyError::ValueRejected {
        port: port.as_str().to_string(),
        type_key: key.clone(),
        reason,
    };
    match buffers.types.value_route(&key) {
        ValueRoute::Codec(from_value) => {
            let payload = from_value(key.clone(), &value).map_err(rejected)?;
            Ok((payload, true))
        }
        ValueRoute::Coerce {
            coercers,
            rust_type,
        } => {
            let accepted = coercers
                .read()
                .get(rust_type)
                .and_then(|coerce| coerce(&value))
                .is_some();
            if !accepted {
                return Err(rejected(format!(
                    "the const coercer of `{rust_type}` does not accept it"
                )));
            }
            Ok((Payload::owned(key, value), false))
        }
        ValueRoute::Passthrough => Ok((Payload::owned(key, value), true)),
        ValueRoute::Unconvertible { rust_type } => Err(TypeKeyError::ValueUnconvertible {
            port: port.as_str().to_string(),
            type_key: key,
            rust_type: rust_type.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host_bridge::{HostBridgeManager, HostBridgePayload};
    use crate::io::{ConstCoercer, new_const_coercer_map};
    use crate::portable::Arc;
    use crate::type_index::{TypeIndex, ValueFromFn};
    use core::any::{Any, type_name};
    use daedalus_transport::RustTypeIdentity;

    const PAIR: &str = "test:value:pair";
    const PLAIN: &str = "test:value:plain";

    #[derive(Debug, PartialEq)]
    struct Pair(i64, i64);

    fn tuple(a: i64, b: i64) -> Value {
        Value::Tuple(vec![Value::Int(a), Value::Int(b)])
    }

    fn pair_from_value(key: TypeKey, value: &Value) -> Result<Payload, String> {
        match value {
            Value::Tuple(items) => match items.as_slice() {
                [Value::Int(a), Value::Int(b)] => Ok(Payload::owned(key, Pair(*a, *b))),
                _ => Err("expected two ints".into()),
            },
            _ => Err("expected a tuple".into()),
        }
    }

    fn pair_coercer() -> ConstCoercer {
        Box::new(|value| match value {
            Value::Tuple(items) => match items.as_slice() {
                [Value::Int(a), Value::Int(b)] => {
                    Some(Box::new(Pair(*a, *b)) as Box<dyn Any + Send + Sync>)
                }
                _ => None,
            },
            _ => None,
        })
    }

    fn pair_rust_type() -> (TypeKey, RustTypeIdentity) {
        (TypeKey::new(PAIR), RustTypeIdentity::of::<Pair>())
    }

    /// A bridge whose input port `in` is declared `key` (or undeclared).
    fn bridge(types: TypeIndex, key: Option<&str>) -> HostBridgeHandle {
        let manager = HostBridgeManager::new();
        manager.set_type_index(types);
        let handle = manager.ensure_handle("host");
        if let Some(key) = key {
            handle.declare_input_key("in", TypeKey::new(key));
        }
        handle
    }

    fn taken(handle: &HostBridgeHandle) -> Payload {
        let mut out: Vec<HostBridgePayload> = Vec::new();
        handle.take_inbound_into(&mut out);
        out.pop().expect("one queued payload").payload
    }

    fn rejected(outcome: FeedOutcome) -> TypeKeyError {
        match outcome {
            FeedOutcome::Rejected(error) => *error,
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    #[test]
    fn an_undeclared_port_names_no_type_to_build() {
        let handle = bridge(TypeIndex::builtin().clone(), None);
        let error = rejected(handle.push_value("in", tuple(1, 2)));
        assert!(
            matches!(error, TypeKeyError::UntypedPort { ref port } if port == "in"),
            "{error}"
        );
    }

    #[test]
    fn a_builtin_or_unregistered_key_takes_the_value_as_is() {
        let handle = bridge(TypeIndex::builtin().clone(), Some(PLAIN));
        assert!(matches!(
            handle.push_value("in", Value::Int(4)),
            FeedOutcome::Accepted { .. }
        ));
        assert_eq!(taken(&handle).get_ref::<Value>(), Some(&Value::Int(4)));
    }

    #[test]
    fn a_payload_codec_builds_the_payload_of_its_key() {
        let (key, identity) = pair_rust_type();
        let codec: ValueFromFn = Arc::new(pair_from_value);
        let types = TypeIndex::new_with_values([], [(key.clone(), identity)], [(key, codec)], None);
        let handle = bridge(types, Some(PAIR));

        assert!(matches!(
            handle.push_value("in", tuple(2, 3)),
            FeedOutcome::Accepted { .. }
        ));
        assert_eq!(taken(&handle).get_ref::<Pair>(), Some(&Pair(2, 3)));

        let error = rejected(handle.push_value("in", Value::Int(1)));
        assert!(
            matches!(error, TypeKeyError::ValueRejected { ref port, ref reason, .. }
                if port == "in" && reason == "expected a tuple"),
            "{error}"
        );
        assert_eq!(handle.pending_inbound(), 0);
    }

    #[test]
    fn a_const_coercer_checks_the_value_and_the_value_is_the_payload() {
        let (key, identity) = pair_rust_type();
        let coercers = new_const_coercer_map();
        coercers.write().insert(type_name::<Pair>(), pair_coercer());
        let types = TypeIndex::new_with_values([], [(key, identity)], [], Some(coercers));
        let handle = bridge(types, Some(PAIR));

        assert!(matches!(
            handle.push_value("in", tuple(5, 6)),
            FeedOutcome::Accepted { .. }
        ));
        // Node reads coerce the `Value` with the same coercer.
        assert_eq!(taken(&handle).get_ref::<Value>(), Some(&tuple(5, 6)));

        let error = rejected(handle.push_value("in", Value::Int(1)));
        assert!(
            matches!(error, TypeKeyError::ValueRejected { ref type_key, .. } if type_key.as_str() == PAIR),
            "{error}"
        );
    }

    #[test]
    fn a_registered_type_without_a_conversion_is_unconvertible() {
        let (key, identity) = pair_rust_type();
        for coercers in [None, Some(new_const_coercer_map())] {
            let types = TypeIndex::new_with_values([], [(key.clone(), identity)], [], coercers);
            let handle = bridge(types, Some(PAIR));
            let error = rejected(handle.push_value("in", tuple(1, 1)));
            assert!(
                matches!(error, TypeKeyError::ValueUnconvertible { ref rust_type, .. }
                    if rust_type.ends_with("Pair")),
                "{error}"
            );
        }
    }
}

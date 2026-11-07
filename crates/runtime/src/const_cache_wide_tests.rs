//! A 49-field `Copy` config taken by value, fed by 49 const inputs (plus three fixed consts, as a
//! stage of the group has), across four stage caches: a steady tick decodes nothing, an edge on
//! one port decodes once for the change and once more when the edge goes quiet, and a new const
//! list decodes once per stage.

use super::*;
use crate::NodeError;
use crate::config::{ConfigError, Sanitized};
use crate::executor::CorrelatedPayload;
use crate::handles::PortId;
use crate::io::{NodeConstInputs, NodeIo, port_buffer};
use crate::portable::Arc;
use core::sync::atomic::{AtomicUsize, Ordering};
use daedalus_data::model::Value;
use daedalus_transport::Payload;

/// `from_io` calls of every [`Wide`] config (only this file's test uses them).
static WIDE_DECODES: AtomicUsize = AtomicUsize::new(0);

/// A config struct of scalar fields, each read as its own typed const input, plus the impls
/// `#[derive(NodeConfig)]` would generate: `from_io` calls `get_typed` once per field.
macro_rules! wide_config {
    ($name:ident; $($f:ident: $t:ty = $v:expr),* $(,)?) => {
        #[derive(Clone, Copy, Debug, PartialEq)]
        struct $name { $($f: $t),* }

        impl $name {
            /// One typed const per field, in declaration order.
            fn const_inputs() -> Vec<(PortId, CorrelatedPayload)> {
                vec![$((
                    PortId::from_static(stringify!($f)),
                    CorrelatedPayload::from_edge(Payload::owned(stringify!($t), $v as $t)),
                )),*]
            }
        }

        impl NodeConfig for $name {
            fn ports(
                _: &daedalus_data::typing::TypeRegistry,
            ) -> Vec<daedalus_registry::capability::PortDecl> {
                Vec::new()
            }
            fn port_names() -> &'static [&'static str] {
                &[$(stringify!($f)),*]
            }
            fn from_io(io: &NodeIo) -> Result<Self, NodeError> {
                WIDE_DECODES.fetch_add(1, Ordering::SeqCst);
                Ok(Self {
                    $($f: io
                        .get_typed::<$t>(stringify!($f))
                        .ok_or_else(|| NodeError::missing_input(stringify!($f)))?),*
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
    };
}

wide_config! {
    Wide;
    f00: usize = 4, f01: i16 = -3, f02: u8 = 7, f03: u32 = 1000, f04: f32 = 0.5, f05: bool = true,
    f06: usize = 4, f07: i16 = -3, f08: u8 = 7, f09: u32 = 1000, f10: f32 = 0.5, f11: bool = true,
    f12: usize = 4, f13: i16 = -3, f14: u8 = 7, f15: u32 = 1000, f16: f32 = 0.5, f17: bool = true,
    f18: usize = 4, f19: i16 = -3, f20: u8 = 7, f21: u32 = 1000, f22: f32 = 0.5, f23: bool = true,
    f24: usize = 4, f25: i16 = -3, f26: u8 = 7, f27: u32 = 1000, f28: f32 = 0.5, f29: bool = true,
    f30: usize = 4, f31: i16 = -3, f32: u8 = 7, f33: u32 = 1000, f34: f32 = 0.5, f35: bool = true,
    f36: usize = 4, f37: i16 = -3, f38: u8 = 7, f39: u32 = 1000, f40: f32 = 0.5, f41: bool = true,
    f42: usize = 4, f43: i16 = -3, f44: u8 = 7, f45: u32 = 1000, f46: f32 = 0.5, f47: bool = true,
    f48: usize = 4,
}

/// The const list of a stage: its 49 config consts and three fixed ones (52 entries, under the
/// shared-list limit).
fn stage_consts() -> Arc<NodeConstInputs> {
    let mut list = Wide::const_inputs();
    for name in ["fixed_a", "fixed_b", "fixed_c"] {
        list.push((
            PortId::from_static(name),
            CorrelatedPayload::from_edge(Payload::owned("value", Value::Int(1))),
        ));
    }
    Arc::new(list.into())
}

/// A stage's io for one call: its consts (shared), and an optional edge input.
fn call(list: &Arc<NodeConstInputs>, edge: Option<(PortId, CorrelatedPayload)>) -> NodeIo {
    let mut buffer = port_buffer();
    buffer.extend(edge);
    NodeIo::for_call(buffer, Arc::default(), Some(list))
}

fn read(cache: &mut ConfigCache<Wide>, io: &NodeIo) -> Wide {
    *cache.get(io, "stage").unwrap()
}

fn decodes() -> usize {
    WIDE_DECODES.load(Ordering::SeqCst)
}

#[test]
fn wide_group_config_decodes_on_change_only() {
    let stages = 4;
    let list = stage_consts();
    let mut caches: Vec<ConfigCache<Wide>> = (0..stages).map(|_| ConfigCache::default()).collect();
    let expected = Wide::const_inputs();

    // First tick: each stage decodes once, to the const values.
    for cache in caches.iter_mut() {
        assert_eq!(read(cache, &call(&list, None)).f03, 1000);
    }
    assert_eq!(decodes(), stages, "one decode per stage on the first tick");
    for cache in &caches {
        assert!(cache.consts.is_some(), "the shared-list fast path is armed");
    }
    assert_eq!(expected.len(), 49);

    // Steady ticks: no from_io at all.
    let steady = decodes();
    for _ in 0..1000 {
        for cache in caches.iter_mut() {
            assert_eq!(read(cache, &call(&list, None)).f48, 4);
        }
    }
    assert_eq!(decodes(), steady, "a steady tick decodes nothing");

    // An edge on one port (a new u32) decodes that stage once, with the new value.
    let edged = (
        PortId::from_static("f03"),
        CorrelatedPayload::from_edge(Payload::owned("u32", 99u32)),
    );
    assert_eq!(read(&mut caches[0], &call(&list, Some(edged))).f03, 99);
    assert_eq!(decodes(), steady + 1);

    // The edge goes quiet: the const comes back once, and the fast path is re-armed.
    assert_eq!(read(&mut caches[0], &call(&list, None)).f03, 1000);
    assert_eq!(decodes(), steady + 2);
    assert!(caches[0].consts.is_some());
    for _ in 0..1000 {
        assert_eq!(read(&mut caches[0], &call(&list, None)).f03, 1000);
    }
    assert_eq!(decodes(), steady + 2, "quiet again: no further decodes");

    // A new const list (a patched graph) decodes once per stage, then settles.
    let patched = {
        let mut next = Wide::const_inputs();
        next[0].1 = CorrelatedPayload::from_edge(Payload::owned("usize", 9usize));
        for name in ["fixed_a", "fixed_b", "fixed_c"] {
            next.push((
                PortId::from_static(name),
                CorrelatedPayload::from_edge(Payload::owned("value", Value::Int(1))),
            ));
        }
        Arc::new(next.into())
    };
    let before_patch = decodes();
    for cache in caches.iter_mut() {
        assert_eq!(read(cache, &call(&patched, None)).f00, 9);
    }
    assert_eq!(decodes(), before_patch + stages);
    for cache in caches.iter_mut() {
        read(cache, &call(&patched, None));
    }
    assert_eq!(decodes(), before_patch + stages);
}

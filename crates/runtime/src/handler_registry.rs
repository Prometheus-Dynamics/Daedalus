use std::collections::HashMap;
use std::sync::{Arc, Mutex, TryLockError};

use crate::executor::{DirectPayloadFn, NodeError};
use crate::io::NodeIo;
use crate::plan::{NodeExecutionKind, RuntimeNode, runtime_node_execution_kind};
use crate::state::ExecutionContext;

/// Helper registry mapping node ids to stateless/stateful handlers.
type StatelessFn = Arc<
    dyn Fn(&RuntimeNode, &ExecutionContext, &mut NodeIo) -> Result<(), NodeError> + Send + Sync,
>;
type StatefulFn = Arc<
    Mutex<
        Box<
            dyn FnMut(&RuntimeNode, &ExecutionContext, &mut NodeIo) -> Result<(), NodeError> + Send,
        >,
    >,
>;

#[derive(Clone)]
pub struct HandlerRegistry {
    // Hot path: keyed by stable numeric ids, not strings.
    stateless: HashMap<u128, StatelessFn>,
    stateful: HashMap<u128, StatefulFn>,
    direct_payload: HashMap<u128, DirectPayloadFn>,
    // Collision detection + support for prefixing.
    ids: HashMap<u128, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum HandlerRegistryError {
    #[error("handler id collision: id='{previous}' and id='{current}' map to {stable_id:x}")]
    HandlerIdCollision {
        previous: String,
        current: String,
        stable_id: u128,
    },
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self {
            stateless: HashMap::new(),
            stateful: HashMap::new(),
            direct_payload: HashMap::new(),
            ids: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.stateless.is_empty() && self.stateful.is_empty() && self.direct_payload.is_empty()
    }

    fn key_for_id(id: &str) -> u128 {
        daedalus_core::stable_id::stable_id128("node", id)
    }

    fn record_id(&mut self, key: u128, id: &str) -> Result<(), HandlerRegistryError> {
        if let Some(prev) = self.ids.get(&key) {
            if prev != id {
                return Err(HandlerRegistryError::HandlerIdCollision {
                    previous: prev.clone(),
                    current: id.to_string(),
                    stable_id: key,
                });
            }
            return Ok(());
        }
        self.ids.insert(key, id.to_string());
        Ok(())
    }

    /// Register a stateless handler.
    ///
    /// # Panics
    ///
    /// Panics when the handler id collides with another registered stable id.
    /// Use [`Self::try_on`] to receive a typed registration error instead.
    pub fn on<F>(&mut self, id: &str, f: F)
    where
        F: Fn(&RuntimeNode, &ExecutionContext, &mut NodeIo) -> Result<(), NodeError>
            + Send
            + Sync
            + 'static,
    {
        self.try_on(id, f)
            .unwrap_or_else(|err| panic!("daedalus-runtime: {err}"));
    }

    pub fn try_on<F>(&mut self, id: &str, f: F) -> Result<(), HandlerRegistryError>
    where
        F: Fn(&RuntimeNode, &ExecutionContext, &mut NodeIo) -> Result<(), NodeError>
            + Send
            + Sync
            + 'static,
    {
        let key = Self::key_for_id(id);
        tracing::trace!(
            target: "daedalus_runtime::handler_registry",
            node_id = id,
            stable_id = format_args!("{key:x}"),
            handler_kind = "stateless",
            "register handler"
        );
        self.record_id(key, id)?;
        self.stateless.insert(key, Arc::new(f));
        Ok(())
    }

    /// Register a stateful handler.
    ///
    /// Stateful handlers are shared by handler id. Cloning a registry with
    /// [`Self::clone_arc`] or prefixing it with [`Self::try_with_prefix`] preserves the same
    /// `FnMut` instance behind a mutex, so calls for that handler id are serialized through one
    /// stateful closure. Register separate handler ids when each node/alias needs independent
    /// mutable state. Re-entering the same stateful handler id while it is already running returns
    /// a `NodeError` instead of blocking indefinitely.
    ///
    /// # Panics
    ///
    /// Panics when the handler id collides with another registered stable id.
    /// Use [`Self::try_on_stateful`] to receive a typed registration error instead.
    pub fn on_stateful<F>(&mut self, id: &str, f: F)
    where
        F: FnMut(&RuntimeNode, &ExecutionContext, &mut NodeIo) -> Result<(), NodeError>
            + Send
            + 'static,
    {
        self.try_on_stateful(id, f)
            .unwrap_or_else(|err| panic!("daedalus-runtime: {err}"));
    }

    pub fn try_on_stateful<F>(&mut self, id: &str, f: F) -> Result<(), HandlerRegistryError>
    where
        F: FnMut(&RuntimeNode, &ExecutionContext, &mut NodeIo) -> Result<(), NodeError>
            + Send
            + 'static,
    {
        let key = Self::key_for_id(id);
        tracing::trace!(
            target: "daedalus_runtime::handler_registry",
            node_id = id,
            stable_id = format_args!("{key:x}"),
            handler_kind = "stateful",
            "register handler"
        );
        self.record_id(key, id)?;
        self.stateful.insert(key, Arc::new(Mutex::new(Box::new(f))));
        Ok(())
    }

    /// Register a direct-payload handler.
    ///
    /// # Panics
    ///
    /// Panics when the handler id collides with another registered stable id.
    /// Use [`Self::try_on_direct_payload`] to receive a typed registration error instead.
    pub fn on_direct_payload<F>(&mut self, id: &str, f: F)
    where
        F: Fn(
                &RuntimeNode,
                &ExecutionContext,
                daedalus_transport::Payload,
            ) -> Result<Option<daedalus_transport::Payload>, NodeError>
            + Send
            + Sync
            + 'static,
    {
        self.try_on_direct_payload(id, f)
            .unwrap_or_else(|err| panic!("daedalus-runtime: {err}"));
    }

    pub fn try_on_direct_payload<F>(&mut self, id: &str, f: F) -> Result<(), HandlerRegistryError>
    where
        F: Fn(
                &RuntimeNode,
                &ExecutionContext,
                daedalus_transport::Payload,
            ) -> Result<Option<daedalus_transport::Payload>, NodeError>
            + Send
            + Sync
            + 'static,
    {
        let key = Self::key_for_id(id);
        tracing::trace!(
            target: "daedalus_runtime::handler_registry",
            node_id = id,
            stable_id = format_args!("{key:x}"),
            handler_kind = "direct_payload",
            "register handler"
        );
        self.record_id(key, id)?;
        self.direct_payload.insert(key, Arc::new(f));
        Ok(())
    }

    /// Merge another registry.
    ///
    /// # Panics
    ///
    /// Panics when the merged registries contain colliding stable handler ids.
    /// Use [`Self::try_merge`] to receive a typed registration error instead.
    pub fn merge(&mut self, other: HandlerRegistry) {
        self.try_merge(other)
            .unwrap_or_else(|err| panic!("daedalus-runtime: {err}"));
    }

    pub fn try_merge(&mut self, other: HandlerRegistry) -> Result<(), HandlerRegistryError> {
        for (k, v) in other.ids {
            self.record_id(k, &v)?;
        }
        self.stateless.extend(other.stateless);
        self.stateful.extend(other.stateful);
        self.direct_payload.extend(other.direct_payload);
        Ok(())
    }

    pub fn has_handler(&self, id: &str) -> bool {
        let key = Self::key_for_id(id);
        self.stateless.contains_key(&key)
            || self.stateful.contains_key(&key)
            || self.direct_payload.contains_key(&key)
    }

    /// Return a copy with every registered handler id prefixed.
    ///
    /// # Panics
    ///
    /// Panics when prefixing creates colliding stable handler ids. Use
    /// [`Self::try_with_prefix`] to receive a typed registration error instead.
    pub fn with_prefix(self, prefix: &str) -> Self {
        self.try_with_prefix(prefix)
            .unwrap_or_else(|err| panic!("daedalus-runtime: {err}"))
    }

    pub fn try_with_prefix(self, prefix: &str) -> Result<Self, HandlerRegistryError> {
        if prefix.is_empty() {
            return Ok(self);
        }
        let mut out = HandlerRegistry::new();
        for (k, v) in self.stateless {
            let Some(old_id) = self.ids.get(&k) else {
                continue;
            };
            let new_id = crate::apply_node_prefix(prefix, old_id);
            let new_key = Self::key_for_id(&new_id);
            out.record_id(new_key, &new_id)?;
            out.stateless.insert(new_key, v);
        }
        for (k, v) in self.stateful {
            let Some(old_id) = self.ids.get(&k) else {
                continue;
            };
            let new_id = crate::apply_node_prefix(prefix, old_id);
            let new_key = Self::key_for_id(&new_id);
            out.record_id(new_key, &new_id)?;
            out.stateful.insert(new_key, v);
        }
        for (k, v) in self.direct_payload {
            let Some(old_id) = self.ids.get(&k) else {
                continue;
            };
            let new_id = crate::apply_node_prefix(prefix, old_id);
            let new_key = Self::key_for_id(&new_id);
            out.record_id(new_key, &new_id)?;
            out.direct_payload.insert(new_key, v);
        }
        Ok(out)
    }

    /// Cheap clone by cloning the underlying Arcs/Mutexes.
    pub fn clone_arc(&self) -> Self {
        Self {
            stateless: self.stateless.clone(),
            stateful: self.stateful.clone(),
            direct_payload: self.direct_payload.clone(),
            ids: self.ids.clone(),
        }
    }
}

impl crate::executor::NodeHandler for HandlerRegistry {
    fn run(
        &self,
        node: &RuntimeNode,
        ctx: &ExecutionContext,
        io: &mut NodeIo,
    ) -> Result<(), NodeError> {
        if let Some(f) = self.stateless.get(&node.stable_id) {
            tracing::trace!(
                target: "daedalus_runtime::handler_registry",
                node_id = %node.id,
                stable_id = format_args!("{:x}", node.stable_id),
                handler_kind = "stateless",
                "run handler"
            );
            return f(node, ctx, io);
        }
        if let Some(f) = self.stateful.get(&node.stable_id) {
            tracing::trace!(
                target: "daedalus_runtime::handler_registry",
                node_id = %node.id,
                stable_id = format_args!("{:x}", node.stable_id),
                handler_kind = "stateful",
                "run handler"
            );
            let mut handler = match f.try_lock() {
                Ok(handler) => handler,
                Err(TryLockError::WouldBlock) => {
                    return Err(NodeError::Handler(format!(
                        "stateful handler {} is already running; reentrant calls are not supported",
                        node.id
                    )));
                }
                Err(TryLockError::Poisoned(_)) => {
                    return Err(NodeError::Handler("stateful handler lock poisoned".into()));
                }
            };
            return handler(node, ctx, io);
        }

        tracing::warn!(
            target: "daedalus_runtime::handler_registry",
            node_id = %node.id,
            stable_id = format_args!("{:x}", node.stable_id),
            "missing handler"
        );
        match runtime_node_execution_kind(node) {
            NodeExecutionKind::NoOp | NodeExecutionKind::HostBridge => Ok(()),
            NodeExecutionKind::External => Err(NodeError::ExternalHandlerUnavailable {
                node: node.id.clone(),
                stable_id: node.stable_id,
            }),
            NodeExecutionKind::HandlerRequired => Err(NodeError::MissingHandler {
                node: node.id.clone(),
                stable_id: node.stable_id,
            }),
        }
    }

    fn direct_payload_handler(&self, stable_id: u128) -> Option<DirectPayloadFn> {
        self.direct_payload.get(&stable_id).cloned()
    }
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use daedalus_planner::ComputeAffinity;

    fn test_node(id: &str) -> RuntimeNode {
        RuntimeNode {
            id: id.to_string(),
            stable_id: daedalus_core::stable_id::stable_id128("node", id),
            bundle: None,
            label: None,
            compute: ComputeAffinity::CpuOnly,
            const_inputs: Vec::new(),
            sync_groups: Vec::new(),
            metadata: Default::default(),
        }
    }

    fn test_context() -> ExecutionContext {
        ExecutionContext {
            state: crate::state::StateStore::default(),
            node_id: Arc::<str>::from("node"),
            metadata: Arc::new(Default::default()),
            graph_metadata: Arc::new(Default::default()),
            capabilities: Arc::new(crate::capabilities::CapabilityRegistry::new()),
            #[cfg(feature = "gpu")]
            gpu: None,
        }
    }

    #[test]
    fn try_merge_reports_handler_id_collision() {
        let mut left = HandlerRegistry::new();
        left.ids.insert(42, "left".to_string());

        let mut right = HandlerRegistry::new();
        right.ids.insert(42, "right".to_string());

        let err = left.try_merge(right).expect_err("collision error");
        assert_eq!(
            err,
            HandlerRegistryError::HandlerIdCollision {
                previous: "left".to_string(),
                current: "right".to_string(),
                stable_id: 42,
            }
        );
    }

    #[test]
    fn stateful_handlers_are_shared_across_clone_and_prefix() {
        let mut registry = HandlerRegistry::new();
        let mut calls = 0_u32;
        registry.on_stateful("counter", move |_node, _ctx, io| {
            calls += 1;
            io.push(Some("out"), calls);
            Ok(())
        });
        let prefixed = registry
            .clone_arc()
            .try_with_prefix("prefixed")
            .expect("prefix should preserve handler");
        let ctx = test_context();

        let mut first_io = NodeIo::empty();
        crate::executor::NodeHandler::run(&registry, &test_node("counter"), &ctx, &mut first_io)
            .expect("first stateful run");
        let first = first_io
            .take_outputs()
            .remove(0)
            .1
            .inner
            .get_ref::<u32>()
            .copied();

        let mut second_io = NodeIo::empty();
        crate::executor::NodeHandler::run(
            &prefixed,
            &test_node("prefixed:counter"),
            &ctx,
            &mut second_io,
        )
        .expect("prefixed stateful run");
        let second = second_io
            .take_outputs()
            .remove(0)
            .1
            .inner
            .get_ref::<u32>()
            .copied();

        assert_eq!(first, Some(1));
        assert_eq!(second, Some(2));
    }

    #[test]
    fn reentrant_stateful_handler_returns_error_instead_of_deadlocking() {
        let holder = Arc::new(Mutex::new(None::<HandlerRegistry>));
        let holder_for_handler = Arc::clone(&holder);
        let mut registry = HandlerRegistry::new();
        registry.on_stateful("reentrant", move |node, ctx, _io| {
            let nested = holder_for_handler
                .lock()
                .expect("holder lock")
                .as_ref()
                .expect("registry installed")
                .clone_arc();
            let mut nested_io = NodeIo::empty();
            let err = crate::executor::NodeHandler::run(&nested, node, ctx, &mut nested_io)
                .expect_err("reentrant stateful call should fail");
            assert!(err.to_string().contains("already running"));
            Ok(())
        });
        *holder.lock().expect("holder lock") = Some(registry.clone_arc());

        let ctx = test_context();
        let mut io = NodeIo::empty();
        crate::executor::NodeHandler::run(&registry, &test_node("reentrant"), &ctx, &mut io)
            .expect("outer stateful run");
    }
}

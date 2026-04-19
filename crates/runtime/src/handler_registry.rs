use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::executor::NodeError;
use crate::io::NodeIo;
use crate::plan::RuntimeNode;
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

pub struct HandlerRegistry {
    // Hot path: keyed by stable numeric ids, not strings.
    stateless: HashMap<u128, StatelessFn>,
    stateful: HashMap<u128, StatefulFn>,
    // Collision detection + support for prefixing.
    ids: HashMap<u128, String>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self {
            stateless: HashMap::new(),
            stateful: HashMap::new(),
            ids: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.stateless.is_empty() && self.stateful.is_empty()
    }

    fn key_for_id(id: &str) -> u128 {
        daedalus_core::stable_id::stable_id128("node", id)
    }

    fn record_id(&mut self, key: u128, id: &str) {
        if let Some(prev) = self.ids.insert(key, id.to_string())
            && prev != id
        {
            // "ids can never collide": detect and refuse to run with ambiguous ids.
            panic!(
                "daedalus-runtime: handler id collision: id='{}' and id='{}' map to {:x}",
                prev, id, key
            );
        }
    }

    pub fn on<F>(&mut self, id: &str, f: F)
    where
        F: Fn(&RuntimeNode, &ExecutionContext, &mut NodeIo) -> Result<(), NodeError>
            + Send
            + Sync
            + 'static,
    {
        if std::env::var_os("DAEDALUS_TRACE_HANDLER_REGISTER").is_some() {
            tracing::warn!("daedalus-runtime: register handler id={}", id);
        }
        let key = Self::key_for_id(id);
        self.record_id(key, id);
        self.stateless.insert(key, Arc::new(f));
    }

    pub fn on_stateful<F>(&mut self, id: &str, f: F)
    where
        F: FnMut(&RuntimeNode, &ExecutionContext, &mut NodeIo) -> Result<(), NodeError>
            + Send
            + 'static,
    {
        if std::env::var_os("DAEDALUS_TRACE_HANDLER_REGISTER").is_some() {
            tracing::warn!("daedalus-runtime: register stateful handler id={}", id);
        }
        let key = Self::key_for_id(id);
        self.record_id(key, id);
        self.stateful.insert(key, Arc::new(Mutex::new(Box::new(f))));
    }

    pub fn merge(&mut self, other: HandlerRegistry) {
        for (k, v) in other.ids {
            self.record_id(k, &v);
        }
        self.stateless.extend(other.stateless);
        self.stateful.extend(other.stateful);
    }

    pub fn has_handler(&self, id: &str) -> bool {
        let key = Self::key_for_id(id);
        self.stateless.contains_key(&key) || self.stateful.contains_key(&key)
    }

    pub fn with_prefix(self, prefix: &str) -> Self {
        if prefix.is_empty() {
            return self;
        }
        let mut out = HandlerRegistry::new();
        for (k, v) in self.stateless {
            let Some(old_id) = self.ids.get(&k) else {
                continue;
            };
            let new_id = crate::apply_node_prefix(prefix, old_id);
            let new_key = Self::key_for_id(&new_id);
            out.record_id(new_key, &new_id);
            out.stateless.insert(new_key, v);
        }
        for (k, v) in self.stateful {
            let Some(old_id) = self.ids.get(&k) else {
                continue;
            };
            let new_id = crate::apply_node_prefix(prefix, old_id);
            let new_key = Self::key_for_id(&new_id);
            out.record_id(new_key, &new_id);
            out.stateful.insert(new_key, v);
        }
        out
    }

    /// Cheap clone by cloning the underlying Arcs/Mutexes.
    pub fn clone_arc(&self) -> Self {
        Self {
            stateless: self.stateless.clone(),
            stateful: self.stateful.clone(),
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
        if std::env::var_os("DAEDALUS_TRACE_HANDLER_RUN").is_some() && node.id == "cv:image:to_gray"
        {
            let has_stateless = self.stateless.contains_key(&node.stable_id);
            let has_stateful = self.stateful.contains_key(&node.stable_id);
            tracing::warn!(
                "daedalus-runtime: handler run node={} stable_id={:x} stateless={} stateful={}",
                node.id,
                node.stable_id,
                has_stateless,
                has_stateful
            );
        }

        if let Some(f) = self.stateless.get(&node.stable_id) {
            return f(node, ctx, io);
        }
        if let Some(f) = self.stateful.get(&node.stable_id) {
            return f.lock().unwrap()(node, ctx, io);
        }

        if std::env::var_os("DAEDALUS_TRACE_MISSING_HANDLERS").is_some() {
            tracing::warn!(
                "daedalus-runtime: missing handler for node id={} stable_id={:x}",
                node.id,
                node.stable_id
            );
        }
        if std::env::var_os("DAEDALUS_TRACE_MISSING_HANDLERS_STDERR").is_some() {
            eprintln!(
                "daedalus-runtime: missing handler for node id={} stable_id={:x}",
                node.id, node.stable_id
            );
        }
        Ok(())
    }
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

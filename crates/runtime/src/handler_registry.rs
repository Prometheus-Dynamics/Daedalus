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
    stateless: HashMap<String, StatelessFn>,
    stateful: HashMap<String, StatefulFn>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self {
            stateless: HashMap::new(),
            stateful: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.stateless.is_empty() && self.stateful.is_empty()
    }

    pub fn on<F>(&mut self, id: &str, f: F)
    where
        F: Fn(&RuntimeNode, &ExecutionContext, &mut NodeIo) -> Result<(), NodeError>
            + Send
            + Sync
            + 'static,
    {
        if std::env::var_os("DAEDALUS_TRACE_HANDLER_REGISTER").is_some() {
            log::warn!("daedalus-runtime: register handler id={}", id);
        }
        self.stateless.insert(id.to_string(), Arc::new(f));
    }

    pub fn on_stateful<F>(&mut self, id: &str, f: F)
    where
        F: FnMut(&RuntimeNode, &ExecutionContext, &mut NodeIo) -> Result<(), NodeError>
            + Send
            + 'static,
    {
        if std::env::var_os("DAEDALUS_TRACE_HANDLER_REGISTER").is_some() {
            log::warn!(
                "daedalus-runtime: register stateful handler id={}",
                id
            );
        }
        self.stateful
            .insert(id.to_string(), Arc::new(Mutex::new(Box::new(f))));
    }

    pub fn merge(&mut self, other: HandlerRegistry) {
        self.stateless.extend(other.stateless);
        self.stateful.extend(other.stateful);
    }

    pub fn has_handler(&self, id: &str) -> bool {
        self.stateless.contains_key(id) || self.stateful.contains_key(id)
    }

    pub fn with_prefix(self, prefix: &str) -> Self {
        if prefix.is_empty() {
            return self;
        }
        let mut out = HandlerRegistry::new();
        for (k, v) in self.stateless {
            out.stateless
                .insert(crate::apply_node_prefix(prefix, &k), v);
        }
        for (k, v) in self.stateful {
            out.stateful.insert(crate::apply_node_prefix(prefix, &k), v);
        }
        out
    }

    /// Cheap clone by cloning the underlying Arcs/Mutexes.
    pub fn clone_arc(&self) -> Self {
        Self {
            stateless: self.stateless.clone(),
            stateful: self.stateful.clone(),
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
        if std::env::var_os("DAEDALUS_TRACE_HANDLER_RUN").is_some()
            && node.id == "cv:image:to_gray"
        {
            let has_stateless = self.stateless.contains_key(&node.id);
            let has_stateful = self.stateful.contains_key(&node.id);
            log::warn!(
                "daedalus-runtime: handler run node={} stateless={} stateful={}",
                node.id,
                has_stateless,
                has_stateful
            );
        }
        if let Some(f) = self.stateless.get(&node.id) {
            let res = f(node, ctx, io);
            if std::env::var_os("DAEDALUS_TRACE_HANDLER_RUN").is_some()
                && node.id == "cv:image:to_gray"
            {
                log::warn!(
                    "daedalus-runtime: handler result node={} ok={}",
                    node.id,
                    res.is_ok()
                );
            }
            res
        } else if let Some(f) = self.stateful.get(&node.id) {
            let res = f.lock().unwrap()(node, ctx, io);
            if std::env::var_os("DAEDALUS_TRACE_HANDLER_RUN").is_some()
                && node.id == "cv:image:to_gray"
            {
                log::warn!(
                    "daedalus-runtime: handler result node={} ok={}",
                    node.id,
                    res.is_ok()
                );
            }
            res
        } else {
            if std::env::var_os("DAEDALUS_TRACE_MISSING_HANDLERS").is_some() {
                log::warn!("daedalus-runtime: missing handler for node id={}", node.id);
            }
            Ok(())
        }
    }
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

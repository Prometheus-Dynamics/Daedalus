use crate::executor::NodeError;
use daedalus_transport::Payload;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

type CapabilityFn = dyn Fn(&[&dyn Any]) -> Result<Payload, NodeError> + Send + Sync;

#[derive(Clone)]
pub struct CapabilityEntry {
    pub type_ids: Vec<TypeId>,
    pub func: Arc<CapabilityFn>,
}

impl CapabilityEntry {
    pub fn new(type_ids: Vec<TypeId>, func: Arc<CapabilityFn>) -> Self {
        Self { type_ids, func }
    }
}

#[derive(Clone, Default)]
pub struct CapabilityRegistry {
    entries: HashMap<String, Vec<CapabilityEntry>>,
}

impl CapabilityRegistry {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub fn register(
        &mut self,
        key: impl Into<String>,
        type_ids: Vec<TypeId>,
        func: Box<CapabilityFn>,
    ) {
        self.entries
            .entry(key.into())
            .or_default()
            .push(CapabilityEntry {
                type_ids,
                func: Arc::from(func),
            });
    }

    pub fn register_typed<T, F>(&mut self, key: impl Into<String>, f: F)
    where
        T: Clone + Send + Sync + 'static,
        F: Fn(&T, &T) -> Result<T, NodeError> + Send + Sync + 'static,
    {
        self.register(
            key,
            vec![TypeId::of::<T>(), TypeId::of::<T>()],
            Box::new(move |args| {
                let a = args
                    .first()
                    .and_then(|value| value.downcast_ref::<T>())
                    .ok_or_else(|| NodeError::InvalidInput("lhs".into()))?;
                let b = args
                    .get(1)
                    .and_then(|value| value.downcast_ref::<T>())
                    .ok_or_else(|| NodeError::InvalidInput("rhs".into()))?;
                f(a, b).map(|out| Payload::owned(std::any::type_name::<T>(), out))
            }),
        );
    }

    pub fn register_typed3<T, F>(&mut self, key: impl Into<String>, f: F)
    where
        T: Clone + Send + Sync + 'static,
        F: Fn(&T, &T, &T) -> Result<T, NodeError> + Send + Sync + 'static,
    {
        self.register(
            key,
            vec![TypeId::of::<T>(), TypeId::of::<T>(), TypeId::of::<T>()],
            Box::new(move |args| {
                let a = args
                    .first()
                    .and_then(|value| value.downcast_ref::<T>())
                    .ok_or_else(|| NodeError::InvalidInput("x".into()))?;
                let b = args
                    .get(1)
                    .and_then(|value| value.downcast_ref::<T>())
                    .ok_or_else(|| NodeError::InvalidInput("lo".into()))?;
                let c = args
                    .get(2)
                    .and_then(|value| value.downcast_ref::<T>())
                    .ok_or_else(|| NodeError::InvalidInput("hi".into()))?;
                f(a, b, c).map(|out| Payload::owned(std::any::type_name::<T>(), out))
            }),
        );
    }

    pub fn get(&self, key: &str) -> Option<&[CapabilityEntry]> {
        self.entries.get(key).map(Vec::as_slice)
    }

    pub fn merge(&mut self, other: CapabilityRegistry) {
        for (key, mut entries) in other.entries {
            self.entries.entry(key).or_default().append(&mut entries);
        }
    }
}

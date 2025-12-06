use crate::executor::{EdgePayload, NodeError};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

type CapabilityFn = dyn Fn(&[&dyn Any]) -> Result<EdgePayload, NodeError> + Send + Sync;

pub struct CapabilityEntry {
    pub type_ids: Vec<TypeId>,
    pub func: Box<CapabilityFn>,
}

impl CapabilityEntry {
    pub fn new(type_ids: Vec<TypeId>, func: Box<CapabilityFn>) -> Self {
        Self { type_ids, func }
    }
}

#[derive(Default)]
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
            .push(CapabilityEntry { type_ids, func });
    }

    pub fn register_typed<T, F>(&mut self, key: impl Into<String>, f: F)
    where
        T: Send + Sync + 'static,
        F: Fn(&T, &T) -> Result<T, NodeError> + Send + Sync + 'static,
    {
        let key = key.into();
        self.register(
            key,
            vec![TypeId::of::<T>(), TypeId::of::<T>()],
            Box::new(move |args: &[&dyn Any]| {
                let a = args
                    .first()
                    .and_then(|v| v.downcast_ref::<T>())
                    .ok_or_else(|| NodeError::InvalidInput("lhs".into()))?;
                let b = args
                    .get(1)
                    .and_then(|v| v.downcast_ref::<T>())
                    .ok_or_else(|| NodeError::InvalidInput("rhs".into()))?;
                f(a, b).map(|out| EdgePayload::Any(std::sync::Arc::new(out)))
            }),
        );
    }

    pub fn register_typed3<T, F>(&mut self, key: impl Into<String>, f: F)
    where
        T: Send + Sync + 'static,
        F: Fn(&T, &T, &T) -> Result<T, NodeError> + Send + Sync + 'static,
    {
        let key = key.into();
        self.register(
            key,
            vec![TypeId::of::<T>(), TypeId::of::<T>(), TypeId::of::<T>()],
            Box::new(move |args: &[&dyn Any]| {
                let a = args
                    .first()
                    .and_then(|v| v.downcast_ref::<T>())
                    .ok_or_else(|| NodeError::InvalidInput("x".into()))?;
                let b = args
                    .get(1)
                    .and_then(|v| v.downcast_ref::<T>())
                    .ok_or_else(|| NodeError::InvalidInput("lo".into()))?;
                let c = args
                    .get(2)
                    .and_then(|v| v.downcast_ref::<T>())
                    .ok_or_else(|| NodeError::InvalidInput("hi".into()))?;
                f(a, b, c).map(|out| EdgePayload::Any(std::sync::Arc::new(out)))
            }),
        );
    }

    pub fn get(&self, key: &str) -> Option<&[CapabilityEntry]> {
        self.entries.get(key).map(|v| v.as_slice())
    }

    pub fn merge(&mut self, other: CapabilityRegistry) {
        for (k, mut v) in other.entries {
            self.entries.entry(k).or_default().append(&mut v);
        }
    }

    /// Register the common arithmetic capabilities for built-in primitives.
    /// Keys are the trait names directly: "Add", "Sub", "Mul", "Div".
    pub fn register_primitive_arithmetic(&mut self) {
        macro_rules! register_math_for {
            ($ty:ty) => {
                self.register_typed::<$ty, _>("Add", |a, b| Ok(a.clone() + b.clone()));
                self.register_typed::<$ty, _>("Sub", |a, b| Ok(a.clone() - b.clone()));
                self.register_typed::<$ty, _>("Mul", |a, b| Ok(a.clone() * b.clone()));
                self.register_typed::<$ty, _>("Div", |a, b| Ok(a.clone() / b.clone()));
            };
        }
        register_math_for!(i8);
        register_math_for!(i16);
        register_math_for!(i32);
        register_math_for!(i64);
        register_math_for!(i128);
        register_math_for!(isize);
        register_math_for!(u8);
        register_math_for!(u16);
        register_math_for!(u32);
        register_math_for!(u64);
        register_math_for!(u128);
        register_math_for!(usize);
        register_math_for!(f32);
        register_math_for!(f64);
    }
}

static GLOBAL: OnceLock<RwLock<CapabilityRegistry>> = OnceLock::new();

pub fn global() -> &'static RwLock<CapabilityRegistry> {
    GLOBAL.get_or_init(|| RwLock::new(CapabilityRegistry::new()))
}

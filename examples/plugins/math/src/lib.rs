//! Math plugin for Daedalus: capability-driven numeric ops (add/sub/mul/div/rem)
//! wired through the global capability registry.

use daedalus::{PluginRegistry, declare_plugin, macros::node, runtime::NodeError};
use std::cmp::Ordering;
use std::ops::{Add, Div, Mul, Rem, Sub};

trait PowOp {
    fn pow(base: Self, exp: Self) -> Result<Self, NodeError>
    where
        Self: Sized;
}

macro_rules! impl_pow_signed {
    ($($ty:ty),+ $(,)?) => {
        $(impl PowOp for $ty {
            fn pow(base: Self, exp: Self) -> Result<Self, NodeError> {
                if exp < 0 {
                    return Err(NodeError::Handler("negative exponent not supported for Pow".into()));
                }
                Ok(base.pow(exp as u32))
            }
        })+
    };
}

macro_rules! impl_pow_unsigned {
    ($($ty:ty),+ $(,)?) => {
        $(impl PowOp for $ty {
            fn pow(base: Self, exp: Self) -> Result<Self, NodeError> {
                Ok(base.pow(exp as u32))
            }
        })+
    };
}

impl_pow_signed!(i8, i16, i32, i64, i128, isize);
impl_pow_unsigned!(u8, u16, u32, u64, u128, usize);

impl PowOp for f32 {
    fn pow(base: Self, exp: Self) -> Result<Self, NodeError> {
        Ok(base.powf(exp))
    }
}

impl PowOp for f64 {
    fn pow(base: Self, exp: Self) -> Result<Self, NodeError> {
        Ok(base.powf(exp))
    }
}

trait ClampOp {
    fn clamp(x: Self, lo: Self, hi: Self) -> Result<Self, NodeError>
    where
        Self: Sized;
}

macro_rules! impl_clamp_ord {
    ($($ty:ty),+ $(,)?) => {
        $(impl ClampOp for $ty {
            fn clamp(x: Self, lo: Self, hi: Self) -> Result<Self, NodeError> {
                if lo > hi {
                    return Err(NodeError::Handler("clamp min > max".into()));
                }
                Ok(x.clamp(lo, hi))
            }
        })+
    };
}

macro_rules! impl_clamp_float {
    ($($ty:ty),+ $(,)?) => {
        $(impl ClampOp for $ty {
            fn clamp(x: Self, lo: Self, hi: Self) -> Result<Self, NodeError> {
                match lo.partial_cmp(&hi) {
                    Some(Ordering::Greater) => {
                        return Err(NodeError::Handler("clamp min > max".into()));
                    }
                    None => {
                        return Err(NodeError::Handler("unordered clamp bounds".into()));
                    }
                    _ => {}
                }
                Ok(x.clamp(lo, hi))
            }
        })+
    };
}

impl_clamp_ord!(
    i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize
);
impl_clamp_float!(f32, f64);

// Generic binary math nodes dispatch through capability registry entries.
#[node(id = "add", capability = "Add", inputs("a", "b"), outputs("out"))]
fn add<T>(a: T, b: T) -> Result<T, NodeError>
where
    T: Add<Output = T> + Clone + Send + Sync + 'static,
{
    Ok(a + b)
}

#[node(id = "sub", capability = "Sub", inputs("a", "b"), outputs("out"))]
fn sub<T>(a: T, b: T) -> Result<T, NodeError>
where
    T: Add<Output = T> + Sub<Output = T> + Clone + Send + Sync + 'static,
{
    Ok(a - b)
}

#[node(id = "mul", capability = "Mul", inputs("a", "b"), outputs("out"))]
fn mul<T>(a: T, b: T) -> Result<T, NodeError>
where
    T: Add<Output = T> + Mul<Output = T> + Clone + Send + Sync + 'static,
{
    Ok(a * b)
}

#[node(id = "div", capability = "Div", inputs("a", "b"), outputs("out"))]
fn div<T>(a: T, b: T) -> Result<T, NodeError>
where
    T: Add<Output = T> + Div<Output = T> + Clone + Send + Sync + 'static,
{
    Ok(a / b)
}

#[node(id = "rem", capability = "Rem", inputs("a", "b"), outputs("out"))]
fn rem<T>(a: T, b: T) -> Result<T, NodeError>
where
    T: Add<Output = T> + Rem<Output = T> + Clone + Send + Sync + 'static,
{
    Ok(a % b)
}

#[node(id = "max", capability = "Max", inputs("a", "b"), outputs("out"))]
fn max<T>(a: T, b: T) -> Result<T, NodeError>
where
    T: PartialOrd + Clone + Send + Sync + 'static,
{
    match a.partial_cmp(&b) {
        Some(Ordering::Greater) | Some(Ordering::Equal) => Ok(a),
        Some(Ordering::Less) => Ok(b),
        None => Err(NodeError::Handler("unordered operands for max".into())),
    }
}

#[node(id = "min", capability = "Min", inputs("a", "b"), outputs("out"))]
fn min<T>(a: T, b: T) -> Result<T, NodeError>
where
    T: PartialOrd + Clone + Send + Sync + 'static,
{
    match a.partial_cmp(&b) {
        Some(Ordering::Less) | Some(Ordering::Equal) => Ok(a),
        Some(Ordering::Greater) => Ok(b),
        None => Err(NodeError::Handler("unordered operands for min".into())),
    }
}

#[node(
    id = "clamp",
    capability = "Clamp",
    inputs("x", "lo", "hi"),
    outputs("out")
)]
fn clamp<T>(x: T, lo: T, hi: T) -> Result<T, NodeError>
where
    T: ClampOp + Clone + Send + Sync + 'static,
{
    ClampOp::clamp(x, lo, hi)
}

#[node(id = "pow", capability = "Pow", inputs("base", "exp"), outputs("out"))]
fn pow<T>(base: T, exp: T) -> Result<T, NodeError>
where
    T: PowOp + Clone + Send + Sync + 'static,
{
    T::pow(base, exp)
}

fn register_all_capabilities(registry: &mut PluginRegistry) {
    fn register_for_arithmetic<T>(registry: &mut PluginRegistry)
    where
        T: Add<Output = T>
            + Sub<Output = T>
            + Mul<Output = T>
            + Div<Output = T>
            + Rem<Output = T>
            + Clone
            + Send
            + Sync
            + 'static,
    {
        registry.register_capability_typed::<T, _>("Add", |a, b| Ok(a.clone() + b.clone()));
        registry.register_capability_typed::<T, _>("Sub", |a, b| Ok(a.clone() - b.clone()));
        registry.register_capability_typed::<T, _>("Mul", |a, b| Ok(a.clone() * b.clone()));
        registry.register_capability_typed::<T, _>("Div", |a, b| Ok(a.clone() / b.clone()));
        registry.register_capability_typed::<T, _>("Rem", |a, b| Ok(a.clone() % b.clone()));
    }

    fn register_for_ordered<T>(registry: &mut PluginRegistry)
    where
        T: PartialOrd + Clone + Send + Sync + 'static,
    {
        registry.register_capability_typed::<T, _>("Max", |a, b| match a.partial_cmp(b) {
            Some(Ordering::Greater) | Some(Ordering::Equal) => Ok(a.clone()),
            Some(Ordering::Less) => Ok(b.clone()),
            None => Err(NodeError::Handler("unordered operands for max".into())),
        });
        registry.register_capability_typed::<T, _>("Min", |a, b| match a.partial_cmp(b) {
            Some(Ordering::Less) | Some(Ordering::Equal) => Ok(a.clone()),
            Some(Ordering::Greater) => Ok(b.clone()),
            None => Err(NodeError::Handler("unordered operands for min".into())),
        });
    }

    fn register_for_pow<T>(registry: &mut PluginRegistry)
    where
        T: PowOp + Clone + Send + Sync + 'static,
    {
        registry.register_capability_typed::<T, _>("Pow", |a, b| PowOp::pow(a.clone(), b.clone()));
    }

    fn register_for_clamp<T>(registry: &mut PluginRegistry)
    where
        T: ClampOp + Clone + Send + Sync + 'static,
    {
        registry.register_capability_typed3::<T, _>("Clamp", |x, lo, hi| {
            ClampOp::clamp(x.clone(), lo.clone(), hi.clone())
        });
    }

    macro_rules! register_primitives {
        ($($ty:ty),+ $(,)?) => {
            $(
                register_for_arithmetic::<$ty>(registry);
                register_for_ordered::<$ty>(registry);
                register_for_pow::<$ty>(registry);
                register_for_clamp::<$ty>(registry);
            )+
        };
    }

    register_primitives!(
        i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize, f32, f64
    );
}

declare_plugin!(
    MathPlugin,
    "math",
    [add, sub, mul, div, rem, max, min, pow, clamp],
    install = |registry| {
        register_all_capabilities(registry);
    }
);

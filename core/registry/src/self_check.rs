//! A config checking itself: that the value its Rust `Default` makes is one its own declaration
//! allows. A config says what a field defaults to in two places, `#[port(default = ..)]` and its
//! `Default`, and its limits in a third, and nothing else compares them.
//!
//! `#[derive(NodeConfig)]` writes `self_check` for the config with what is here. The pieces are
//! chosen by what a field's type can do, with no bound on it: a field that isn't a number has no
//! limit to check, and one that can't be compared or printed is passed over.

use core::fmt::{Arguments, Debug};

use daedalus_types::Literal;

use crate::record::PortRule;

/// Where a config's faults go: the field (empty for the config as a whole), and what is wrong.
pub type Report<'a> = &'a mut dyn FnMut(&'static str, Arguments<'_>);

/// A config with nothing to check, for a declaration written by hand.
pub fn nothing(_: Report<'_>) {}

/// One field's value, for the checks its type allows.
pub struct Probe<'a, T>(pub &'a T);

/// A field's declared default and the value `Default` gives it.
pub struct Pair<'a, T>(pub &'a T, pub &'a T);

/// A type that may have a `Default`.
pub struct Made<T>(core::marker::PhantomData<T>);

impl<T> Made<T> {
    pub const fn new() -> Made<T> {
        Made(core::marker::PhantomData)
    }
}

impl<T> Default for Made<T> {
    fn default() -> Made<T> {
        Made::new()
    }
}

/// The type's default.
pub trait MakeDefault<T> {
    fn make(&self) -> Option<T>;
}

impl<T: Default> MakeDefault<T> for Made<T> {
    fn make(&self) -> Option<T> {
        Some(T::default())
    }
}

/// A type with no `Default`.
pub trait MakeNothing<T> {
    fn make(&self) -> Option<T>;
}

impl<T> MakeNothing<T> for &Made<T> {
    fn make(&self) -> Option<T> {
        None
    }
}

/// A numeric field's value, and whether it is a whole number that is even.
pub trait Number {
    fn number(&self) -> Option<(f64, bool)>;
}

macro_rules! whole {
    ($($ty:ty)*) => {$(
        impl Number for Probe<'_, $ty> {
            fn number(&self) -> Option<(f64, bool)> {
                Some((*self.0 as f64, *self.0 % 2 == 0))
            }
        }
    )*};
}

whole!(u8 u16 u32 u64 usize i8 i16 i32 i64 isize);

impl Number for Probe<'_, f32> {
    fn number(&self) -> Option<(f64, bool)> {
        Some((f64::from(*self.0), false))
    }
}

impl Number for Probe<'_, f64> {
    fn number(&self) -> Option<(f64, bool)> {
        Some((*self.0, false))
    }
}

/// A field that is not a number.
pub trait NotANumber {
    fn number(&self) -> Option<(f64, bool)>;
}

impl<T> NotANumber for &Probe<'_, T> {
    fn number(&self) -> Option<(f64, bool)> {
        None
    }
}

/// Reports a declared default that is not the value `Default` gives.
pub trait Same {
    fn same(&self, field: &'static str, report: Report<'_>);
}

impl<T: PartialEq + Debug> Same for Pair<'_, T> {
    fn same(&self, field: &'static str, report: Report<'_>) {
        if self.0 != self.1 {
            report(
                field,
                format_args!(
                    "declares the default {:?}, but `Default` gives {:?}",
                    self.0, self.1
                ),
            );
        }
    }
}

/// A field whose values can't be compared or printed.
pub trait Incomparable {
    fn same(&self, field: &'static str, report: Report<'_>);
}

impl<T> Incomparable for &Pair<'_, T> {
    fn same(&self, _: &'static str, _: Report<'_>) {}
}

fn limit(limit: &Option<Literal>) -> Option<f64> {
    match limit {
        Some(Literal::Int(value)) => Some(*value as f64),
        Some(Literal::Float(value)) => Some(*value),
        _ => None,
    }
}

/// Reports a number that breaks `rule`: below its minimum, above its maximum, or even where it
/// must be odd. The step is a hint and breaks nothing.
pub fn rule(field: &'static str, rule: &PortRule, (value, even): (f64, bool), report: Report<'_>) {
    if let Some(min) = limit(&rule.min).filter(|min| value < *min) {
        report(
            field,
            format_args!("`Default` gives {value}, which is below its minimum {min}"),
        );
    } else if let Some(max) = limit(&rule.max).filter(|max| value > *max) {
        report(
            field,
            format_args!("`Default` gives {value}, which is above its maximum {max}"),
        );
    } else if rule.odd && even {
        report(
            field,
            format_args!("`Default` gives {value}, which is not odd"),
        );
    }
}

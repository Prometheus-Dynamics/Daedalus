//! Naming a port, and the Rust types that go in and come out of one.

use daedalus_capi_sys::{Api, At, Code, Handle};

pub use daedalus_capi_sys::Pushed;

/// A host port's index in its graph, looked up once so later calls skip the name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PortIndex(pub usize);

/// A way to name a port: its name, or its index.
pub trait ToPort {
    #[doc(hidden)]
    fn at(&self) -> At<'_>;
}

impl ToPort for &str {
    fn at(&self) -> At<'_> {
        At::Name(self)
    }
}

impl ToPort for PortIndex {
    fn at(&self) -> At<'_> {
        At::Index(self.0)
    }
}

/// A Rust type that is pushed to and taken from a port. An integer type goes through the
/// surface's 64-bit integer, a float through its 64-bit float, and the library refuses a
/// number the port's own type can't hold.
pub trait PortValue: Sized {
    #[doc(hidden)]
    fn push(self, api: &Api, graph: Handle, port: At<'_>) -> Result<(), Code>;
    #[doc(hidden)]
    fn take(api: &Api, graph: Handle, port: At<'_>) -> Result<Option<Self>, Code>;
}

/// The status for a number that doesn't fit, as the library numbers it.
const OUT_OF_RANGE: Code = 10;

macro_rules! integers {
    ($($ty:ty),*) => {$(
        impl PortValue for $ty {
            fn push(self, api: &Api, graph: Handle, port: At<'_>) -> Result<(), Code> {
                let wide = i64::try_from(self).map_err(|_| OUT_OF_RANGE)?;
                api.push_i64(graph, port, wide)
            }

            fn take(api: &Api, graph: Handle, port: At<'_>) -> Result<Option<Self>, Code> {
                match api.take_i64(graph, port)? {
                    Some(wide) => <$ty>::try_from(wide).map(Some).map_err(|_| OUT_OF_RANGE),
                    None => Ok(None),
                }
            }
        }
    )*};
}

integers!(i8, i16, i32, i64, u8, u16, u32, u64);

impl PortValue for f64 {
    fn push(self, api: &Api, graph: Handle, port: At<'_>) -> Result<(), Code> {
        api.push_f64(graph, port, self)
    }

    fn take(api: &Api, graph: Handle, port: At<'_>) -> Result<Option<Self>, Code> {
        api.take_f64(graph, port)
    }
}

impl PortValue for f32 {
    fn push(self, api: &Api, graph: Handle, port: At<'_>) -> Result<(), Code> {
        api.push_f64(graph, port, f64::from(self))
    }

    fn take(api: &Api, graph: Handle, port: At<'_>) -> Result<Option<Self>, Code> {
        // Narrowing is what the caller asked for by taking an `f32`.
        Ok(api.take_f64(graph, port)?.map(|wide| wide as f32))
    }
}

impl PortValue for bool {
    fn push(self, api: &Api, graph: Handle, port: At<'_>) -> Result<(), Code> {
        api.push_bool(graph, port, self)
    }

    fn take(api: &Api, graph: Handle, port: At<'_>) -> Result<Option<Self>, Code> {
        api.take_bool(graph, port)
    }
}

impl PortValue for Vec<u8> {
    fn push(self, api: &Api, graph: Handle, port: At<'_>) -> Result<(), Code> {
        api.push_bytes(graph, port, &self)
    }

    fn take(api: &Api, graph: Handle, port: At<'_>) -> Result<Option<Self>, Code> {
        api.take_bytes(graph, port)
    }
}

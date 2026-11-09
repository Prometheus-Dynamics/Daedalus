//! The smallest authoring crate: a body macro with no runtime. [`sample`] expands `#[node]` with
//! no default features, so this crate building for a bare-metal target proves the expansion needs
//! no allocator.

#![no_std]
#![forbid(unsafe_code)]

pub use daedalus_registry as registry;
pub use daedalus_types as types;

#[doc(hidden)]
pub use daedalus_derive as derive;

/// A fan-in parameter. The macro recognizes it by name.
pub struct FanIn<T>(pub T);

/// The contract every authoring crate implements: it expands to a const-evaluable `BodyRecord`.
/// `args` is the backend's own, here the port types of a body held in a const.
#[macro_export]
macro_rules! __daedalus_body {
    (
        kind = const,
        name = ($($name:tt)*),
        args = ($($port:ident : $ty:ty),* $(,)?),
        params = (),
        ret = (),
        decl = $decl:ident
    ) => {
        $crate::registry::BodyRecord {
            backend: "fixture",
            at: $crate::registry::SourceAt(::core::concat!(::core::file!(), ":", ::core::line!())),
            needs: &[],
            state: ::core::option::Option::None,
            ports: &[$((
                ::core::stringify!($port),
                $crate::registry::TypeRef::Key(<$ty as $crate::types::Typed>::KEY),
            )),*],
            memory: &[],
            code: &$crate::registry::NoCode,
            text: $($name)*,
        }
    };
    (
        kind = transfer,
        name = ($($name:tt)*),
        args = ($($args:tt)*),
        params = ($($params:tt)*),
        ret = $ret:ty,
        decl = $decl:ident
    ) => {
        $crate::registry::BodyRecord {
            backend: "fixture",
            at: $crate::registry::SourceAt(::core::concat!(::core::file!(), ":", ::core::line!())),
            needs: &[],
            state: ::core::option::Option::None,
            ports: &[],
            memory: &[],
            code: &$crate::registry::NoCode,
            text: "",
        }
    };
    (
        kind = fn,
        name = ($($name:tt)*),
        args = ($($args:tt)*),
        params = ($($params:tt)*),
        ret = $ret:ty,
        decl = $decl:ident
    ) => {
        $crate::registry::BodyRecord {
            backend: "fixture",
            at: $crate::registry::SourceAt(::core::concat!(::core::file!(), ":", ::core::line!())),
            needs: &[$($args)*],
            state: ::core::option::Option::None,
            ports: &[],
            memory: &[],
            code: &$crate::registry::NoCode,
            text: "",
        }
    };
}

/// Nodes written the way an author would, built with the crate.
pub mod sample {
    use daedalus_macros::node;
    use daedalus_registry::NodeRecord;

    #[node(id = "fixture.add", summary = "Adds.", inputs(a, b), outputs(sum), backend = crate)]
    pub fn add(a: u32, b: u32) -> u32 {
        a + b
    }

    #[node(id = "fixture.pick", backend = crate, generics(T, instance(u32, "u32"), instance(u8, "u8")))]
    pub fn pick<T: Copy>(value: &T) -> T {
        *value
    }

    pub const SOURCE: &str = "source text";

    #[daedalus_macros::transfer(from = "fixture.a", to = "fixture.b", cost = copy, backend = crate)]
    pub fn widen(value: &u8) -> u32 {
        *value as u32
    }

    /// A default key, built for bare metal and used from another crate by the derive tests.
    #[derive(daedalus_derive::Type)]
    pub struct Sample {
        pub count: u32,
        pub flag: bool,
    }

    pub const NODES: &[&NodeRecord] = &[&ADD_NODE, &PICK_U32_NODE, &PICK_U8_NODE];
}

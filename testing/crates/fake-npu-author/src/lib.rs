//! The authoring crate of a backend Daedalus has never heard of: a software model of an NPU.
//! It is the only part a node's author depends on. `#[node]` knows nothing of it: the macro
//! expands to a call of this crate's body macro, at the path the node names.
//!
//! ```ignore
//! #[node(
//!     id = "vision.double",
//!     inputs(x),
//!     outputs(y),
//!     backend = daedalus_fake_npu_author(model = "scale2", x: u32, y: u32),
//! )]
//! const DOUBLE: &str = "scale2";
//! ```
//!
//! A model is held in a const, so the backend's arguments give each port its type, as they do
//! for any body with no Rust signature.

#![no_std]
#![forbid(unsafe_code)]

#[doc(hidden)]
pub use daedalus_registry as __registry;
#[doc(hidden)]
pub use daedalus_types as __types;

/// The id of the backend these bodies run on.
pub const BACKEND_ID: &str = "fake-npu";

/// What `#[node]` calls for a body on this backend. Not called by hand. A model is a const, so
/// a function body is refused with a message that says so.
#[macro_export]
macro_rules! __daedalus_body {
    (
        kind = const,
        name = ($($name:tt)*),
        args = (model = $model:literal $(, $port:ident : $ty:ty)* $(,)?),
        params = (),
        ret = (),
        bind = $bind:tt,
        decl = $decl:ident,
        init = ($($init:tt)*)
    ) => {
        $crate::__registry::BodyRecord {
            backend: $crate::BACKEND_ID,
            at: $crate::__registry::SourceAt(::core::concat!(::core::file!(), ":", ::core::line!())),
            needs: &[],
            state: ::core::option::Option::None,
            ports: &[$((
                ::core::stringify!($port),
                $crate::__registry::TypeRef::Key(<$ty as $crate::__types::Typed>::KEY),
            )),*],
            memory: &[],
            code: &$crate::__registry::NoCode,
            // The model the backend runs. The const itself only has to be text.
            text: {
                const _: &str = $($name)*;
                $model
            },
        }
    };
    (kind = const, $($rest:tt)*) => {
        ::core::compile_error!(
            "a fake NPU node is `backend = daedalus_fake_npu_author(model = \"<name>\", <port>: <Type>, ..)`"
        )
    };
    (kind = $other:ident, $($rest:tt)*) => {
        ::core::compile_error!("a fake NPU node is a const naming a model, not a function")
    };
}

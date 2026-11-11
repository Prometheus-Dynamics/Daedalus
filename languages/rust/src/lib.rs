//! The Rust worker SDK. A node is an ordinary function declared with `#[node]`, with
//! `backend = daedalus_lang_rust`. [`serve`] runs the worker loop on stdin and stdout: it builds the
//! hello from the nodes' registry records, runs each node under a panic guard, and answers the wire
//! protocol. Output from node bodies goes through [`Context::log`], never to stdout.

#![forbid(unsafe_code)]

pub mod codec;
pub mod context;
mod serve;

pub use codec::{Pack, Pair, Wire};
pub use context::{Context, Event};
/// The registry records `#[node]` writes into.
pub use daedalus_registry as registry;
pub use daedalus_wire::WireValue;
pub use serve::{NodeError, NodeFn, RustBody, serve, state_type};

/// The body macro `#[node]` calls for a function of this crate. A Rust node's body is a function
/// that takes its inputs by name and returns its outputs.
#[macro_export]
macro_rules! __daedalus_body {
    (
        kind = fn,
        name = ($($name:tt)*),
        args = ($($args:tt)*),
        params = ($($pname:ident : $pty:ty,)*),
        ret = $ret:ty,
        bind = $bind:tt,
        decl = $decl:ident
    ) => {
        $crate::registry::BodyRecord {
            backend: "rust",
            at: $crate::registry::SourceAt(::core::concat!(::core::file!(), ":", ::core::line!())),
            needs: &[],
            state: ::core::option::Option::None,
            ports: &[],
            memory: &[],
            code: &{
                fn __adapter(
                    inputs: &[(&'static str, $crate::WireValue)],
                    _cx: &mut $crate::Context,
                ) -> ::core::result::Result<::std::vec::Vec<$crate::WireValue>, $crate::NodeError> {
                    $(
                        let $pname: $pty = $crate::codec::input(inputs, ::core::stringify!($pname))?;
                    )*
                    let out: $ret = ($($name)*)($($pname),*);
                    ::core::result::Result::Ok(<$ret as $crate::Pack>::pack(out))
                }
                $crate::RustBody {
                    run: __adapter,
                    state: ::core::option::Option::None,
                }
            },
            text: "",
        }
    };
}

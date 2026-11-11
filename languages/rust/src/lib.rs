//! The Rust worker SDK. A node is an ordinary function declared with `#[node]`, with
//! `backend = daedalus_lang_rust`. [`serve`] runs the worker loop on stdin and stdout: it builds the
//! hello from the nodes' registry records, runs each node under a panic guard, and answers the wire
//! protocol. Output from node bodies goes through [`Context::log`], never to stdout.

#![forbid(unsafe_code)]

pub mod codec;
pub mod context;
mod serve;

pub use codec::{Pack, Pair, Wire, WorkerReturn};
pub use context::{Context, Event};
/// The registry records `#[node]` writes into.
pub use daedalus_registry as registry;
pub use daedalus_wire::WireValue;
pub use serve::{NodeError, NodeFn, RustBody, serve, state_type};

/// The context a node body takes as `&Ctx`: the tick's cancel flag, and progress and log events.
pub type Ctx = Context;

/// The error a node body returns in a `Result`: a fault code and a reason. The same fields as the
/// in-process `Fault`, with the worker's own code type.
pub type Fault = NodeError;

/// The body macro `#[node]` calls for a function of this crate. The `bind` roles say how each
/// parameter is read: an input is decoded from its port, `&T` and `&mut T` are borrowed from a value
/// read for the call, the state is read before the call and stored after it, and `&Ctx` is the
/// context. The return is a value, or a `Result` whose error is the node's fault.
#[macro_export]
macro_rules! __daedalus_body {
    (
        kind = fn,
        name = ($($name:tt)*),
        args = ($($backend_args:tt)*),
        params = ($($params:tt)*),
        ret = $ret:ty,
        bind = (
            state = ($($state:tt)*),
            config = ($($config:tt)*),
            fixed = $fixed:tt,
            params = ($($entries:tt)*),
            returns = ($($returns:tt)*)
        ),
        decl = $decl:ident
    ) => {
        $crate::registry::BodyRecord {
            // A worker's node is served by the process backend, which is the backend the planner places it on.
            backend: "process",
            at: $crate::registry::SourceAt(::core::concat!(::core::file!(), ":", ::core::line!())),
            needs: &[],
            state: ::core::option::Option::None,
            ports: &[],
            memory: &[],
            code: &{
                fn __adapter(
                    inputs: &[(&'static str, $crate::WireValue)],
                    cx: &mut $crate::Context,
                ) -> ::core::result::Result<::std::vec::Vec<$crate::WireValue>, $crate::NodeError> {
                    $crate::__worker_call!(
                        inputs = inputs,
                        cx = cx,
                        ret = $ret,
                        name = ($($name)*),
                        pre = [],
                        args = [],
                        post = [],
                        entries = ($($entries)*)
                    )
                }
                $crate::RustBody {
                    run: __adapter,
                    state: $crate::__state_fn!($($state)*),
                }
            },
            text: "",
        }
    };
}

/// The state type's wire type, when the node names one. It must be `Typed`, since the host declares
/// it in the hello.
#[doc(hidden)]
#[macro_export]
macro_rules! __state_fn {
    () => {
        ::core::option::Option::None
    };
    ($state:ty) => {
        ::core::option::Option::Some($crate::state_type::<$state>)
    };
}

/// Walks a node's parameters in order, and builds its call: each parameter adds its setup, its
/// argument, and, for the state, the store after the call.
#[doc(hidden)]
#[macro_export]
macro_rules! __worker_call {
    (
        inputs = $inputs:ident,
        cx = $cx:ident,
        ret = $ret:ty,
        name = ($($name:tt)*),
        pre = [$($pre:tt)*],
        args = [$($args:tt)*],
        post = [$($post:tt)*],
        entries = ()
    ) => {{
        $($pre)*
        let __out = ($($name)*)($($args)*);
        $($post)*
        <$ret as $crate::WorkerReturn>::into_outputs(__out)
    }};
    (
        inputs = $inputs:ident,
        cx = $cx:ident,
        ret = $ret:ty,
        name = ($($name:tt)*),
        pre = [$($pre:tt)*],
        args = [$($args:tt)*],
        post = [$($post:tt)*],
        entries = (state $index:literal $port:literal $id:ident : $t:ty , $($rest:tt)*)
    ) => {
        $crate::__worker_call!(
            inputs = $inputs,
            cx = $cx,
            ret = $ret,
            name = ($($name)*),
            pre = [$($pre)* let mut $id: $t = $crate::codec::state_value::<$t>($cx);],
            args = [$($args)* &mut $id,],
            post = [$($post)* $crate::codec::store_state($cx, &$id);],
            entries = ($($rest)*)
        )
    };
    (
        inputs = $inputs:ident,
        cx = $cx:ident,
        ret = $ret:ty,
        name = ($($name:tt)*),
        pre = [$($pre:tt)*],
        args = [$($args:tt)*],
        post = [$($post:tt)*],
        entries = (take $index:literal $port:literal $id:ident : $t:ty , $($rest:tt)*)
    ) => {
        $crate::__worker_call!(
            inputs = $inputs,
            cx = $cx,
            ret = $ret,
            name = ($($name)*),
            pre = [$($pre)* let $id: $t = $crate::codec::input($inputs, $port)?;],
            args = [$($args)* $id,],
            post = [$($post)*],
            entries = ($($rest)*)
        )
    };
    (
        inputs = $inputs:ident,
        cx = $cx:ident,
        ret = $ret:ty,
        name = ($($name:tt)*),
        pre = [$($pre:tt)*],
        args = [$($args:tt)*],
        post = [$($post:tt)*],
        entries = (read $index:literal $port:literal $id:ident : $t:ty , $($rest:tt)*)
    ) => {
        $crate::__worker_call!(
            inputs = $inputs,
            cx = $cx,
            ret = $ret,
            name = ($($name)*),
            pre = [$($pre)* let $id: $t = $crate::codec::input($inputs, $port)?;],
            args = [$($args)* &$id,],
            post = [$($post)*],
            entries = ($($rest)*)
        )
    };
    (
        inputs = $inputs:ident,
        cx = $cx:ident,
        ret = $ret:ty,
        name = ($($name:tt)*),
        pre = [$($pre:tt)*],
        args = [$($args:tt)*],
        post = [$($post:tt)*],
        entries = (modify $index:literal $port:literal $id:ident : $t:ty , $($rest:tt)*)
    ) => {
        $crate::__worker_call!(
            inputs = $inputs,
            cx = $cx,
            ret = $ret,
            name = ($($name)*),
            pre = [$($pre)* let mut $id: $t = $crate::codec::input($inputs, $port)?;],
            args = [$($args)* &mut $id,],
            post = [$($post)*],
            entries = ($($rest)*)
        )
    };
    (
        inputs = $inputs:ident,
        cx = $cx:ident,
        ret = $ret:ty,
        name = ($($name:tt)*),
        pre = [$($pre:tt)*],
        args = [$($args:tt)*],
        post = [$($post:tt)*],
        entries = (opt_take $index:literal $port:literal $id:ident : $t:ty , $($rest:tt)*)
    ) => {
        $crate::__worker_call!(
            inputs = $inputs,
            cx = $cx,
            ret = $ret,
            name = ($($name)*),
            pre = [$($pre)* let $id: ::core::option::Option<$t> = $crate::codec::optional_input($inputs, $port)?;],
            args = [$($args)* $id,],
            post = [$($post)*],
            entries = ($($rest)*)
        )
    };
    (
        inputs = $inputs:ident,
        cx = $cx:ident,
        ret = $ret:ty,
        name = ($($name:tt)*),
        pre = [$($pre:tt)*],
        args = [$($args:tt)*],
        post = [$($post:tt)*],
        entries = (opt_read $index:literal $port:literal $id:ident : $t:ty , $($rest:tt)*)
    ) => {
        $crate::__worker_call!(
            inputs = $inputs,
            cx = $cx,
            ret = $ret,
            name = ($($name)*),
            pre = [$($pre)* let $id: ::core::option::Option<$t> = $crate::codec::optional_input($inputs, $port)?;],
            args = [$($args)* $id.as_ref(),],
            post = [$($post)*],
            entries = ($($rest)*)
        )
    };
    (
        inputs = $inputs:ident,
        cx = $cx:ident,
        ret = $ret:ty,
        name = ($($name:tt)*),
        pre = [$($pre:tt)*],
        args = [$($args:tt)*],
        post = [$($post:tt)*],
        entries = (ctx $index:literal $port:literal $id:ident : $t:ty , $($rest:tt)*)
    ) => {
        $crate::__worker_call!(
            inputs = $inputs,
            cx = $cx,
            ret = $ret,
            name = ($($name)*),
            pre = [$($pre)*],
            args = [$($args)* &*$cx,],
            post = [$($post)*],
            entries = ($($rest)*)
        )
    };
    (
        inputs = $inputs:ident,
        cx = $cx:ident,
        ret = $ret:ty,
        name = ($($name:tt)*),
        pre = [$($pre:tt)*],
        args = [$($args:tt)*],
        post = [$($post:tt)*],
        entries = ($($rest:tt)+)
    ) => {
        ::core::compile_error!(
            "the Rust worker's #[node] does not take this kind of parameter yet"
        )
    };
}

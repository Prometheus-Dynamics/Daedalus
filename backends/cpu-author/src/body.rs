//! The body expansion. `#[node]` hands it each parameter's role and position, so a wrapper is a
//! flat list of bindings and a call.

/// Expands a function into a `BodyRecord` whose payload builds its step.
#[macro_export]
#[doc(hidden)]
macro_rules! __daedalus_body {
    (kind = const, $($rest:tt)*) => {
        ::core::compile_error!("a CPU body is a function")
    };
    (
        kind = $kind:tt,
        name = ($($name:tt)*),
        args = ($($needs:expr),* $(,)?),
        params = ($($params:tt)*),
        ret = $ret:ty,
        bind = (
            state = ($($state:ty)?),
            config = ($($config:ty)?),
            fixed = $fixed:literal,
            params = ($($role:ident $at:literal $port:literal $arg:ident : $ty:ty,)*),
            returns = (
                $flow:ident,
                $pattern:tt,
                [$($okind:ident $oat:tt $oname:ident : $oty:ty,)*]
            )
        ),
        decl = $decl:ident
    ) => {{
        const __OUTPUTS: usize = [$($oat),*].len();
        // Whether each output is written inline, which keeps nothing between ticks.
        const __WRITE: [bool; __OUTPUTS] = [$({
            const __KIND: $crate::__cpu::Kind<$oty> = $crate::__kind!($oty);
            __KIND.write.is_some()
        }),*];
        const _: () = ::core::assert!(
            $crate::__cpu::total(&[$($crate::__is_fanin!($role)),*]) < 2,
            "a CPU node takes at most one FanIn"
        );

        struct __Run<__K> {
            node: u32,
            inputs: usize,
            state: $crate::__or_unit!($($state)?),
            config: $crate::__or_unit!($($config)?),
            consts: __K,
            outs: [[u8; 16]; __OUTPUTS],
            sharing: [$crate::__cpu::Keep; __OUTPUTS],
            keep: ($($crate::__cpu::Keeping<$oty, { __WRITE[$oat] }>,)*),
        }

        #[allow(unused_mut, unused_variables, clippy::all)]
        impl<__K: $crate::__cpu::ConstTable + ::core::marker::Send + 'static> $crate::__cpu::OwnedStep
            for __Run<__K>
        {
            #[inline]
            fn run(
                &mut self,
                __slots: &mut $crate::__cpu::Slots,
                __cx: &mut $crate::__cpu::StepCx,
            ) -> $crate::__cpu::StepEnd {
                let __this = self;
                let __cells = __slots.as_mut_slice();
                let (__in, __out) = __cells.split_at_mut(__this.inputs);
                let (__fixed, __fan) = __in.split_at_mut($fixed);
                $($crate::__check!($role $at; __fixed, __this);)*
                if __cx.restarting() {
                    __this.state = ::core::default::Default::default();
                }
                let mut __next = __fixed.iter_mut();
                $($crate::__bind!($role $at $arg: $ty; __next, __this, __cx, __fan);)*
                let __ret = ($($name)*)($($arg),*);
                let $pattern = $crate::__unfail!($flow; __ret, __this, __cx);
                $($crate::__emit!($okind $oat $oname: $oty; __out, __this);)*
                $crate::__cpu::StepEnd::Done
            }
        }

        $crate::__cpu::__if_alloc! {
        fn __make(
            __setup: &$crate::__cpu::Setup<'_>,
        ) -> ::core::result::Result<$crate::__cpu::Step, $crate::__cpu::StepString> {
            let mut __consts = $crate::__cpu::consts_for(__setup, $fixed);
            $($crate::__const!($role $at $port: $ty; __setup, __consts);)*
            ::core::result::Result::Ok($crate::__cpu::Step::boxed(__Run {
                node: __setup.node.0,
                inputs: __setup.inputs as usize,
                state: ::core::default::Default::default(),
                config: $crate::__config!(__setup; $($config)?),
                consts: __consts,
                outs: [[0; 16]; __OUTPUTS],
                sharing: $crate::__cpu::keep_array::<__OUTPUTS>(__setup.keep),
                keep: ($($crate::__emit_default!($oty),)*),
            }))
        }
        }

        $crate::__registry::BodyRecord {
            backend: "cpu",
            at: $crate::__registry::SourceAt(::core::concat!(
                ::core::file!(),
                ":",
                ::core::line!()
            )),
            needs: &[$($needs),*],
            state: ::core::option::Option::None,
            ports: &[],
            memory: &[],
            code: &$crate::__cpu::__cpu_body!(
                __make,
                $crate::__cpu::BodyShape {
                    path: ::core::concat!(::core::module_path!(), "::", ::core::stringify!($($name)*)),
                    params: &[$($crate::__cpu::ParamShape {
                        role: $crate::__role!($role),
                        at: $at,
                        port: $port,
                        ty: ::core::any::type_name::<$ty>,
                        copy: $crate::__is_copy!($ty),
                    },)*],
                    outputs: &[$($crate::__cpu::OutputShape {
                        optional: $crate::__optional!($okind),
                        ty: ::core::any::type_name::<$oty>,
                        copy: $crate::__is_copy!($oty),
                    },)*],
                    fallible: $crate::__fallible!($flow),
                },
                $crate::__arena!(
                    config = ($($config)?),
                    fixed = $fixed,
                    params = ($($role $at: $ty,)*),
                    outputs = ($($oat: $oty,)*)
                )
            ),
            text: "",
        }
    }};
}

/// The body as a step that can live in a fixed block of memory, if it can: `__Run` from the calling
/// expansion, with its constants left to the runner.
#[macro_export]
#[doc(hidden)]
macro_rules! __arena {
    (config = ($config:ty), $($rest:tt)*) => {
        ::core::option::Option::None
    };
    (
        config = (),
        fixed = $fixed:literal,
        params = ($($role:ident $at:literal: $ty:ty,)*),
        outputs = ($($oat:literal: $oty:ty,)*)
    ) => {{
        impl $crate::__cpu::Fresh for __Run<$crate::__cpu::NoConsts> {
            fn fresh(node: u32, inputs: u32) -> Self {
                __Run {
                    node,
                    inputs: inputs as usize,
                    state: ::core::default::Default::default(),
                    config: (),
                    consts: $crate::__cpu::NoConsts,
                    outs: [[0; 16]; __OUTPUTS],
                    sharing: [$crate::__cpu::Keep::Fresh; __OUTPUTS],
                    keep: ($($crate::__emit_default!($oty),)*),
                }
            }
        }

        const __INPUTS: [u16; $fixed] = {
            let mut __sizes = [0; $fixed];
            $($crate::__port_size!($role $at: $ty; __sizes);)*
            __sizes
        };
        const __SIZES: [u16; __OUTPUTS] = {
            let mut __sizes = [0; __OUTPUTS];
            $(__sizes[$oat] = ::core::mem::size_of::<$oty>() as u16;)*
            __sizes
        };
        const __FAN: ::core::option::Option<u16> = {
            #[allow(unused_mut)]
            let mut __fan = ::core::option::Option::None;
            $($crate::__fan_size!($role: $ty; __fan);)*
            __fan
        };
        const __FITS: bool = !::core::mem::needs_drop::<__Run<$crate::__cpu::NoConsts>>()
            $(&& $crate::__port_fits!($role: $ty))*
            $(&& { const __KIND: $crate::__cpu::Kind<$oty> = $crate::__kind!($oty); __KIND.write.is_some() })*;
        const __BODY: ::core::option::Option<$crate::__cpu::ArenaBody> = if __FITS {
            ::core::option::Option::Some($crate::__cpu::ArenaBody::of::<__Run<$crate::__cpu::NoConsts>>(
                &__INPUTS, __FAN, &__SIZES,
            ))
        } else {
            ::core::option::Option::None
        };
        __BODY
    }};
}

#[macro_export]
#[doc(hidden)]
macro_rules! __port_size {
    (state $($rest:tt)*) => {};
    (config_ref $($rest:tt)*) => {};
    (config_val $($rest:tt)*) => {};
    (ctx $($rest:tt)*) => {};
    (fanin $($rest:tt)*) => {};
    ($role:ident $at:literal: $ty:ty; $sizes:ident) => {
        $sizes[$at] = ::core::mem::size_of::<$ty>() as u16;
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __fan_size {
    (fanin: $ty:ty; $fan:ident) => {
        $fan = ::core::option::Option::Some(::core::mem::size_of::<$ty>() as u16);
    };
    ($other:ident: $ty:ty; $fan:ident) => {};
}

/// Whether a parameter's value can be held in a cell's own buffer.
#[macro_export]
#[doc(hidden)]
macro_rules! __port_fits {
    (state: $ty:ty) => {
        true
    };
    (config_ref: $ty:ty) => {
        true
    };
    (config_val: $ty:ty) => {
        true
    };
    (ctx: $ty:ty) => {
        true
    };
    ($role:ident: $ty:ty) => {{
        const __KIND: $crate::__cpu::Kind<$ty> = $crate::__kind!($ty);
        __KIND.write.is_some()
    }};
}

#[macro_export]
#[doc(hidden)]
macro_rules! __role {
    (read) => {
        $crate::__cpu::Role::Read
    };
    (take) => {
        $crate::__cpu::Role::Take
    };
    (modify) => {
        $crate::__cpu::Role::Modify
    };
    (opt_read) => {
        $crate::__cpu::Role::OptRead
    };
    (opt_take) => {
        $crate::__cpu::Role::OptTake
    };
    (opt_modify) => {
        $crate::__cpu::Role::OptModify
    };
    (state) => {
        $crate::__cpu::Role::State
    };
    (config_ref) => {
        $crate::__cpu::Role::ConfigRef
    };
    (config_val) => {
        $crate::__cpu::Role::ConfigVal
    };
    (ctx) => {
        $crate::__cpu::Role::Context
    };
    (fanin) => {
        $crate::__cpu::Role::FanIn
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __optional {
    (option) => {
        true
    };
    (value) => {
        false
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __fallible {
    (fallible) => {
        true
    };
    (plain) => {
        false
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __or_unit {
    () => {
        ()
    };
    ($ty:ty) => {
        $ty
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __is_fanin {
    (fanin) => {
        1
    };
    ($other:ident) => {
        0
    };
}

/// How a concrete type is stored.
#[macro_export]
#[doc(hidden)]
macro_rules! __kind {
    ($ty:ty) => {{
        #[allow(unused_imports)]
        use $crate::__cpu::{NotClone as _, NotCopy as _, NotHandle as _, NotLit as _};
        $crate::__cpu::Kind::<$ty> {
            read: $crate::__cpu::CopyOf::<$ty>::READ,
            write: $crate::__cpu::CopyOf::<$ty>::WRITE,
            clone: $crate::__cpu::CloneOf::<$ty>::CLONE,
            copy: $crate::__cpu::CopyOf::<$ty>::IS,
            decode: $crate::__cpu::LitOf::<$ty>::DECODE,
            handle: $crate::__cpu::HandleOf::<$ty>::HANDLE,
        }
    }};
}

#[macro_export]
#[doc(hidden)]
macro_rules! __is_copy {
    ($ty:ty) => {{
        #[allow(unused_imports)]
        use $crate::__cpu::NotCopy as _;
        $crate::__cpu::CopyOf::<$ty>::IS
    }};
}

/// Returns `Skipped` when a required input has nothing. Done before any input is taken.
#[macro_export]
#[doc(hidden)]
macro_rules! __check {
    (read $at:literal; $fixed:ident, $this:ident) => { $crate::__require!($at; $fixed, $this) };
    (take $at:literal; $fixed:ident, $this:ident) => { $crate::__require!($at; $fixed, $this) };
    (modify $at:literal; $fixed:ident, $this:ident) => { $crate::__require!($at; $fixed, $this) };
    ($other:ident $at:literal; $fixed:ident, $this:ident) => {};
}

#[macro_export]
#[doc(hidden)]
macro_rules! __require {
    ($at:literal; $fixed:ident, $this:ident) => {
        if !$crate::__cpu::ports::present(&$fixed[$at], &$this.consts, $at) {
            return $crate::__cpu::StepEnd::Skipped;
        }
    };
}

/// Fixes an input's constant while planning.
#[macro_export]
#[doc(hidden)]
macro_rules! __const {
    (state $($rest:tt)*) => {};
    (config_ref $($rest:tt)*) => {};
    (config_val $($rest:tt)*) => {};
    (ctx $($rest:tt)*) => {};
    (fanin $($rest:tt)*) => {};
    ($role:ident $at:literal $port:literal: $ty:ty; $setup:ident, $consts:ident) => {{
        const __KIND: $crate::__cpu::Kind<$ty> = $crate::__kind!($ty);
        $crate::__cpu::store_const($setup, $port, $at, __KIND, &mut $consts)?;
    }};
}

#[macro_export]
#[doc(hidden)]
macro_rules! __config {
    ($setup:ident;) => {
        ()
    };
    ($setup:ident; $config:ty) => {{
        let __fields = $setup.config_fields();
        <$config>::from_literals(|name| $crate::__cpu::field(&__fields, name))
            .map_err(|_| $crate::__cpu::StepString::from("the config doesn't fit its type"))?
    }};
}

/// Binds one parameter.
#[macro_export]
#[doc(hidden)]
macro_rules! __bind {
    (state $at:literal $arg:ident: $ty:ty; $next:ident, $this:ident, $cx:ident, $fan:ident) => {
        let $arg: &mut $ty = &mut $this.state;
    };
    (config_ref $at:literal $arg:ident: $ty:ty; $next:ident, $this:ident, $cx:ident, $fan:ident) => {
        let $arg: &$ty = &$this.config;
    };
    (config_val $at:literal $arg:ident: $ty:ty; $next:ident, $this:ident, $cx:ident, $fan:ident) => {
        let $arg: $ty = ::core::clone::Clone::clone(&$this.config);
    };
    (ctx $at:literal $arg:ident: $ty:ty; $next:ident, $this:ident, $cx:ident, $fan:ident) => {
        let $arg: &$ty = &*$cx;
    };
    (fanin $at:literal $arg:ident: $ty:ty; $next:ident, $this:ident, $cx:ident, $fan:ident) => {
        let $arg: $crate::__cpu::ports::FanIn<'_, $ty> = {
            const __KIND: $crate::__cpu::Kind<$ty> = $crate::__kind!($ty);
            $crate::__cpu::ports::FanIn::new(&*$fan, __KIND)
        };
    };
    (read $at:literal $arg:ident: $ty:ty; $next:ident, $this:ident, $cx:ident, $fan:ident) => {
        let __cell = $next.next();
        let mut __hold: ::core::option::Option<$ty> = ::core::option::Option::None;
        let $arg: &$ty = match $crate::__read!($at, $ty; __cell, __hold, $this) {
            ::core::option::Option::Some(value) => value,
            ::core::option::Option::None => {
                return $crate::__cpu::ports::unusable($cx, $this.node);
            }
        };
    };
    (opt_read $at:literal $arg:ident: $ty:ty; $next:ident, $this:ident, $cx:ident, $fan:ident) => {
        let __cell = $next.next();
        let mut __hold: ::core::option::Option<$ty> = ::core::option::Option::None;
        let $arg: ::core::option::Option<&$ty> = $crate::__read!($at, $ty; __cell, __hold, $this);
    };
    (take $at:literal $arg:ident: $ty:ty; $next:ident, $this:ident, $cx:ident, $fan:ident) => {
        let $arg: $ty = match $crate::__take!($at, $ty; $next, $this) {
            ::core::option::Option::Some(value) => value,
            ::core::option::Option::None => {
                return $crate::__cpu::ports::unusable($cx, $this.node);
            }
        };
    };
    (opt_take $at:literal $arg:ident: $ty:ty; $next:ident, $this:ident, $cx:ident, $fan:ident) => {
        let $arg: ::core::option::Option<$ty> = $crate::__take!($at, $ty; $next, $this);
    };
    (modify $at:literal $arg:ident: $ty:ty; $next:ident, $this:ident, $cx:ident, $fan:ident) => {
        let mut __value: $ty = match $crate::__take!($at, $ty; $next, $this) {
            ::core::option::Option::Some(value) => value,
            ::core::option::Option::None => {
                return $crate::__cpu::ports::unusable($cx, $this.node);
            }
        };
        let $arg: &mut $ty = &mut __value;
    };
    (opt_modify $at:literal $arg:ident: $ty:ty; $next:ident, $this:ident, $cx:ident, $fan:ident) => {
        let mut __value: ::core::option::Option<$ty> = $crate::__take!($at, $ty; $next, $this);
        let $arg: ::core::option::Option<&mut $ty> = __value.as_mut();
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __read {
    ($at:literal, $ty:ty; $cell:ident, $hold:ident, $this:ident) => {{
        const __KIND: $crate::__cpu::Kind<$ty> = $crate::__kind!($ty);
        match $cell {
            ::core::option::Option::Some(cell) => {
                $crate::__cpu::ports::read(&*cell, &$this.consts, $at, __KIND, &mut $hold)
            }
            ::core::option::Option::None => ::core::option::Option::None,
        }
    }};
}

#[macro_export]
#[doc(hidden)]
macro_rules! __take {
    ($at:literal, $ty:ty; $next:ident, $this:ident) => {{
        const __KIND: $crate::__cpu::Kind<$ty> = $crate::__kind!($ty);
        match $next.next() {
            ::core::option::Option::Some(cell) => {
                $crate::__cpu::ports::take(cell, &$this.consts, $at, __KIND)
            }
            ::core::option::Option::None => ::core::option::Option::None,
        }
    }};
}

/// Turns an `Err` into a fault and leaves the step.
#[macro_export]
#[doc(hidden)]
macro_rules! __unfail {
    (plain; $ret:ident, $this:ident, $cx:ident) => {
        $ret
    };
    (fallible; $ret:ident, $this:ident, $cx:ident) => {
        match $ret {
            ::core::result::Result::Ok(value) => value,
            ::core::result::Result::Err(error) => {
                $cx.fail($this.node, &error);
                return $crate::__cpu::StepEnd::Fault;
            }
        }
    };
}

/// Writes one output.
#[macro_export]
#[doc(hidden)]
macro_rules! __emit {
    (value $at:tt $name:ident: $ty:ty; $out:ident, $this:ident) => {{
        const __KIND: $crate::__cpu::Kind<$ty> = $crate::__kind!($ty);
        $crate::__cpu::ports::encode(
            $name,
            __KIND,
            $this.sharing[$at],
            &mut $this.keep.$at,
            &mut $this.outs[$at],
            &mut $out[$at],
        );
    }};
    (option $at:tt $name:ident: $ty:ty; $out:ident, $this:ident) => {{
        if let ::core::option::Option::Some(__value) = $name {
            const __KIND: $crate::__cpu::Kind<$ty> = $crate::__kind!($ty);
            $crate::__cpu::ports::encode(
                __value,
                __KIND,
                $this.sharing[$at],
                &mut $this.keep.$at,
                &mut $this.outs[$at],
                &mut $out[$at],
            );
        }
    }};
}

/// The starting value of one output's kept memory.
#[macro_export]
#[doc(hidden)]
macro_rules! __emit_default {
    ($ty:ty) => {
        ::core::default::Default::default()
    };
}

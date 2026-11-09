//! The state slots of a `state(T)` node. The macros keep a stateful node's state in one tuple,
//! `(T, ...)`: `T` first, then the caches of its decoded config and inputs. The caches are
//! disposable, so only `T` crosses the wire.

use daedalus::data::to_value::ToValue;

/// A node's stored state tuple: its state `State` in the first slot, the rest defaulted.
pub trait StateSlots: Default + Send + Sync + 'static {
    /// The `state(T)` type that crosses the wire.
    type State: ToValue + Send + Sync + 'static;
    fn state(&self) -> &Self::State;
    fn state_mut(&mut self) -> &mut Self::State;
}

macro_rules! state_slots {
    ($(($($rest:ident),*)),* $(,)?) => {
        $(
            impl<T, $($rest),*> StateSlots for (T, $($rest,)*)
            where
                T: ToValue + Default + Send + Sync + 'static,
                $($rest: Default + Send + Sync + 'static,)*
            {
                type State = T;
                fn state(&self) -> &T {
                    &self.0
                }
                fn state_mut(&mut self) -> &mut T {
                    &mut self.0
                }
            }
        )*
    };
}

state_slots!((), (A), (A, B), (A, B, C), (A, B, C, D));

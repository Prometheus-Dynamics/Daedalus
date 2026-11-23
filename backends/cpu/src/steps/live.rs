//! A constant changed while the graph runs. A step that has constants registers how each is
//! decoded; a host offers a new value between ticks, which is decoded there and left for the
//! step, and has the runner mark the step's next call; the step takes what arrived when it sees
//! the mark. A run with nothing to take does nothing for this. Only on a host with `std`:
//! anywhere else a step's constants are what it was built with.

use alloc::boxed::Box;
use core::any::Any;

use daedalus_runner::Held;

/// A decoded value on its way to a step.
pub enum Arrived {
    /// For the fixed input at this index.
    Input(usize, Held),
    /// The node's config, as its own type.
    Config(Box<dyn Any + Send>),
}

#[cfg(feature = "std")]
mod shared {
    use alloc::boxed::Box;
    use alloc::string::String;
    use alloc::sync::{Arc, Weak};
    use alloc::vec::Vec;
    use std::sync::{Mutex, PoisonError};

    use daedalus_backend::{NotTaken, Repatch};
    use daedalus_graph::Lit;
    use daedalus_types::NodeIndex;

    use super::Arrived;

    /// What has arrived for one step.
    #[derive(Default)]
    struct Inbox {
        arrived: Mutex<Vec<Arrived>>,
    }

    type Decode = Box<dyn Fn(&Lit) -> Result<Arrived, String> + Send + Sync>;

    struct Reader {
        node: NodeIndex,
        port: String,
        decode: Decode,
        /// Dead once the step is dropped: a step compiled and then discarded reads nothing.
        inbox: Weak<Inbox>,
    }

    /// How the constants of one unit's steps are decoded, by node and port.
    #[derive(Default)]
    pub struct Readers(Mutex<Vec<Reader>>);

    impl core::fmt::Debug for Readers {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            f.write_str("Readers")
        }
    }

    impl Readers {
        fn all(&self) -> std::sync::MutexGuard<'_, Vec<Reader>> {
            self.0.lock().unwrap_or_else(PoisonError::into_inner)
        }

        /// As what a compiled unit hands back. `None` when no step registered a constant.
        pub fn into_patch(self) -> Option<Arc<dyn Repatch>> {
            let any = !self.all().is_empty();
            any.then(|| Arc::new(self) as Arc<dyn Repatch>)
        }
    }

    impl Repatch for Readers {
        fn offer(&self, node: NodeIndex, port: &str, value: &Lit) -> Result<(), NotTaken> {
            let mut taken = Err(NotTaken::NoReader);
            for reader in self.all().iter() {
                if reader.node != node || reader.port != port {
                    continue;
                }
                let Some(inbox) = reader.inbox.upgrade() else {
                    continue;
                };
                let arrived = (reader.decode)(value).map_err(NotTaken::Refused)?;
                let mut waiting = inbox.arrived.lock().unwrap_or_else(PoisonError::into_inner);
                waiting.push(arrived);
                taken = Ok(());
            }
            taken
        }

        fn reads(&self) -> bool {
            let all = self.all();
            all.iter().any(|reader| reader.inbox.strong_count() > 0)
        }
    }

    /// A step's end of it: nothing until one of its constants is registered.
    #[derive(Default)]
    pub struct Live {
        inbox: Option<Arc<Inbox>>,
    }

    impl Live {
        /// Registers how `node`'s `port` is decoded.
        pub fn read(
            &mut self,
            readers: Option<&Readers>,
            node: NodeIndex,
            port: &str,
            decode: impl Fn(&Lit) -> Result<Arrived, String> + Send + Sync + 'static,
        ) {
            let Some(readers) = readers else { return };
            let inbox = self.inbox.get_or_insert_with(Arc::default);
            readers.all().push(Reader {
                node,
                port: String::from(port),
                decode: Box::new(decode),
                inbox: Arc::downgrade(inbox),
            });
        }

        /// What arrived, in the order it did.
        pub fn take(&mut self) -> Vec<Arrived> {
            let Some(inbox) = &self.inbox else {
                return Vec::new();
            };
            let mut waiting = inbox.arrived.lock().unwrap_or_else(PoisonError::into_inner);
            core::mem::take(&mut *waiting)
        }
    }
}

#[cfg(not(feature = "std"))]
mod shared {
    use alloc::string::String;
    use alloc::vec::Vec;

    use daedalus_graph::Lit;
    use daedalus_types::NodeIndex;

    use super::Arrived;

    /// Without `std` nothing is offered.
    #[derive(Debug, Default)]
    pub struct Readers;

    impl Readers {
        pub fn into_patch(self) -> Option<alloc::sync::Arc<dyn daedalus_backend::Repatch>> {
            None
        }
    }

    #[derive(Default)]
    pub struct Live;

    impl Live {
        pub fn read(
            &mut self,
            _: Option<&Readers>,
            _: NodeIndex,
            _: &str,
            _: impl Fn(&Lit) -> Result<Arrived, String> + Send + Sync + 'static,
        ) {
        }

        pub fn take(&mut self) -> Vec<Arrived> {
            Vec::new()
        }
    }
}

pub use shared::{Live, Readers};

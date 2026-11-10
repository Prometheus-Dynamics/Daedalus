//! Feed, step and take. A host owns one loaded graph and one port table for it.

use core::fmt;

use daedalus_runner::{RunError, Runner, TickReport};
use daedalus_types::{TypeIndex, TypeKey};
use daedalus_value::{DropReason, Value};

use crate::label::Label;
use crate::ports::{Dir, Ports};

/// What a feed did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeedOutcome {
    /// How many consumer steps were woken. A feed never runs a tick.
    pub woke: u32,
    /// Why a value was given up under the port's policy, if one was.
    pub dropped: Option<DropReason>,
}

/// Why a host call was refused. Every variant that involves a port names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostError {
    UnknownPort {
        port: Label,
    },
    NotAnInput {
        port: Label,
    },
    NotAnOutput {
        port: Label,
    },
    /// The value's type is not the port's. Both keys are named; `found` is empty when the host has
    /// no key for the index it was given.
    TypeMismatch {
        port: Label,
        expected: Label,
        found_index: u32,
        found: Label,
    },
    /// The value is not a typed payload, so it can't come out as a Rust type.
    NotTyped {
        port: Label,
    },
    /// The payload is typed, but not as the Rust type the caller asked for.
    WrongRustType {
        port: Label,
    },
    /// The input's queue is full.
    Full {
        port: Label,
    },
    /// A typed payload was fed as a plain value. Feed it with the typed feed.
    Untyped {
        port: Label,
    },
    /// The type's serializer refused the value.
    Refused {
        port: Label,
        key: Label,
    },
    /// The graph refused the tick.
    Run(RunError),
}

impl fmt::Display for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HostError::UnknownPort { port } => write!(f, "no port named {port}"),
            HostError::NotAnInput { port } => write!(f, "port {port} is an output, not an input"),
            HostError::NotAnOutput { port } => write!(f, "port {port} is an input, not an output"),
            HostError::TypeMismatch {
                port,
                expected,
                found_index,
                found,
            } => write!(
                f,
                "port {port} expects type {expected}, got type {found} (index {found_index})"
            ),
            HostError::NotTyped { port } => write!(f, "port {port} holds no typed payload"),
            HostError::WrongRustType { port } => {
                write!(f, "port {port} holds a payload of another Rust type")
            }
            HostError::Full { port } => write!(f, "port {port} is full"),
            HostError::Untyped { port } => write!(f, "port {port} needs the typed feed"),
            HostError::Refused { port, key } => {
                write!(f, "port {port} refused a value of type {key}")
            }
            HostError::Run(error) => write!(f, "the run was refused: {error:?}"),
        }
    }
}

/// One loaded graph, its ports, and the key of each type index.
pub struct Host<'a> {
    pub(crate) runner: Runner<'a>,
    pub(crate) ports: Ports<'a>,
    pub(crate) types: &'a [TypeKey],
    /// One per port once a host sets a policy. Empty means every port takes every value.
    pub(crate) inbound: std::vec::Vec<crate::inbound::Inbound>,
    pub(crate) now_ns: u64,
    /// Whether a feed to a held slot wakes its readers. A plan holds any slot with several
    /// readers, and those are ordinary inputs.
    pub(crate) held_feeds_wake: bool,
    /// The buffers of a runner this host owns. Declared last, so it is dropped after `runner`.
    #[cfg(feature = "std")]
    pub(crate) owned: Option<daedalus_runner::owned_run::RunBuffers>,
}

impl<'a> Host<'a> {
    /// A host over `runner`. `types[i]` is the key of type index `i`, for error messages.
    pub fn new(runner: Runner<'a>, ports: Ports<'a>, types: &'a [TypeKey]) -> Host<'a> {
        Host {
            runner,
            ports,
            types,
            inbound: std::vec::Vec::new(),
            now_ns: 0,
            held_feeds_wake: false,
            #[cfg(feature = "std")]
            owned: None,
        }
    }

    /// The label for a type index, or an empty label when the host has no key for it.
    fn key_label(&self, index: TypeIndex) -> Label {
        self.types
            .get(index.0 as usize)
            .map_or(Label::new(""), |key| Label::new(key.as_str()))
    }

    fn mismatch(&self, port: &str, expected: TypeIndex, found: TypeIndex) -> HostError {
        HostError::TypeMismatch {
            port: Label::new(port),
            expected: self.key_label(expected),
            found_index: found.0,
            found: self.key_label(found),
        }
    }

    pub(crate) fn input(&self, name: &str) -> Result<&'a crate::ports::PortSpec<'a>, HostError> {
        let spec = self.ports.find(name).ok_or(HostError::UnknownPort {
            port: Label::new(name),
        })?;
        match spec.dir {
            Dir::Input => Ok(spec),
            Dir::Output => Err(HostError::NotAnInput {
                port: Label::new(name),
            }),
        }
    }

    pub(crate) fn output(&self, name: &str) -> Result<&'a crate::ports::PortSpec<'a>, HostError> {
        let spec = self.ports.find(name).ok_or(HostError::UnknownPort {
            port: Label::new(name),
        })?;
        match spec.dir {
            Dir::Output => Ok(spec),
            Dir::Input => Err(HostError::NotAnOutput {
                port: Label::new(name),
            }),
        }
    }

    pub(crate) fn wake_consumers(&mut self, wake: &[u32]) -> u32 {
        for &step in wake {
            self.runner.wake(step);
        }
        wake.len() as u32
    }

    /// Stores `value` in the input port `name`, and wakes its consumers. It never runs a tick.
    pub fn feed(&mut self, name: &str, value: Value) -> Result<FeedOutcome, HostError> {
        let spec = self.input(name)?;
        if value.ty != spec.ty {
            return Err(self.mismatch(name, spec.ty, value.ty));
        }
        if self.runner.is_held(spec.slot) {
            return self.replace_held(name, spec.slot, spec.wake, value);
        }
        let dropped = match self.admit(name, spec.slot)? {
            crate::inbound::Admit::Store(reason) => reason,
            crate::inbound::Admit::Discard(reason) => {
                return Ok(FeedOutcome {
                    woke: 0,
                    dropped: Some(reason),
                });
            }
        };
        self.runner
            .feed_value(spec.slot, value)
            .map_err(|error| self.feed_error(name, error))?;
        self.note_arrival(name);
        Ok(FeedOutcome {
            woke: self.wake_consumers(spec.wake),
            dropped,
        })
    }

    /// Stores up to 16 bytes of plain data in the input port `name`, in place, with the port's own
    /// type and memory space: a number goes in as its little-endian bytes. Nothing is allocated.
    /// More than 16 bytes are refused as [`HostError::Refused`].
    pub fn feed_inline(&mut self, name: &str, bytes: &[u8]) -> Result<FeedOutcome, HostError> {
        let spec = self.input(name)?;
        let inline = daedalus_value::InlineBytes::from_slice(bytes).ok_or(HostError::Refused {
            port: Label::new(name),
            key: Label::new(spec.key.as_str()),
        })?;
        let value = Value {
            ty: spec.ty,
            space: spec.space,
            owner: daedalus_value::Owner::Inline(inline),
        };
        self.feed(name, value)
    }

    /// Stores a typed payload in the input port `name`, and wakes its consumers.
    pub fn feed_typed<T: Send + Sync + 'static>(
        &mut self,
        name: &str,
        value: std::sync::Arc<T>,
    ) -> Result<FeedOutcome, HostError> {
        let spec = self.input(name)?;
        let held = self.runner.is_held(spec.slot);
        if held {
            self.runner.clear_held(spec.slot).map_err(HostError::Run)?;
        }
        self.runner
            .feed_typed(spec.slot, spec.ty, spec.space, value)
            .map_err(|error| self.feed_error(name, error))?;
        if held && !self.held_feeds_wake {
            return Ok(FeedOutcome {
                woke: 0,
                dropped: None,
            });
        }
        Ok(FeedOutcome {
            woke: self.wake_consumers(spec.wake),
            dropped: None,
        })
    }

    /// Declares the version of what the input port `name` delivers, or clears it so the port
    /// compares by identity. A reusable node's stamp uses this instead of the value's identity.
    pub fn set_input_version(&mut self, name: &str, version: Option<u64>) -> Result<(), HostError> {
        let spec = self.input(name)?;
        self.runner.set_input_version(spec.slot, version);
        Ok(())
    }

    /// Forgets every stored output, so reusable nodes run again.
    pub fn clear_reuse(&mut self) {
        self.runner.clear_reuse();
    }

    /// Several feeds in order. Stops at the first refused one; the ones before it stay fed.
    pub fn feed_all<'n>(
        &mut self,
        items: impl IntoIterator<Item = (&'n str, Value)>,
    ) -> Result<(), HostError> {
        for (name, value) in items {
            self.feed(name, value)?;
        }
        Ok(())
    }

    pub(crate) fn feed_error(&self, name: &str, error: RunError) -> HostError {
        match error {
            RunError::Overflow { .. } => HostError::Full {
                port: Label::new(name),
            },
            RunError::Untyped { .. } => HostError::Untyped {
                port: Label::new(name),
            },
            other => HostError::Run(other),
        }
    }

    /// Runs one tick.
    pub fn step(&mut self) -> Result<TickReport, HostError> {
        self.expire_stale();
        self.runner.tick().map_err(HostError::Run)
    }

    /// Sets the host's clock, in nanoseconds: what freshness rules and backoff measure against.
    pub fn set_clock(&mut self, now_ns: u64) {
        self.now_ns = now_ns;
        self.runner.set_clock(now_ns);
    }

    /// Takes the oldest value out of the output port `name`, if there is one.
    pub fn take_value(&mut self, name: &str) -> Result<Option<Value>, HostError> {
        let spec = self.output(name)?;
        match self.runner.peek_type(spec.slot) {
            None => Ok(None),
            Some(found) if found != spec.ty => Err(self.mismatch(name, spec.ty, found)),
            Some(_) => Ok(self.runner.take_value(spec.slot)),
        }
    }

    /// Takes the oldest value out of the output port `name`, ready to hand to other graphs.
    #[cfg(feature = "std")]
    pub fn take_forward(
        &mut self,
        name: &str,
    ) -> Result<Option<daedalus_runner::Forward>, HostError> {
        let spec = self.output(name)?;
        Ok(self.runner.take_forward(spec.slot))
    }

    /// Delivers a forwarded value to the input port `name`. The types were compared when the link
    /// was made, since each plan numbers its types itself.
    #[cfg(feature = "std")]
    pub fn feed_forward(
        &mut self,
        name: &str,
        forward: daedalus_runner::Forward,
    ) -> Result<FeedOutcome, HostError> {
        let spec = self.input(name)?;
        let held = self.runner.is_held(spec.slot);
        if held {
            self.runner.clear_held(spec.slot).map_err(HostError::Run)?;
        }
        self.runner
            .feed_forward(spec.slot, spec.ty, spec.space, forward)
            .map_err(|error| self.feed_error(name, error))?;
        Ok(FeedOutcome {
            woke: if held {
                0
            } else {
                self.wake_consumers(spec.wake)
            },
            dropped: None,
        })
    }

    /// Takes every value out of the output port `name`, oldest first.
    pub fn drain_value(&mut self, name: &str) -> Result<std::vec::Vec<Value>, HostError> {
        let mut values = std::vec::Vec::new();
        while let Some(value) = self.take_value(name)? {
            values.push(value);
        }
        Ok(values)
    }

    /// The graph's runner, for hosts that need more than the port calls.
    pub fn runner(&mut self) -> &mut Runner<'a> {
        &mut self.runner
    }
}

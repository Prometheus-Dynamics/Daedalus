//! A client for the service protocol. It connects over a unix socket or loopback TCP, runs the
//! hello, and gives one typed method per op. Requests pipeline: several may be sent before the
//! first is answered, from one thread or from many, and each answer is matched by its id.
//!
//! A client asked to keep trying reconnects after a disconnect with a backoff capped at 8 s, and
//! never stops on its own. Its watches are subscribed again once it is back. While it is
//! disconnected a call fails with `Disconnected`. A request that was in flight when the
//! connection dropped is resent only if sending it twice is harmless (ping, inspect, trace);
//! any other returns `UnknownDelivery`, since the server may or may not have acted on it.

#![forbid(unsafe_code)]

mod connect;
mod reconnect;
mod session;
mod watch;

pub use connect::{Client, ClientError, Endpoint, Options, Pending};
pub use reconnect::{BACKOFF_CAP, BACKOFF_START, Backoff, Clock, SystemClock};
pub use session::{Fed, Stopped};
pub use watch::Watch;

/// The protocol's own names: ops, replies, events and errors.
pub use daedalus_wire::service as protocol;
pub use daedalus_wire::{WireError, WireType, WireValue};

use std::fmt::Debug;

pub mod bounded;
pub mod broadcast;
pub mod newest;
pub mod unbounded;

#[cfg(feature = "async-channels")]
pub mod async_;
pub mod policy;

pub use bounded::{BoundedReceiver, BoundedSender, bounded};
pub use broadcast::{BroadcastReceiver, BroadcastSender, broadcast};
pub use newest::{NewestReceiver, NewestSender, newest};
pub use unbounded::{UnboundedReceiver, UnboundedSender, unbounded};

#[cfg(feature = "metrics")]
pub use bounded::bounded_with_metrics;
#[cfg(feature = "metrics")]
pub use broadcast::broadcast_with_metrics;
#[cfg(feature = "metrics")]
pub use newest::newest_with_metrics;
#[cfg(feature = "metrics")]
pub use unbounded::unbounded_with_metrics;

/// Result of attempting to enqueue a message.
///
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backpressure {
    Ok,
    Full,
    Closed,
}

/// Result of attempting to receive a message.
///
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecvOutcome<T> {
    Data(T),
    Empty,
    Closed,
}

/// Common sender interface.
///
pub trait ChannelSend<T>: Send + Sync {
    fn send(&self, value: T) -> Backpressure;
}

/// Common receiver interface (non-blocking).
///
pub trait ChannelRecv<T>: Send + Sync {
    fn try_recv(&self) -> RecvOutcome<T>;
}

/// Behavior when peers drop.
///
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseBehavior {
    /// Mark closed when either senders or receivers are gone (fail fast).
    FailFast,
    /// Keep channel open until senders are gone (receivers may drop).
    DrainUntilSendersDone,
}

/// Snapshot stats for a channel.
///
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChannelStats {
    pub enqueued: u64,
    pub dropped: u64,
    pub drained: u64,
    pub depth: usize,
    pub closed: bool,
}

use super::CloseBehavior;
use super::bounded::{BoundedReceiver, BoundedSender, bounded_with_behavior};
use super::broadcast::{BroadcastReceiver, BroadcastSender, broadcast_with_behavior};
use super::newest::{NewestReceiver, NewestSender, newest_with_behavior};
use super::unbounded::{UnboundedReceiver, UnboundedSender, unbounded_with_behavior};

#[cfg(feature = "async-channels")]
use super::async_::{
    AsyncReceiver, AsyncSender, bounded_async_with_behavior, broadcast_async_with_behavior,
    newest_async_with_behavior, unbounded_async_with_behavior,
};
#[cfg(feature = "async-channels")]
use std::sync::Arc;

/// Selector for edge policies to construct matching channels.
pub enum EdgePolicy {
    Bounded {
        capacity: usize,
        close: CloseBehavior,
    },
    Unbounded {
        close: CloseBehavior,
    },
    Broadcast {
        capacity: usize,
        close: CloseBehavior,
    },
    Newest {
        close: CloseBehavior,
    },
}

pub fn build_bounded<T>(
    capacity: usize,
    close: CloseBehavior,
) -> (BoundedSender<T>, BoundedReceiver<T>) {
    bounded_with_behavior(capacity, close)
}

pub fn build_unbounded<T>(close: CloseBehavior) -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    unbounded_with_behavior(close)
}

pub fn build_broadcast<T: Send + Sync>(
    capacity: usize,
    close: CloseBehavior,
) -> (BroadcastSender<T>, BroadcastReceiver<T>) {
    broadcast_with_behavior(capacity, close)
}

pub fn build_newest<T>(close: CloseBehavior) -> (NewestSender<T>, NewestReceiver<T>) {
    newest_with_behavior(close)
}

#[cfg(feature = "async-channels")]
pub fn build_async<T: Send + Sync>(policy: EdgePolicy) -> ChannelAsync<T> {
    match policy {
        EdgePolicy::Bounded { capacity, close } => {
            let (tx, rx) = bounded_async_with_behavior(capacity, close);
            ChannelAsync::Bounded {
                sender: tx,
                receiver: rx,
            }
        }
        EdgePolicy::Unbounded { close } => {
            let (tx, rx) = unbounded_async_with_behavior(close);
            ChannelAsync::Unbounded {
                sender: tx,
                receiver: rx,
            }
        }
        EdgePolicy::Broadcast { capacity, close } => {
            let (tx, rx) = broadcast_async_with_behavior::<T>(capacity, close);
            ChannelAsync::Broadcast {
                sender: tx,
                receiver: rx,
            }
        }
        EdgePolicy::Newest { close } => {
            let (tx, rx) = newest_async_with_behavior::<T>(close);
            ChannelAsync::Newest {
                sender: tx,
                receiver: rx,
            }
        }
    }
}

#[cfg(feature = "async-channels")]
pub enum ChannelAsync<T: Send + Sync> {
    Bounded {
        sender: AsyncSender<T, super::bounded::BoundedSender<T>>,
        receiver: AsyncReceiver<T, super::bounded::BoundedReceiver<T>>,
    },
    Unbounded {
        sender: AsyncSender<T, super::unbounded::UnboundedSender<T>>,
        receiver: AsyncReceiver<T, super::unbounded::UnboundedReceiver<T>>,
    },
    Broadcast {
        sender: AsyncSender<Arc<T>, super::broadcast::BroadcastSender<T>>,
        receiver: AsyncReceiver<Arc<T>, super::broadcast::BroadcastReceiver<T>>,
    },
    Newest {
        sender: AsyncSender<Arc<T>, super::newest::NewestSender<T>>,
        receiver: AsyncReceiver<Arc<T>, super::newest::NewestReceiver<T>>,
    },
}

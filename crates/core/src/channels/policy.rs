use super::CloseBehavior;
use super::bounded::{BoundedReceiver, BoundedSender, bounded_with_behavior};
use super::broadcast::{BroadcastReceiver, BroadcastSender, broadcast_with_behavior};
use super::unbounded::{UnboundedReceiver, UnboundedSender, unbounded_with_behavior};

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

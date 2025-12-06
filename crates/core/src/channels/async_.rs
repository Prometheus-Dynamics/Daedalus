use std::sync::Arc;

use tokio::task::yield_now;

use super::bounded::{BoundedReceiver, BoundedSender, bounded, bounded_with_behavior};
use super::broadcast::{BroadcastReceiver, BroadcastSender, broadcast, broadcast_with_behavior};
use super::newest::{NewestReceiver, NewestSender, newest, newest_with_behavior};
use super::unbounded::{UnboundedReceiver, UnboundedSender, unbounded, unbounded_with_behavior};
use super::{Backpressure, ChannelRecv, ChannelSend, CloseBehavior, RecvOutcome};

/// Async wrappers around the sync channels. These are thin convenience layers
/// that poll in a loop with cooperative yields; they keep the underlying fast
/// sync paths intact.
pub struct AsyncSender<T, S> {
    inner: Arc<S>,
    send_fn: fn(&S, T) -> Backpressure,
}

pub struct AsyncReceiver<T, R> {
    inner: Arc<R>,
    recv_fn: fn(&R) -> RecvOutcome<T>,
}

pub type BroadcastAsyncSender<T> = AsyncSender<Arc<T>, BroadcastSender<T>>;
pub type BroadcastAsyncReceiver<T> = AsyncReceiver<Arc<T>, BroadcastReceiver<T>>;
pub type NewestAsyncSender<T> = AsyncSender<Arc<T>, NewestSender<T>>;
pub type NewestAsyncReceiver<T> = AsyncReceiver<Arc<T>, NewestReceiver<T>>;

fn channel_send<T, S: ChannelSend<T>>(sender: &S, value: T) -> Backpressure {
    sender.send(value)
}

fn channel_recv<T, R: ChannelRecv<T>>(receiver: &R) -> RecvOutcome<T> {
    receiver.try_recv()
}

impl<T: Send + Sync + 'static> AsyncSender<T, BoundedSender<T>> {
    pub async fn send(&self, value: T) -> Backpressure {
        let mut value = Some(value);
        loop {
            let v = value.take().expect("value missing");
            match self.inner.try_send_owned(v) {
                Ok(()) => return Backpressure::Ok,
                Err((Backpressure::Closed, _v)) => return Backpressure::Closed,
                Err((Backpressure::Full, v)) => {
                    value = Some(v);
                    let notify = self.inner.async_notify();
                    notify.notified().await;
                }
                Err((Backpressure::Ok, _v)) => return Backpressure::Ok,
            }
        }
    }
}

impl<T: Send + Sync + 'static> AsyncSender<T, UnboundedSender<T>> {
    pub async fn send(&self, value: T) -> Backpressure {
        (self.send_fn)(&self.inner, value)
    }
}

impl<T: Send + Sync + 'static> AsyncSender<Arc<T>, BroadcastSender<T>> {
    pub async fn send(&self, value: Arc<T>) -> Backpressure {
        (self.send_fn)(&self.inner, value)
    }
}

impl<T: Send + Sync + 'static> AsyncSender<Arc<T>, NewestSender<T>> {
    pub async fn send(&self, value: Arc<T>) -> Backpressure {
        (self.send_fn)(&self.inner, value)
    }
}

impl<T: Send + Sync + 'static, R> AsyncReceiver<T, R> {
    pub async fn recv(&self) -> RecvOutcome<T> {
        loop {
            match (self.recv_fn)(&self.inner) {
                RecvOutcome::Data(v) => return RecvOutcome::Data(v),
                RecvOutcome::Closed => return RecvOutcome::Closed,
                RecvOutcome::Empty => {
                    yield_now().await;
                    continue;
                }
            }
        }
    }
}

pub fn bounded_async<T: Send>(
    capacity: usize,
) -> (
    AsyncSender<T, BoundedSender<T>>,
    AsyncReceiver<T, BoundedReceiver<T>>,
) {
    let (tx, rx) = bounded(capacity);
    let tx = Arc::new(tx);
    let rx = Arc::new(rx);
    (
        AsyncSender {
            inner: Arc::clone(&tx),
            send_fn: channel_send::<T, BoundedSender<T>>,
        },
        AsyncReceiver {
            inner: Arc::clone(&rx),
            recv_fn: channel_recv::<T, BoundedReceiver<T>>,
        },
    )
}

pub fn bounded_async_with_behavior<T: Send>(
    capacity: usize,
    close_behavior: CloseBehavior,
) -> (
    AsyncSender<T, BoundedSender<T>>,
    AsyncReceiver<T, BoundedReceiver<T>>,
) {
    let (tx, rx) = bounded_with_behavior(capacity, close_behavior);
    let tx = Arc::new(tx);
    let rx = Arc::new(rx);
    (
        AsyncSender {
            inner: Arc::clone(&tx),
            send_fn: channel_send::<T, BoundedSender<T>>,
        },
        AsyncReceiver {
            inner: Arc::clone(&rx),
            recv_fn: channel_recv::<T, BoundedReceiver<T>>,
        },
    )
}

pub fn unbounded_async<T: Send>() -> (
    AsyncSender<T, UnboundedSender<T>>,
    AsyncReceiver<T, UnboundedReceiver<T>>,
) {
    let (tx, rx) = unbounded();
    let tx = Arc::new(tx);
    let rx = Arc::new(rx);
    (
        AsyncSender {
            inner: Arc::clone(&tx),
            send_fn: channel_send::<T, UnboundedSender<T>>,
        },
        AsyncReceiver {
            inner: Arc::clone(&rx),
            recv_fn: channel_recv::<T, UnboundedReceiver<T>>,
        },
    )
}

pub fn unbounded_async_with_behavior<T: Send>(
    close_behavior: CloseBehavior,
) -> (
    AsyncSender<T, UnboundedSender<T>>,
    AsyncReceiver<T, UnboundedReceiver<T>>,
) {
    let (tx, rx) = unbounded_with_behavior(close_behavior);
    let tx = Arc::new(tx);
    let rx = Arc::new(rx);
    (
        AsyncSender {
            inner: Arc::clone(&tx),
            send_fn: channel_send::<T, UnboundedSender<T>>,
        },
        AsyncReceiver {
            inner: Arc::clone(&rx),
            recv_fn: channel_recv::<T, UnboundedReceiver<T>>,
        },
    )
}

pub fn broadcast_async<T: Send + Sync>(
    capacity: usize,
) -> (BroadcastAsyncSender<T>, BroadcastAsyncReceiver<T>) {
    let (tx, rx) = broadcast(capacity);
    let tx = Arc::new(tx);
    let rx = Arc::new(rx);
    (
        AsyncSender {
            inner: Arc::clone(&tx),
            send_fn: channel_send::<Arc<T>, BroadcastSender<T>>,
        },
        AsyncReceiver {
            inner: Arc::clone(&rx),
            recv_fn: channel_recv::<Arc<T>, BroadcastReceiver<T>>,
        },
    )
}

pub fn broadcast_async_with_behavior<T: Send + Sync>(
    capacity: usize,
    close_behavior: CloseBehavior,
) -> (BroadcastAsyncSender<T>, BroadcastAsyncReceiver<T>) {
    let (tx, rx) = broadcast_with_behavior(capacity, close_behavior);
    let tx = Arc::new(tx);
    let rx = Arc::new(rx);
    (
        AsyncSender {
            inner: Arc::clone(&tx),
            send_fn: channel_send::<Arc<T>, BroadcastSender<T>>,
        },
        AsyncReceiver {
            inner: Arc::clone(&rx),
            recv_fn: channel_recv::<Arc<T>, BroadcastReceiver<T>>,
        },
    )
}

pub fn newest_async<T: Send + Sync>() -> (NewestAsyncSender<T>, NewestAsyncReceiver<T>) {
    let (tx, rx) = newest();
    let tx = Arc::new(tx);
    let rx = Arc::new(rx);
    (
        AsyncSender {
            inner: Arc::clone(&tx),
            send_fn: channel_send::<Arc<T>, NewestSender<T>>,
        },
        AsyncReceiver {
            inner: Arc::clone(&rx),
            recv_fn: channel_recv::<Arc<T>, NewestReceiver<T>>,
        },
    )
}

pub fn newest_async_with_behavior<T: Send + Sync>(
    close_behavior: CloseBehavior,
) -> (NewestAsyncSender<T>, NewestAsyncReceiver<T>) {
    let (tx, rx) = newest_with_behavior(close_behavior);
    let tx = Arc::new(tx);
    let rx = Arc::new(rx);
    (
        AsyncSender {
            inner: Arc::clone(&tx),
            send_fn: channel_send::<Arc<T>, NewestSender<T>>,
        },
        AsyncReceiver {
            inner: Arc::clone(&rx),
            recv_fn: channel_recv::<Arc<T>, NewestReceiver<T>>,
        },
    )
}

use std::sync::Arc;

use tokio::sync::Notify;

use super::bounded::{BoundedReceiver, BoundedSender, bounded, bounded_with_behavior};
use super::broadcast::{BroadcastReceiver, BroadcastSender, broadcast, broadcast_with_behavior};
use super::newest::{NewestReceiver, NewestSender, newest, newest_with_behavior};
use super::unbounded::{UnboundedReceiver, UnboundedSender, unbounded, unbounded_with_behavior};
use super::{Backpressure, ChannelRecv, ChannelSend, CloseBehavior, RecvOutcome};

/// Async wrappers around the sync channels. These are thin convenience layers
/// around the sync channels; they keep the underlying fast sync paths intact
/// while using notifications instead of spin polling for waits.
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

#[doc(hidden)]
pub trait AsyncRecvNotify {
    fn recv_notify(&self) -> Arc<Notify>;
}

impl<T> AsyncRecvNotify for BoundedReceiver<T> {
    fn recv_notify(&self) -> Arc<Notify> {
        self.async_notify()
    }
}

impl<T> AsyncRecvNotify for UnboundedReceiver<T> {
    fn recv_notify(&self) -> Arc<Notify> {
        self.async_notify()
    }
}

impl<T: Send + Sync> AsyncRecvNotify for BroadcastReceiver<T> {
    fn recv_notify(&self) -> Arc<Notify> {
        self.async_notify()
    }
}

impl<T: Send + Sync> AsyncRecvNotify for NewestReceiver<T> {
    fn recv_notify(&self) -> Arc<Notify> {
        self.async_notify()
    }
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

impl<T: Send + Sync + 'static, R: AsyncRecvNotify> AsyncReceiver<T, R> {
    pub async fn recv(&self) -> RecvOutcome<T> {
        loop {
            match (self.recv_fn)(&self.inner) {
                RecvOutcome::Data(v) => return RecvOutcome::Data(v),
                RecvOutcome::Closed => return RecvOutcome::Closed,
                RecvOutcome::Empty => {
                    let notify = self.inner.recv_notify();
                    let notified = notify.notified();
                    match (self.recv_fn)(&self.inner) {
                        RecvOutcome::Data(v) => return RecvOutcome::Data(v),
                        RecvOutcome::Closed => return RecvOutcome::Closed,
                        RecvOutcome::Empty => notified.await,
                    }
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn bounded_recv_wakes_on_send() {
        let (tx, rx) = bounded_async(1);
        let recv = tokio::time::timeout(Duration::from_millis(100), rx.recv());
        tx.send(7_u32).await;
        assert_eq!(recv.await.expect("recv timeout"), RecvOutcome::Data(7));
    }

    #[tokio::test]
    async fn unbounded_recv_wakes_on_close() {
        let (tx, rx) = unbounded_async::<u32>();
        let recv = tokio::time::timeout(Duration::from_millis(100), rx.recv());
        drop(tx);
        assert_eq!(recv.await.expect("recv timeout"), RecvOutcome::Closed);
    }

    #[tokio::test]
    async fn newest_recv_wakes_on_send() {
        let (tx, rx) = newest_async();
        let recv = tokio::time::timeout(Duration::from_millis(100), rx.recv());
        tx.send(Arc::new(9_u32)).await;
        assert_eq!(
            recv.await.expect("recv timeout"),
            RecvOutcome::Data(Arc::new(9))
        );
    }

    #[tokio::test]
    async fn broadcast_recv_wakes_on_send() {
        let (tx, rx) = broadcast_async(1);
        let recv = tokio::time::timeout(Duration::from_millis(100), rx.recv());
        tx.send(Arc::new(11_u32)).await;
        assert_eq!(
            recv.await.expect("recv timeout"),
            RecvOutcome::Data(Arc::new(11))
        );
    }
}

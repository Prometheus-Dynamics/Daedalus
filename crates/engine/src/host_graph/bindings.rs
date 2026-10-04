use std::marker::PhantomData;

use daedalus_runtime::TypeIndex;
use daedalus_runtime::executor::DirectHostRoute;
use daedalus_runtime::handles::PortId;
use daedalus_runtime::host_bridge::HostBridgeHandle;
use daedalus_transport::{FeedOutcome, Payload, TypeKey, TypeKeyError};

pub struct HostGraphSubscription {
    pub(crate) host: HostBridgeHandle,
    pub(crate) port: PortId,
}

pub struct HostGraphInput<T> {
    pub(crate) host: HostBridgeHandle,
    pub(crate) port: PortId,
    pub(crate) type_key: TypeKey,
    pub(crate) _ty: PhantomData<T>,
}

impl<T> HostGraphInput<T>
where
    T: Send + Sync + 'static,
{
    pub fn push(&self, value: T) -> FeedOutcome {
        self.host
            .feed_payload(&self.port, Payload::owned(self.type_key.clone(), value))
    }

    pub fn port(&self) -> &str {
        self.port.as_str()
    }
}

pub struct HostGraphPayloadInput {
    pub(crate) host: HostBridgeHandle,
    pub(crate) port: PortId,
}

impl HostGraphPayloadInput {
    pub fn push(&self, payload: Payload) -> FeedOutcome {
        self.host.feed_payload(&self.port, payload)
    }

    pub fn port(&self) -> &str {
        self.port.as_str()
    }
}

pub struct HostGraphOutput<T> {
    pub(crate) host: HostBridgeHandle,
    pub(crate) port: PortId,
    pub(crate) _ty: PhantomData<T>,
}

impl<T> HostGraphOutput<T>
where
    T: Send + Sync + 'static,
{
    pub fn try_take(&self) -> Result<Option<T>, Box<Payload>> {
        self.host.try_pop_owned::<T>(&self.port)
    }

    pub fn port(&self) -> &str {
        self.port.as_str()
    }
}

pub struct HostGraphPayloadOutput {
    pub(crate) host: HostBridgeHandle,
    pub(crate) port: PortId,
}

impl HostGraphPayloadOutput {
    pub fn try_take(&self) -> Option<Payload> {
        self.host.try_pop_payload(&self.port)
    }

    pub fn port(&self) -> &str {
        self.port.as_str()
    }
}

pub struct HostGraphLane<I> {
    pub(crate) route: DirectHostRoute,
    pub(crate) type_key: TypeKey,
    pub(crate) _input: PhantomData<I>,
}

impl HostGraphSubscription {
    pub fn try_recv_payload(&self) -> Option<Payload> {
        self.host.try_pop_payload(&self.port)
    }

    pub fn try_recv<T>(&self) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.host.try_pop(&self.port)
    }
}

/// A `run_once` input: `(port, value)` (key from the graph's type index) or
/// `(port, key, value)`.
pub trait HostGraphRunInput {
    type Value;

    fn into_parts(self, types: &TypeIndex) -> Result<(PortId, TypeKey, Self::Value), TypeKeyError>;
}

impl<P, I> HostGraphRunInput for (P, I)
where
    P: Into<PortId>,
    I: Send + Sync + 'static,
{
    type Value = I;

    fn into_parts(self, types: &TypeIndex) -> Result<(PortId, TypeKey, Self::Value), TypeKeyError> {
        Ok((self.0.into(), types.key_of::<I>()?, self.1))
    }
}

impl<P, K, I> HostGraphRunInput for (P, K, I)
where
    P: Into<PortId>,
    K: Into<TypeKey>,
    I: Send + Sync + 'static,
{
    type Value = I;

    fn into_parts(
        self,
        _types: &TypeIndex,
    ) -> Result<(PortId, TypeKey, Self::Value), TypeKeyError> {
        Ok((self.0.into(), self.1.into(), self.2))
    }
}

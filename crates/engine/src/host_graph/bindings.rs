use std::marker::PhantomData;

use daedalus_runtime::executor::DirectHostRoute;
use daedalus_runtime::handles::PortId;
use daedalus_runtime::host_bridge::HostBridgeHandle;
use daedalus_runtime::transport::typeexpr_transport_key;
use daedalus_transport::{FeedOutcome, Payload, TypeKey};

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
        self.host.feed_payload_ref(
            self.port.as_str(),
            Payload::owned(self.type_key.clone(), value),
        )
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
        self.host.feed_payload_ref(self.port.as_str(), payload)
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
        self.host.try_pop_owned::<T>(self.port.as_str())
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
        self.host.try_pop_payload(self.port.as_str())
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
        self.host.try_pop_payload(self.port.as_str())
    }

    pub fn try_recv<T>(&self) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.host.try_pop(self.port.as_str())
    }
}

pub trait HostGraphRunInput {
    type Value;

    fn into_parts(self) -> (PortId, TypeKey, Self::Value);
}

impl<I> HostGraphRunInput for (&str, I)
where
    I: Send + Sync + 'static,
{
    type Value = I;

    fn into_parts(self) -> (PortId, TypeKey, Self::Value) {
        (PortId::from(self.0), type_key_for::<I>(), self.1)
    }
}

impl<I> HostGraphRunInput for (String, I)
where
    I: Send + Sync + 'static,
{
    type Value = I;

    fn into_parts(self) -> (PortId, TypeKey, Self::Value) {
        (PortId::from(self.0), type_key_for::<I>(), self.1)
    }
}

impl<I> HostGraphRunInput for (PortId, I)
where
    I: Send + Sync + 'static,
{
    type Value = I;

    fn into_parts(self) -> (PortId, TypeKey, Self::Value) {
        (self.0, type_key_for::<I>(), self.1)
    }
}

impl<I, K> HostGraphRunInput for (&str, K, I)
where
    I: Send + Sync + 'static,
    K: Into<TypeKey>,
{
    type Value = I;

    fn into_parts(self) -> (PortId, TypeKey, Self::Value) {
        (PortId::from(self.0), self.1.into(), self.2)
    }
}

impl<I, K> HostGraphRunInput for (String, K, I)
where
    I: Send + Sync + 'static,
    K: Into<TypeKey>,
{
    type Value = I;

    fn into_parts(self) -> (PortId, TypeKey, Self::Value) {
        (PortId::from(self.0), self.1.into(), self.2)
    }
}

impl<I, K> HostGraphRunInput for (PortId, K, I)
where
    I: Send + Sync + 'static,
    K: Into<TypeKey>,
{
    type Value = I;

    fn into_parts(self) -> (PortId, TypeKey, Self::Value) {
        (self.0, self.1.into(), self.2)
    }
}

pub(crate) fn type_key_for<T: 'static>() -> TypeKey {
    typeexpr_transport_key(&daedalus_data::typing::type_expr::<T>())
        .unwrap_or_else(|_| TypeKey::new(std::any::type_name::<T>()))
}

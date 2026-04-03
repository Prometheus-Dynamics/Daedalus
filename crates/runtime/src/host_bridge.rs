use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::sync::{OnceLock, RwLock};
use std::task::Poll;

use crate::convert::convert_arc;
use crate::executor::{CorrelatedValue, NodeError, RuntimeValue, next_correlation_id};
use crate::io::NodeIo;
use crate::plan::EdgePolicyKind;
use daedalus_data::json;
use daedalus_data::model::{TypeExpr, Value, ValueType};
use daedalus_data::named_types::HostExportPolicy;
use daedalus_data::typing;
use futures_util::future::poll_fn;
use futures_util::task::AtomicWaker;

#[cfg(feature = "gpu")]
use daedalus_gpu::GpuContextHandle;

use image::DynamicImage;

/// Metadata key attached to host-bridge descriptors to mark them for runtime wiring.
pub const HOST_BRIDGE_META_KEY: &str = "host_bridge";
/// Canonical registry id for the host-bridge node.
pub const HOST_BRIDGE_ID: &str = "io.host_bridge";

/// Shared buffers for a single host bridge node. Host code pushes into `inbound`
/// (host -> graph), handler drains it and forwards into runtime edges; graph
/// outputs are collected into `outbound` for host consumption.
#[derive(Default)]
struct HostBridgeBuffers {
    inbound: HashMap<String, VecDeque<CorrelatedValue>>,
    outbound: HashMap<String, VecDeque<CorrelatedValue>>,
    wakers: HashMap<String, Arc<AtomicWaker>>,
}

/// Handle for interacting with a specific host bridge node.
#[derive(Clone)]
pub struct HostBridgeHandle {
    alias: String,
    shared: Arc<Mutex<HostBridgeBuffers>>,
    outgoing: HashMap<String, EdgePolicyKind>,
    outgoing_types: HashMap<String, TypeExpr>,
    incoming_types: HashMap<String, TypeExpr>,
    #[cfg(feature = "gpu")]
    gpu: Arc<Mutex<Option<GpuContextHandle>>>,
}

#[cfg(feature = "gpu")]
fn payload_any_type(any: &dyn Any) -> Option<&'static str> {
    if any.is::<DynamicImage>() {
        return Some(std::any::type_name::<DynamicImage>());
    }
    if any.is::<image::GrayImage>() {
        return Some(std::any::type_name::<image::GrayImage>());
    }
    if any.is::<image::GrayAlphaImage>() {
        return Some(std::any::type_name::<image::GrayAlphaImage>());
    }
    if any.is::<image::RgbImage>() {
        return Some(std::any::type_name::<image::RgbImage>());
    }
    if any.is::<image::RgbaImage>() {
        return Some(std::any::type_name::<image::RgbaImage>());
    }
    if any.is::<Arc<DynamicImage>>() {
        return Some(std::any::type_name::<Arc<DynamicImage>>());
    }
    if any.is::<Arc<image::GrayImage>>() {
        return Some(std::any::type_name::<Arc<image::GrayImage>>());
    }
    if any.is::<Arc<image::GrayAlphaImage>>() {
        return Some(std::any::type_name::<Arc<image::GrayAlphaImage>>());
    }
    if any.is::<Arc<image::RgbImage>>() {
        return Some(std::any::type_name::<Arc<image::RgbImage>>());
    }
    if any.is::<Arc<image::RgbaImage>>() {
        return Some(std::any::type_name::<Arc<image::RgbaImage>>());
    }
    if any.is::<daedalus_gpu::Backing<DynamicImage>>() {
        return Some(std::any::type_name::<daedalus_gpu::Backing<DynamicImage>>());
    }
    if any.is::<daedalus_gpu::Backing<image::GrayImage>>() {
        return Some(std::any::type_name::<daedalus_gpu::Backing<image::GrayImage>>());
    }
    if any.is::<daedalus_gpu::Backing<image::RgbImage>>() {
        return Some(std::any::type_name::<daedalus_gpu::Backing<image::RgbImage>>());
    }
    if any.is::<daedalus_gpu::Backing<image::RgbaImage>>() {
        return Some(std::any::type_name::<daedalus_gpu::Backing<image::RgbaImage>>());
    }
    if any.is::<daedalus_gpu::Compute<DynamicImage>>() {
        return Some(std::any::type_name::<daedalus_gpu::Compute<DynamicImage>>());
    }
    if any.is::<daedalus_gpu::Compute<image::GrayImage>>() {
        return Some(std::any::type_name::<daedalus_gpu::Compute<image::GrayImage>>());
    }
    if any.is::<daedalus_gpu::Compute<image::RgbImage>>() {
        return Some(std::any::type_name::<daedalus_gpu::Compute<image::RgbImage>>());
    }
    if any.is::<daedalus_gpu::Compute<image::RgbaImage>>() {
        return Some(std::any::type_name::<daedalus_gpu::Compute<image::RgbaImage>>());
    }
    if any.is::<daedalus_gpu::DataCell>() {
        return Some(std::any::type_name::<daedalus_gpu::DataCell>());
    }
    None
}

#[cfg(not(feature = "gpu"))]
fn payload_any_type(_any: &dyn Any) -> Option<&'static str> {
    None
}

fn cross_dylib_any_ref<'a, T: 'static>(any: &'a dyn Any) -> Option<&'a T> {
    let expected = std::any::type_name_of_val(any);
    let actual = std::any::type_name::<T>();
    if expected != actual && !expected.ends_with(actual) && !actual.ends_with(expected) {
        return None;
    }
    let size_ok = std::mem::size_of_val(any) == std::mem::size_of::<T>();
    let align_ok = std::mem::align_of_val(any) == std::mem::align_of::<T>();
    if !(size_ok && align_ok) {
        return None;
    }
    let (data_ptr, _): (*const (), *const ()) = unsafe { std::mem::transmute(any) };
    Some(unsafe { &*(data_ptr as *const T) })
}

fn unwrap_nested_any<'a>(mut any: &'a (dyn Any + 'static)) -> &'a (dyn Any + 'static) {
    loop {
        if let Some(inner) = any.downcast_ref::<Arc<dyn Any>>() {
            any = inner.as_ref();
            continue;
        }
        if let Some(inner) = any.downcast_ref::<Box<dyn Any>>() {
            any = inner.as_ref();
            continue;
        }
        if let Some(inner) = any.downcast_ref::<Arc<Box<dyn Any>>>() {
            any = inner.as_ref().as_ref();
            continue;
        }
        if let Some(inner) = any.downcast_ref::<Box<Arc<dyn Any>>>() {
            any = inner.as_ref().as_ref();
            continue;
        }
        if let Some(inner) = any.downcast_ref::<Arc<dyn Any + Send + Sync>>() {
            any = inner.as_ref();
            continue;
        }
        if let Some(inner) = any.downcast_ref::<Box<dyn Any + Send + Sync>>() {
            any = inner.as_ref();
            continue;
        }
        if let Some(inner) = any.downcast_ref::<Arc<Box<dyn Any + Send + Sync>>>() {
            any = inner.as_ref().as_ref();
            continue;
        }
        if let Some(inner) = any.downcast_ref::<Box<Arc<dyn Any + Send + Sync>>>() {
            any = inner.as_ref().as_ref();
            continue;
        }
        return any;
    }
}

#[cfg(feature = "gpu")]
fn any_is_gpu_image_handle(any: &(dyn Any + 'static)) -> bool {
    unwrap_nested_any(any)
        .downcast_ref::<daedalus_gpu::GpuImageHandle>()
        .is_some()
}

fn cross_dylib_arc_downcast_direct<T: Any + Send + Sync + 'static>(
    arc: Arc<dyn Any + Send + Sync>,
) -> Option<Arc<T>> {
    let any = arc.as_ref();
    // Only reinterpret direct `Arc<T>` allocations. Nested wrappers are handled by the
    // existing `Arc<Arc<T>>` / nested-`Any` branches below.
    if std::any::type_name_of_val(any) != std::any::type_name_of_val(unwrap_nested_any(any)) {
        return None;
    }
    cross_dylib_any_ref::<T>(any)?;
    let raw: *const (dyn Any + Send + Sync) = Arc::into_raw(arc);
    let (data_ptr, _vtable): (*const (), *const ()) = unsafe { std::mem::transmute(raw) };
    Some(unsafe { Arc::from_raw(data_ptr as *const T) })
}

impl HostBridgeHandle {
    fn new(
        alias: String,
        shared: Arc<Mutex<HostBridgeBuffers>>,
        outgoing: HashMap<String, EdgePolicyKind>,
        outgoing_types: HashMap<String, TypeExpr>,
        incoming_types: HashMap<String, TypeExpr>,
        #[cfg(feature = "gpu")] gpu: Arc<Mutex<Option<GpuContextHandle>>>,
    ) -> Self {
        Self {
            alias,
            shared,
            outgoing,
            outgoing_types,
            incoming_types,
            #[cfg(feature = "gpu")]
            gpu,
        }
    }

    /// Push a payload for a given output port on the bridge (host -> graph).
    pub fn push(
        &self,
        port: impl AsRef<str>,
        payload: RuntimeValue,
        correlation_id: Option<u64>,
    ) -> u64 {
        let port = port.as_ref().to_ascii_lowercase();
        if let RuntimeValue::Any(any) = &payload
            && let Some(ty) = payload_any_type(any.as_ref())
        {
            log::debug!(
                "host bridge port {}: accepting Any payload type {}",
                port,
                ty
            );
        }
        let id = correlation_id.unwrap_or_else(next_correlation_id);
        let mut guard = self.shared.lock().expect("host bridge poisoned");
        let q = guard.inbound.entry(port.clone()).or_default();
        let cap = host_bridge_inbound_cap();
        if cap > 0 && q.len() >= cap {
            q.pop_front();
        }
        q.push_back(CorrelatedValue {
            correlation_id: id,
            inner: payload,
            enqueued_at: std::time::Instant::now(),
        });
        if let Some(waker) = guard.wakers.get(&port) {
            waker.wake();
        }
        id
    }

    fn restore_outbound(&self, port: &str, payload: CorrelatedValue) {
        if let Ok(mut buf) = self.shared.lock() {
            let key = port.to_ascii_lowercase();
            let q = buf.outbound.entry(key.clone()).or_default();
            q.push_front(payload);
            if let Some(waker) = buf.wakers.get(&key) {
                waker.wake();
            }
        }
    }

    /// Push any typed payload (auto-wrapped).
    pub fn push_any<T: Any + Send + Sync + 'static>(&self, port: impl AsRef<str>, value: T) -> u64 {
        self.push(port, RuntimeValue::Any(Arc::new(value)), None)
    }

    /// Push a serialized payload, decoding it into a runtime payload first.
    pub fn push_serialized(
        &self,
        port: impl AsRef<str>,
        payload: HostBridgeSerialized,
        correlation_id: Option<u64>,
    ) -> Result<u64, NodeError> {
        let port = port.as_ref().to_ascii_lowercase();
        let port_type = self.outgoing_types.get(&port);
        let edge_payload = deserialize_serialized_payload(&port, port_type, payload)?;
        Ok(self.push(&port, edge_payload, correlation_id))
    }

    /// Async wait for the next payload on a port.
    pub async fn recv(&self, port: impl AsRef<str>) -> Option<CorrelatedValue> {
        let port = port.as_ref().to_ascii_lowercase();
        poll_fn(|cx| {
            let mut guard = self.shared.lock().expect("host bridge poisoned");
            if let Some(q) = guard.outbound.get_mut(&port)
                && let Some(item) = q.pop_front()
            {
                return Poll::Ready(Some(item));
            }
            let waker = guard
                .wakers
                .entry(port.clone())
                .or_insert_with(|| Arc::new(AtomicWaker::new()))
                .clone();
            waker.register(cx.waker());
            Poll::Pending
        })
        .await
    }

    /// Async wait for the next payload on a port and serialize it.
    pub async fn recv_serialized(
        &self,
        port: impl AsRef<str>,
    ) -> Result<Option<HostBridgeSerializedValue>, NodeError> {
        let port = port.as_ref().to_ascii_lowercase();
        let payload = self.recv(&port).await;
        match payload {
            Some(p) => {
                serialize_outbound_payload(&port, self.incoming_types.get(&port), p).map(Some)
            }
            None => Ok(None),
        }
    }

    /// Pop and downcast a single payload from the outbound queue for a port.
    pub fn pull_any<T: Any + Clone>(&self, port: impl AsRef<str>) -> Option<T> {
        let port = port.as_ref().to_ascii_lowercase();
        let mut guard = self.shared.lock().ok()?;
        let payload = guard.outbound.get_mut(&port)?.pop_front()?;
        match payload.inner {
            RuntimeValue::Any(a) => {
                let any = unwrap_nested_any(a.as_ref());
                any.downcast_ref::<T>()
                    .or_else(|| cross_dylib_any_ref::<T>(any))
                    .cloned()
            }
            _ => None,
        }
    }

    /// Try to pop a single payload emitted by the graph on the given port.
    pub fn try_pop(&self, port: impl AsRef<str>) -> Option<CorrelatedValue> {
        let port = port.as_ref().to_ascii_lowercase();
        let mut guard = self.shared.lock().expect("host bridge poisoned");
        guard.outbound.get_mut(&port).and_then(|q| q.pop_front())
    }

    /// Try to pop and serialize a single payload emitted by the graph.
    pub fn try_pop_serialized(
        &self,
        port: impl AsRef<str>,
    ) -> Result<Option<HostBridgeSerializedValue>, NodeError> {
        let port = port.as_ref().to_ascii_lowercase();
        let payload = self.try_pop(&port);
        match payload {
            Some(p) => {
                serialize_outbound_payload(&port, self.incoming_types.get(&port), p).map(Some)
            }
            None => Ok(None),
        }
    }

    /// Drain all pending payloads for the given port.
    pub fn drain(&self, port: impl AsRef<str>) -> Vec<CorrelatedValue> {
        let port = port.as_ref().to_ascii_lowercase();
        let mut guard = self.shared.lock().expect("host bridge poisoned");
        guard
            .outbound
            .remove(&port)
            .map(|q| q.into_iter().collect())
            .unwrap_or_default()
    }

    /// Clear all pending payloads for the given port without materializing them.
    pub fn clear(&self, port: impl AsRef<str>) -> usize {
        let port = port.as_ref().to_ascii_lowercase();
        let mut guard = self.shared.lock().expect("host bridge poisoned");
        if let Some(queue) = guard.outbound.get_mut(&port) {
            let len = queue.len();
            queue.clear();
            len
        } else {
            0
        }
    }

    /// Drain and serialize all pending payloads for the given port.
    pub fn drain_serialized(
        &self,
        port: impl AsRef<str>,
    ) -> Result<Vec<HostBridgeSerializedValue>, NodeError> {
        let port = port.as_ref().to_ascii_lowercase();
        let drained = self.drain(&port);
        drained
            .into_iter()
            .map(|p| serialize_outbound_payload(&port, self.incoming_types.get(&port), p))
            .collect()
    }

    /// Ports supported by this bridge (outgoing from host into the graph).
    pub fn ports(&self) -> impl Iterator<Item = &str> {
        self.outgoing.keys().map(|k| k.as_str())
    }

    /// Alias for `ports()` to make direction explicit (`host -> graph`).
    pub fn outgoing_ports(&self) -> impl Iterator<Item = &str> {
        self.ports()
    }

    /// Return a snapshot of known graph->host port names.
    ///
    /// This uses both:
    /// - inferred port types from the planner (preferred)
    /// - any ports that have appeared in the outbound queues
    pub fn incoming_port_names(&self) -> Vec<String> {
        let mut out: Vec<String> = self.incoming_types.keys().cloned().collect();
        if let Ok(guard) = self.shared.lock() {
            out.extend(guard.outbound.keys().cloned());
        }
        out.sort();
        out.dedup();
        out
    }

    /// Iterate all known graph->host ports as `HostPortOwned` values.
    ///
    /// This is the ergonomic path for host consumers:
    ///
    /// ```ignore
    /// for port in host.incoming_ports().filter(|p| p.can_type_to::<Value>()) {
    ///   if let Some((_corr, value)) = port.try_pop::<Value>()? { /* ... */ }
    /// }
    /// ```
    pub fn incoming_ports(&self) -> HostPortOwnedIter<'_> {
        HostPortOwnedIter {
            handle: self,
            names: self.incoming_port_names(),
            idx: 0,
        }
    }

    /// Iterate over a provided list of graph->host port names as "typed ports" that can be
    /// filtered and popped in an idiomatic Rust style.
    pub fn iter_ports<'a>(&'a self, ports: &'a [String]) -> HostPorts<'a> {
        HostPorts {
            handle: self,
            ports,
            idx: 0,
        }
    }

    /// Returns the node alias associated with this bridge.
    pub fn alias(&self) -> &str {
        &self.alias
    }

    pub fn outgoing_port_type(&self, port: impl AsRef<str>) -> Option<&TypeExpr> {
        let port = port.as_ref().to_ascii_lowercase();
        self.outgoing_types.get(&port)
    }

    pub fn incoming_port_type(&self, port: impl AsRef<str>) -> Option<&TypeExpr> {
        let port = port.as_ref().to_ascii_lowercase();
        self.incoming_types.get(&port)
    }

    #[cfg(feature = "gpu")]
    fn gpu_ctx(&self) -> Option<GpuContextHandle> {
        self.gpu.lock().ok().and_then(|g| g.as_ref().cloned())
    }

    /// Try to pop a single payload for `port` and decode it into a `Value` when possible,
    /// without going through JSON string serialization.
    ///
    /// This is intended for "non-image" / structured outputs that hosts want to sample cheaply.
    /// If the payload is not value-like (e.g. an image payload stored in `Any`), this returns an error.
    pub fn try_pop_value(&self, port: impl AsRef<str>) -> Result<Option<(u64, Value)>, NodeError> {
        let port = port.as_ref().to_ascii_lowercase();
        let Some(payload) = self.try_pop(&port) else {
            return Ok(None);
        };
        let corr = payload.correlation_id;
        let value = match payload.inner {
            RuntimeValue::Unit => Value::Unit,
            RuntimeValue::Bytes(bytes) => Value::Bytes(bytes.to_vec().into()),
            RuntimeValue::Value(value) => value,
            RuntimeValue::Any(any) => any_to_value(any.as_ref()).ok_or_else(|| {
                NodeError::InvalidInput(format!(
                    "host bridge port {port}: payload is not value-like"
                ))
            })?,
            #[cfg(feature = "gpu")]
            RuntimeValue::Data(_) => {
                return Err(NodeError::InvalidInput(format!(
                    "host bridge port {port}: gpu payloads are not value-like"
                )));
            }
        };
        Ok(Some((corr, value)))
    }

    /// Drain all pending payloads for `port`, decoding each into a `Value` when possible.
    ///
    /// Any payload that cannot be represented as a `Value` produces an error.
    pub fn drain_values(&self, port: impl AsRef<str>) -> Result<Vec<(u64, Value)>, NodeError> {
        let port = port.as_ref().to_ascii_lowercase();
        let drained = self.drain(&port);
        drained
            .into_iter()
            .map(|p| {
                let corr = p.correlation_id;
                let value = match p.inner {
                    RuntimeValue::Unit => Value::Unit,
                    RuntimeValue::Bytes(bytes) => Value::Bytes(bytes.to_vec().into()),
                    RuntimeValue::Value(value) => value,
                    RuntimeValue::Any(any) => any_to_value(any.as_ref()).ok_or_else(|| {
                        NodeError::InvalidInput(format!(
                            "host bridge port {port}: payload is not value-like"
                        ))
                    })?,
                    #[cfg(feature = "gpu")]
                    RuntimeValue::Data(_) => {
                        return Err(NodeError::InvalidInput(format!(
                            "host bridge port {port}: gpu payloads are not value-like"
                        )));
                    }
                };
                Ok((corr, value))
            })
            .collect()
    }
}

/// Iterator adapter returned by `HostBridgeHandle::incoming_ports`.
pub struct HostPortOwnedIter<'a> {
    handle: &'a HostBridgeHandle,
    names: Vec<String>,
    idx: usize,
}

impl<'a> Iterator for HostPortOwnedIter<'a> {
    type Item = HostPortOwned<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let name = self.names.get(self.idx)?.clone();
        self.idx += 1;
        Some(HostPortOwned {
            handle: self.handle,
            name,
        })
    }
}

/// A single graph->host port, owning its name (useful for iterator/filter use).
pub struct HostPortOwned<'a> {
    handle: &'a HostBridgeHandle,
    name: String,
}

impl<'a> HostPortOwned<'a> {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn resolved_type(&self) -> Option<&TypeExpr> {
        self.handle.incoming_port_type(&self.name)
    }

    /// Alias for `can_poll` (terminology: "can this port be typed/popped as T?").
    pub fn can_type_to<T: HostPollable>(&self) -> bool {
        self.can_poll::<T>()
    }

    pub fn can_poll<T: HostPollable>(&self) -> bool {
        T::can_poll(self.resolved_type())
    }

    pub fn is_type_expr(&self, ty: &TypeExpr) -> bool {
        self.resolved_type().is_some_and(|t| t == ty)
    }

    pub fn try_pop<T: HostPollable>(&self) -> Result<Option<(u64, T)>, NodeError> {
        let Some(payload) = self.handle.try_pop(self.name()) else {
            return Ok(None);
        };
        let corr = payload.correlation_id;
        match T::decode(
            self.handle,
            self.name(),
            self.resolved_type(),
            payload.clone(),
        ) {
            Ok(value) => Ok(Some((corr, value))),
            Err(err) => {
                self.handle.restore_outbound(self.name(), payload);
                Err(err)
            }
        }
    }

    /// Pop a raw correlated payload without decoding it.
    pub fn try_pop_raw(&self) -> Option<CorrelatedValue> {
        self.handle.try_pop(self.name())
    }

    /// Restore a raw correlated payload previously popped from this port.
    pub fn restore_raw(&self, payload: CorrelatedValue) {
        self.handle.restore_outbound(self.name(), payload);
    }

    /// Pop and attempt to downcast directly from `Any` without going through `Value`.
    pub fn try_pop_any<T: Any + Clone>(&self) -> Option<(u64, T)> {
        let payload = self.handle.try_pop(self.name())?;
        let corr = payload.correlation_id;
        match &payload.inner {
            RuntimeValue::Any(a) => {
                let any = unwrap_nested_any(a.as_ref());
                let value = a
                    .downcast_ref::<T>()
                    .or_else(|| cross_dylib_any_ref::<T>(a.as_ref()))
                    .or_else(|| any.downcast_ref::<T>())
                    .or_else(|| cross_dylib_any_ref::<T>(any))
                    .cloned();
                #[cfg(feature = "gpu")]
                let value = value.or_else(|| {
                    a.downcast_ref::<daedalus_gpu::Backing<T>>()
                        .map(|backing| backing.clone().into_owned())
                        .or_else(|| {
                            any.downcast_ref::<daedalus_gpu::Backing<T>>()
                                .map(|backing| backing.clone().into_owned())
                        })
                });
                if value.is_none() {
                    log::warn!(
                        "host bridge port {}: try_pop_any failed expected={} actual={}",
                        self.name(),
                        std::any::type_name::<T>(),
                        std::any::type_name_of_val(any)
                    );
                    self.handle.restore_outbound(self.name(), payload);
                }
                value.map(|v| (corr, v))
            }
            _ => {
                self.handle.restore_outbound(self.name(), payload);
                None
            }
        }
    }

    /// Pop an `Any` payload as a shared `Arc<T>` without cloning the underlying value.
    pub fn try_pop_any_arc<T: Any + Send + Sync + 'static>(&self) -> Option<(u64, Arc<T>)> {
        let payload = self.handle.try_pop(self.name())?;
        let corr = payload.correlation_id;
        match &payload.inner {
            RuntimeValue::Any(a) => {
                let any = unwrap_nested_any(a.as_ref());
                let value = Arc::downcast::<T>(a.clone())
                    .ok()
                    .or_else(|| cross_dylib_arc_downcast_direct::<T>(a.clone()))
                    .or_else(|| {
                        Arc::downcast::<Arc<T>>(a.clone())
                            .ok()
                            .map(|arc| (*arc).clone())
                    })
                    .or_else(|| any.downcast_ref::<Arc<T>>().cloned())
                    .or_else(|| cross_dylib_any_ref::<Arc<T>>(a.as_ref()).cloned())
                    .or_else(|| cross_dylib_any_ref::<Arc<T>>(any).cloned());
                #[cfg(feature = "gpu")]
                let value = value.or_else(|| {
                    a.downcast_ref::<daedalus_gpu::Backing<T>>()
                        .and_then(daedalus_gpu::Backing::shared_arc)
                        .or_else(|| {
                            any.downcast_ref::<daedalus_gpu::Backing<T>>()
                                .and_then(daedalus_gpu::Backing::shared_arc)
                        })
                });
                if value.is_none() {
                    log::warn!(
                        "host bridge port {}: try_pop_any_arc failed expected={} actual={}",
                        self.name(),
                        std::any::type_name::<Arc<T>>(),
                        std::any::type_name_of_val(any)
                    );
                    self.handle.restore_outbound(self.name(), payload);
                }
                value.map(|v| (corr, v))
            }
            _ => {
                self.handle.restore_outbound(self.name(), payload);
                None
            }
        }
    }
}

/// Iterator adapter returned by `HostBridgeHandle::iter_ports`.
pub struct HostPorts<'a> {
    handle: &'a HostBridgeHandle,
    ports: &'a [String],
    idx: usize,
}

impl<'a> Iterator for HostPorts<'a> {
    type Item = HostPort<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let port = self.ports.get(self.idx)?.as_str();
        self.idx += 1;
        Some(HostPort {
            handle: self.handle,
            name: port,
        })
    }
}

/// A single graph->host port, providing typed polling and convenience helpers.
#[derive(Clone, Copy)]
pub struct HostPort<'a> {
    handle: &'a HostBridgeHandle,
    name: &'a str,
}

impl<'a> HostPort<'a> {
    pub fn name(&self) -> &'a str {
        self.name
    }

    /// The resolved graph->host port type (after planner inference), if known.
    pub fn resolved_type(&self) -> Option<&'a TypeExpr> {
        self.handle.incoming_port_type(self.name)
    }

    /// Alias for `can_poll` (terminology: "can this port be typed/popped as T?").
    pub fn can_type_to<T: HostPollable>(&self) -> bool {
        self.can_poll::<T>()
    }

    /// Returns `true` if this port can be pulled as `T` without additional user glue.
    pub fn can_poll<T: HostPollable>(&self) -> bool {
        T::can_poll(self.resolved_type())
    }

    pub fn is_type_expr(&self, ty: &TypeExpr) -> bool {
        self.resolved_type().is_some_and(|t| t == ty)
    }

    /// Try to pop a single value from this port and decode it as `T`.
    pub fn try_pop<T: HostPollable>(&self) -> Result<Option<(u64, T)>, NodeError> {
        let Some(payload) = self.handle.try_pop(self.name) else {
            return Ok(None);
        };
        let corr = payload.correlation_id;
        match T::decode(
            self.handle,
            self.name,
            self.resolved_type(),
            payload.clone(),
        ) {
            Ok(value) => Ok(Some((corr, value))),
            Err(err) => {
                self.handle.restore_outbound(self.name, payload);
                Err(err)
            }
        }
    }

    /// Pop a raw correlated payload without decoding it.
    pub fn try_pop_raw(&self) -> Option<CorrelatedValue> {
        self.handle.try_pop(self.name)
    }

    /// Restore a raw correlated payload previously popped from this port.
    pub fn restore_raw(&self, payload: CorrelatedValue) {
        self.handle.restore_outbound(self.name, payload);
    }

    /// Pop and attempt to downcast directly from `Any` without going through `Value`.
    pub fn try_pop_any<T: Any + Clone>(&self) -> Option<(u64, T)> {
        let payload = self.handle.try_pop(self.name)?;
        let corr = payload.correlation_id;
        match &payload.inner {
            RuntimeValue::Any(a) => {
                let any = unwrap_nested_any(a.as_ref());
                let value = a
                    .downcast_ref::<T>()
                    .or_else(|| cross_dylib_any_ref::<T>(a.as_ref()))
                    .or_else(|| any.downcast_ref::<T>())
                    .or_else(|| cross_dylib_any_ref::<T>(any))
                    .cloned();
                #[cfg(feature = "gpu")]
                let value = value.or_else(|| {
                    a.downcast_ref::<daedalus_gpu::Backing<T>>()
                        .map(|backing| backing.clone().into_owned())
                        .or_else(|| {
                            any.downcast_ref::<daedalus_gpu::Backing<T>>()
                                .map(|backing| backing.clone().into_owned())
                        })
                });
                if value.is_none() {
                    log::warn!(
                        "host bridge port {}: try_pop_any failed expected={} actual={}",
                        self.name,
                        std::any::type_name::<T>(),
                        std::any::type_name_of_val(any)
                    );
                    self.handle.restore_outbound(self.name, payload);
                }
                value.map(|v| (corr, v))
            }
            _ => {
                self.handle.restore_outbound(self.name, payload);
                None
            }
        }
    }

    /// Pop an `Any` payload as a shared `Arc<T>` without cloning the underlying value.
    pub fn try_pop_any_arc<T: Any + Send + Sync + 'static>(&self) -> Option<(u64, Arc<T>)> {
        let payload = self.handle.try_pop(self.name)?;
        let corr = payload.correlation_id;
        match &payload.inner {
            RuntimeValue::Any(a) => {
                let any = unwrap_nested_any(a.as_ref());
                let value = Arc::downcast::<T>(a.clone())
                    .ok()
                    .or_else(|| cross_dylib_arc_downcast_direct::<T>(a.clone()))
                    .or_else(|| {
                        Arc::downcast::<Arc<T>>(a.clone())
                            .ok()
                            .map(|arc| (*arc).clone())
                    })
                    .or_else(|| any.downcast_ref::<Arc<T>>().cloned())
                    .or_else(|| cross_dylib_any_ref::<Arc<T>>(a.as_ref()).cloned())
                    .or_else(|| cross_dylib_any_ref::<Arc<T>>(any).cloned());
                #[cfg(feature = "gpu")]
                let value = value.or_else(|| {
                    a.downcast_ref::<daedalus_gpu::Backing<T>>()
                        .and_then(daedalus_gpu::Backing::shared_arc)
                        .or_else(|| {
                            any.downcast_ref::<daedalus_gpu::Backing<T>>()
                                .and_then(daedalus_gpu::Backing::shared_arc)
                        })
                });
                if value.is_none() {
                    log::warn!(
                        "host bridge port {}: try_pop_any_arc failed expected={} actual={}",
                        self.name,
                        std::any::type_name::<Arc<T>>(),
                        std::any::type_name_of_val(any)
                    );
                    self.handle.restore_outbound(self.name, payload);
                }
                value.map(|v| (corr, v))
            }
            _ => {
                self.handle.restore_outbound(self.name, payload);
                None
            }
        }
    }
}

/// Trait used by `HostPort::can_poll::<T>()` and `HostPort::try_pop::<T>()`.
pub trait HostPollable: Sized {
    fn can_poll(port_type: Option<&TypeExpr>) -> bool;

    fn decode(
        handle: &HostBridgeHandle,
        port: &str,
        port_type: Option<&TypeExpr>,
        payload: CorrelatedValue,
    ) -> Result<Self, NodeError>;
}

impl HostPollable for Value {
    fn can_poll(_port_type: Option<&TypeExpr>) -> bool {
        true
    }

    fn decode(
        _handle: &HostBridgeHandle,
        port: &str,
        _port_type: Option<&TypeExpr>,
        payload: CorrelatedValue,
    ) -> Result<Self, NodeError> {
        match payload.inner {
            RuntimeValue::Unit => Ok(Value::Unit),
            RuntimeValue::Bytes(bytes) => Ok(Value::Bytes(bytes.to_vec().into())),
            RuntimeValue::Value(value) => Ok(value),
            RuntimeValue::Any(any) => any_to_value(any.as_ref()).ok_or_else(|| {
                let ty = std::any::type_name_of_val(any.as_ref());
                log::warn!(
                    "host bridge port {}: Any payload not value-like (type={})",
                    port,
                    ty
                );
                NodeError::InvalidInput(format!(
                    "host bridge port {port}: payload is not value-like (type={ty})"
                ))
            }),
            #[cfg(feature = "gpu")]
            RuntimeValue::Data(_) => Err(NodeError::InvalidInput(format!(
                "host bridge port {port}: gpu payloads are not value-like"
            ))),
        }
    }
}

impl HostPollable for DynamicImage {
    fn can_poll(port_type: Option<&TypeExpr>) -> bool {
        let Some(port_type) = port_type else {
            return false;
        };
        let target = typing::lookup_type::<DynamicImage>()
            .unwrap_or_else(|| TypeExpr::opaque("image:dynamic"));
        port_type == &target || typing::can_convert_typeexpr(port_type, &target)
    }

    fn decode(
        handle: &HostBridgeHandle,
        port: &str,
        _port_type: Option<&TypeExpr>,
        payload: CorrelatedValue,
    ) -> Result<Self, NodeError> {
        let _ = handle;
        match payload.inner {
            RuntimeValue::Any(any) => {
                // Prefer consuming the owned `Arc<T>` stored in `Any` so image host outputs can
                // move their buffers through the bridge without cloning full frames per pop.
                let any = match Arc::downcast::<DynamicImage>(any) {
                    Ok(img) => return Ok(Arc::unwrap_or_clone(img)),
                    Err(any) => any,
                };
                let any = match Arc::downcast::<image::RgbaImage>(any) {
                    Ok(img) => return Ok(DynamicImage::ImageRgba8(Arc::unwrap_or_clone(img))),
                    Err(any) => any,
                };
                let any = match Arc::downcast::<image::RgbImage>(any) {
                    Ok(img) => return Ok(DynamicImage::ImageRgb8(Arc::unwrap_or_clone(img))),
                    Err(any) => any,
                };
                let any = match Arc::downcast::<image::GrayImage>(any) {
                    Ok(img) => return Ok(DynamicImage::ImageLuma8(Arc::unwrap_or_clone(img))),
                    Err(any) => any,
                };
                let any = match Arc::downcast::<image::GrayAlphaImage>(any) {
                    Ok(img) => return Ok(DynamicImage::ImageLumaA8(Arc::unwrap_or_clone(img))),
                    Err(any) => any,
                };
                #[cfg(feature = "gpu")]
                let any = match Arc::downcast::<daedalus_gpu::Backing<DynamicImage>>(any) {
                    Ok(img) => return Ok(Arc::unwrap_or_clone(img).into_owned()),
                    Err(any) => any,
                };
                #[cfg(feature = "gpu")]
                let any = match Arc::downcast::<daedalus_gpu::Backing<image::GrayImage>>(any) {
                    Ok(img) => {
                        return Ok(DynamicImage::ImageLuma8(
                            Arc::unwrap_or_clone(img).into_owned(),
                        ));
                    }
                    Err(any) => any,
                };
                #[cfg(feature = "gpu")]
                let any = match Arc::downcast::<daedalus_gpu::Backing<image::RgbImage>>(any) {
                    Ok(img) => {
                        return Ok(DynamicImage::ImageRgb8(
                            Arc::unwrap_or_clone(img).into_owned(),
                        ));
                    }
                    Err(any) => any,
                };
                #[cfg(feature = "gpu")]
                let any = match Arc::downcast::<daedalus_gpu::Backing<image::RgbaImage>>(any) {
                    Ok(img) => {
                        return Ok(DynamicImage::ImageRgba8(
                            Arc::unwrap_or_clone(img).into_owned(),
                        ));
                    }
                    Err(any) => any,
                };

                let inner_any = unwrap_nested_any(any.as_ref());
                if let Some(img) = inner_any.downcast_ref::<DynamicImage>().cloned() {
                    return Ok(img);
                }
                if let Some(img) = inner_any.downcast_ref::<image::RgbaImage>() {
                    return Ok(DynamicImage::ImageRgba8(img.clone()));
                }
                if let Some(img) = inner_any.downcast_ref::<image::RgbImage>() {
                    return Ok(DynamicImage::ImageRgb8(img.clone()));
                }
                if let Some(img) = inner_any.downcast_ref::<image::GrayImage>() {
                    return Ok(DynamicImage::ImageLuma8(img.clone()));
                }
                if let Some(img) = inner_any.downcast_ref::<image::GrayAlphaImage>() {
                    return Ok(DynamicImage::ImageLumaA8(img.clone()));
                }
                if let Some(img) = inner_any.downcast_ref::<Arc<DynamicImage>>() {
                    return Ok((**img).clone());
                }
                if let Some(img) = inner_any.downcast_ref::<Arc<image::RgbaImage>>() {
                    return Ok(DynamicImage::ImageRgba8((**img).clone()));
                }
                if let Some(img) = inner_any.downcast_ref::<Arc<image::RgbImage>>() {
                    return Ok(DynamicImage::ImageRgb8((**img).clone()));
                }
                if let Some(img) = inner_any.downcast_ref::<Arc<image::GrayImage>>() {
                    return Ok(DynamicImage::ImageLuma8((**img).clone()));
                }
                if let Some(img) = inner_any.downcast_ref::<Arc<image::GrayAlphaImage>>() {
                    return Ok(DynamicImage::ImageLumaA8((**img).clone()));
                }
                #[cfg(feature = "gpu")]
                if let Some(img) = inner_any.downcast_ref::<daedalus_gpu::Backing<DynamicImage>>() {
                    return Ok(img.clone().into_owned());
                }
                #[cfg(feature = "gpu")]
                if let Some(img) =
                    inner_any.downcast_ref::<daedalus_gpu::Backing<image::GrayImage>>()
                {
                    return Ok(DynamicImage::ImageLuma8(img.clone().into_owned()));
                }
                #[cfg(feature = "gpu")]
                if let Some(img) =
                    inner_any.downcast_ref::<daedalus_gpu::Backing<image::RgbImage>>()
                {
                    return Ok(DynamicImage::ImageRgb8(img.clone().into_owned()));
                }
                #[cfg(feature = "gpu")]
                if let Some(img) =
                    inner_any.downcast_ref::<daedalus_gpu::Backing<image::RgbaImage>>()
                {
                    return Ok(DynamicImage::ImageRgba8(img.clone().into_owned()));
                }
                #[cfg(feature = "gpu")]
                {
                    // Accept `Compute<DynamicImage>` sent through `Any`.
                    if let Some(p) = inner_any
                        .downcast_ref::<daedalus_gpu::Compute<DynamicImage>>()
                        .cloned()
                    {
                        return match p {
                            daedalus_gpu::Compute::Cpu(img) => Ok(img),
                            daedalus_gpu::Compute::Gpu(h) => {
                                let ctx = handle.gpu_ctx().ok_or_else(|| {
                                    NodeError::InvalidInput(format!(
                                        "host bridge port {port}: gpu output requires a GPU context"
                                    ))
                                })?;
                                <DynamicImage as daedalus_gpu::DeviceBridge>::download(&h, &ctx)
                                    .map_err(|e| {
                                        NodeError::InvalidInput(format!(
                                            "host bridge port {port}: failed to download gpu image ({e})"
                                        ))
                                    })
                            }
                        };
                    }

                    // Accept `DataCell` carrying a CPU/GPU image.
                    if let Some(ep) = inner_any.downcast_ref::<daedalus_gpu::DataCell>().cloned() {
                        if let Some(cpu) = ep.clone_cpu::<DynamicImage>() {
                            return Ok(cpu);
                        }
                        if ep.is_gpu() {
                            let ctx = handle.gpu_ctx().ok_or_else(|| {
                                NodeError::InvalidInput(format!(
                                    "host bridge port {port}: gpu output requires a GPU context"
                                ))
                            })?;
                            if let Ok(downloaded) = ep.download(&ctx)
                                && let Some(cpu) = downloaded.as_cpu::<DynamicImage>().cloned()
                            {
                                return Ok(cpu);
                            }
                        }
                    }

                    // Accept a raw GPU handle sent through `Any`.
                    if let Some(h) = inner_any
                        .downcast_ref::<daedalus_gpu::GpuImageHandle>()
                        .cloned()
                    {
                        let ctx = handle.gpu_ctx().ok_or_else(|| {
                            NodeError::InvalidInput(format!(
                                "host bridge port {port}: gpu output requires a GPU context"
                            ))
                        })?;
                        return <DynamicImage as daedalus_gpu::DeviceBridge>::download(&h, &ctx)
                            .map_err(|e| {
                                NodeError::InvalidInput(format!(
                                    "host bridge port {port}: failed to download gpu image ({e})"
                                ))
                            });
                    }
                }
                if let Some(converted) = convert_arc::<DynamicImage>(&any) {
                    return Ok(converted);
                }
                let ty = std::any::type_name_of_val(inner_any);
                Err(NodeError::InvalidInput(format!(
                    "host bridge port {port}: Any payload is not a DynamicImage (type={ty})"
                )))
            }
            #[cfg(feature = "gpu")]
            RuntimeValue::Data(ep) => {
                if let Some(cpu) = ep.try_downcast_cpu_any::<daedalus_gpu::Backing<DynamicImage>>()
                {
                    return Ok(cpu.into_owned());
                }
                if let Some(cpu) =
                    ep.try_downcast_cpu_any::<daedalus_gpu::Backing<image::GrayImage>>()
                {
                    return Ok(DynamicImage::ImageLuma8(cpu.into_owned()));
                }
                if let Some(cpu) =
                    ep.try_downcast_cpu_any::<daedalus_gpu::Backing<image::RgbImage>>()
                {
                    return Ok(DynamicImage::ImageRgb8(cpu.into_owned()));
                }
                if let Some(cpu) =
                    ep.try_downcast_cpu_any::<daedalus_gpu::Backing<image::RgbaImage>>()
                {
                    return Ok(DynamicImage::ImageRgba8(cpu.into_owned()));
                }
                if let Some(cpu) = ep.clone_cpu::<DynamicImage>() {
                    return Ok(cpu);
                }
                if let Some(gpu) = ep.clone_gpu::<DynamicImage>() {
                    let ctx = handle.gpu_ctx().ok_or_else(|| {
                        NodeError::InvalidInput(format!(
                            "host bridge port {port}: gpu output requires a GPU context"
                        ))
                    })?;
                    return <DynamicImage as daedalus_gpu::DeviceBridge>::download(&gpu, &ctx)
                        .map_err(|e| {
                            NodeError::InvalidInput(format!(
                                "host bridge port {port}: failed to download gpu payload ({e})"
                            ))
                        });
                }
                Err(NodeError::InvalidInput(format!(
                    "host bridge port {port}: payload does not contain an image"
                )))
            }
            RuntimeValue::Unit | RuntimeValue::Bytes(_) => Err(NodeError::InvalidInput(format!(
                "host bridge port {port}: payload is not an image"
            ))),
            RuntimeValue::Value(_) => Err(NodeError::InvalidInput(format!(
                "host bridge port {port}: payload is not an image"
            ))),
        }
    }
}

#[cfg(feature = "gpu")]
impl HostPollable for daedalus_gpu::GpuImageHandle {
    fn can_poll(port_type: Option<&TypeExpr>) -> bool {
        // Host image ports are represented as `image:dynamic` in TypeExpr-land.
        matches!(port_type, Some(t) if *t == TypeExpr::opaque("image:dynamic"))
    }

    fn decode(
        _handle: &HostBridgeHandle,
        port: &str,
        _port_type: Option<&TypeExpr>,
        payload: CorrelatedValue,
    ) -> Result<Self, NodeError> {
        match payload.inner {
            RuntimeValue::Data(ep) => ep.clone_gpu::<DynamicImage>().ok_or_else(|| {
                NodeError::InvalidInput(format!(
                    "host bridge port {port}: payload does not contain a gpu image handle"
                ))
            }),
            RuntimeValue::Any(any) => unwrap_nested_any(any.as_ref())
                .downcast_ref::<daedalus_gpu::GpuImageHandle>()
                .cloned()
                .ok_or_else(|| {
                    NodeError::InvalidInput(format!(
                        "host bridge port {port}: Any payload is not a gpu image handle"
                    ))
                }),
            RuntimeValue::Unit | RuntimeValue::Bytes(_) => Err(NodeError::InvalidInput(format!(
                "host bridge port {port}: payload is not a gpu image handle"
            ))),
            RuntimeValue::Value(_) => Err(NodeError::InvalidInput(format!(
                "host bridge port {port}: payload is not a gpu image handle"
            ))),
        }
    }
}

#[cfg(feature = "gpu")]
impl<T> HostPollable for daedalus_gpu::Compute<T>
where
    T: daedalus_gpu::DeviceBridge + Clone + Send + Sync + 'static,
    T::Device: Clone + Send + Sync + 'static,
{
    fn can_poll(_port_type: Option<&TypeExpr>) -> bool {
        true
    }

    fn decode(
        handle: &HostBridgeHandle,
        port: &str,
        _port_type: Option<&TypeExpr>,
        payload: CorrelatedValue,
    ) -> Result<Self, NodeError> {
        match payload.inner {
            RuntimeValue::Data(ep) => {
                if let Some(cpu) = ep.try_downcast_cpu_any::<daedalus_gpu::Backing<T>>() {
                    return Ok(daedalus_gpu::Compute::Cpu(cpu.into_owned()));
                }
                if let Some(cpu) = ep.clone_cpu::<T>() {
                    return Ok(daedalus_gpu::Compute::Cpu(cpu));
                }
                if let Some(g) = ep.clone_gpu::<T>() {
                    return Ok(daedalus_gpu::Compute::Gpu(g));
                }
                Err(NodeError::InvalidInput(format!(
                    "host bridge port {port}: payload does not contain requested type"
                )))
            }
            RuntimeValue::Any(any) => {
                let inner_any = unwrap_nested_any(any.as_ref());
                if let Some(p) = inner_any
                    .downcast_ref::<daedalus_gpu::Compute<T>>()
                    .cloned()
                {
                    return Ok(p);
                }
                if let Some(cpu) = inner_any.downcast_ref::<T>().cloned() {
                    return Ok(daedalus_gpu::Compute::Cpu(cpu));
                }
                if let Some(cpu) = inner_any.downcast_ref::<daedalus_gpu::Backing<T>>() {
                    return Ok(daedalus_gpu::Compute::Cpu(cpu.clone().into_owned()));
                }
                if let Some(g) = inner_any.downcast_ref::<T::Device>().cloned() {
                    return Ok(daedalus_gpu::Compute::Gpu(g));
                }
                if let Some(ep) = inner_any.downcast_ref::<daedalus_gpu::DataCell>().cloned() {
                    if let Some(cpu) = ep.clone_cpu::<T>() {
                        return Ok(daedalus_gpu::Compute::Cpu(cpu));
                    }
                    if let Some(g) = ep.clone_gpu::<T>() {
                        return Ok(daedalus_gpu::Compute::Gpu(g));
                    }
                    if ep.is_gpu()
                        && let Some(ctx) = handle.gpu_ctx()
                        && let Ok(downloaded) = ep.download(&ctx)
                        && let Some(cpu) = downloaded.as_cpu::<T>()
                    {
                        return Ok(daedalus_gpu::Compute::Cpu(cpu.clone()));
                    }
                }
                Err(NodeError::InvalidInput(format!(
                    "host bridge port {port}: Any payload is not compatible with requested payload type"
                )))
            }
            RuntimeValue::Unit | RuntimeValue::Bytes(_) => Err(NodeError::InvalidInput(format!(
                "host bridge port {port}: payload is not compatible with requested payload type"
            ))),
            RuntimeValue::Value(_) => Err(NodeError::InvalidInput(format!(
                "host bridge port {port}: payload is not compatible with requested payload type"
            ))),
        }
    }
}

/// Shared value-pop behavior for `HostPort`-like items.
pub trait HostValuePort {
    fn name(&self) -> &str;
    fn try_pop_value(&self) -> Result<Option<(u64, Value)>, NodeError>;
}

impl<'a> HostValuePort for HostPort<'a> {
    fn name(&self) -> &str {
        self.name()
    }

    fn try_pop_value(&self) -> Result<Option<(u64, Value)>, NodeError> {
        self.try_pop::<Value>()
    }
}

impl<'a> HostValuePort for HostPortOwned<'a> {
    fn name(&self) -> &str {
        self.name()
    }

    fn try_pop_value(&self) -> Result<Option<(u64, Value)>, NodeError> {
        self.try_pop::<Value>()
    }
}

/// Iterator extensions over "host ports" (both `HostPort` and `HostPortOwned`).
pub trait HostPortIterExt: Iterator + Sized
where
    Self::Item: HostValuePort,
{
    fn try_pop_all_values(self) -> Result<Vec<(String, u64, Value)>, NodeError> {
        let mut out = Vec::new();
        for port in self {
            if let Some((corr, value)) = port.try_pop_value()? {
                out.push((port.name().to_string(), corr, value));
            }
        }
        Ok(out)
    }
}

impl<I> HostPortIterExt for I
where
    I: Iterator + Sized,
    I::Item: HostValuePort,
{
}

/// Collection of host bridge handles keyed by node alias.
#[derive(Clone, Default)]
pub struct HostBridgeManager {
    inner: Arc<Mutex<HashMap<String, Arc<Mutex<HostBridgeBuffers>>>>>,
    outgoing: Arc<Mutex<HashMap<String, HashMap<String, EdgePolicyKind>>>>,
    outgoing_types: Arc<Mutex<HashMap<String, HashMap<String, TypeExpr>>>>,
    incoming_types: Arc<Mutex<HashMap<String, HashMap<String, TypeExpr>>>>,
    outbound_filters: Arc<Mutex<HashMap<String, Option<std::collections::BTreeSet<String>>>>>,
    #[cfg(feature = "gpu")]
    gpu: Arc<Mutex<Option<GpuContextHandle>>>,
}

static HOST_BRIDGE_OUTBOUND_LOGS: AtomicU64 = AtomicU64::new(0);
static HOST_BRIDGE_OUTBOUND_DROPS: AtomicU64 = AtomicU64::new(0);

impl HostBridgeManager {
    /// Create an empty manager.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a GPU context handle so host-side polling can download GPU-resident payloads
    /// without requiring the caller to thread `GpuContextHandle` explicitly.
    #[cfg(feature = "gpu")]
    pub fn attach_gpu(&self, gpu: GpuContextHandle) {
        let mut guard = self.gpu.lock().expect("host bridge gpu lock poisoned");
        *guard = Some(gpu);
    }

    /// Register a host bridge node by alias with its outgoing ports and policies.
    pub fn register_bridge(
        &self,
        alias: impl Into<String>,
        ports: impl IntoIterator<Item = (String, EdgePolicyKind)>,
    ) {
        let alias = alias.into().to_ascii_lowercase();
        {
            let mut guard = self.inner.lock().expect("host bridge map poisoned");
            guard.entry(alias.clone()).or_default();
        }
        let mut guard = self.outgoing.lock().expect("host bridge ports poisoned");
        guard.insert(alias, ports.into_iter().collect());
    }

    /// Register port type metadata for a host bridge alias.
    pub fn register_port_types(
        &self,
        alias: impl Into<String>,
        outgoing: impl IntoIterator<Item = (String, TypeExpr)>,
        incoming: impl IntoIterator<Item = (String, TypeExpr)>,
    ) {
        let alias = alias.into().to_ascii_lowercase();
        {
            let mut guard = self.inner.lock().expect("host bridge map poisoned");
            guard.entry(alias.clone()).or_default();
        }
        let mut out_guard = self
            .outgoing_types
            .lock()
            .expect("host bridge types poisoned");
        let out_map = out_guard.entry(alias.clone()).or_default();
        for (port, ty) in outgoing {
            out_map.insert(port.to_ascii_lowercase(), ty);
        }

        let mut in_guard = self
            .incoming_types
            .lock()
            .expect("host bridge types poisoned");
        let in_map = in_guard.entry(alias).or_default();
        for (port, ty) in incoming {
            in_map.insert(port.to_ascii_lowercase(), ty);
        }
    }

    /// Restrict graph -> host publication for a bridge alias to a specific set of inbound ports.
    ///
    /// Passing `None` clears the filter and allows all inbound ports through.
    pub fn set_outbound_port_filter(
        &self,
        alias: impl Into<String>,
        ports: Option<std::collections::BTreeSet<String>>,
    ) {
        let alias = alias.into().to_ascii_lowercase();
        {
            let mut guard = self.inner.lock().expect("host bridge map poisoned");
            guard.entry(alias.clone()).or_default();
        }
        let normalized = ports.map(|ports| {
            ports
                .into_iter()
                .map(|port| port.trim().to_ascii_lowercase())
                .filter(|port| !port.is_empty())
                .collect::<std::collections::BTreeSet<_>>()
        });
        let mut guard = self
            .outbound_filters
            .lock()
            .expect("host bridge filter map poisoned");
        guard.insert(alias, normalized);
    }

    /// Build a manager from a runtime plan by detecting nodes tagged as host bridges.
    pub fn from_plan(plan: &crate::plan::RuntimePlan) -> Self {
        let mgr = Self::new();
        mgr.populate_from_plan(plan);
        mgr
    }

    /// Populate an existing manager based on the runtime plan.
    pub fn populate_from_plan(&self, plan: &crate::plan::RuntimePlan) {
        fn parse_type_map(value: Option<&Value>) -> Vec<(String, TypeExpr)> {
            let Some(Value::Map(entries)) = value else {
                return Vec::new();
            };
            let mut out = Vec::new();
            for (k, v) in entries {
                let (Value::String(port), Value::String(json)) = (k, v) else {
                    continue;
                };
                if let Ok(ty) = serde_json::from_str::<TypeExpr>(json) {
                    out.push((port.to_ascii_lowercase(), ty));
                }
            }
            out.sort_by(|a, b| a.0.cmp(&b.0));
            out
        }

        for (idx, node) in plan.nodes.iter().enumerate() {
            let is_bridge = matches!(
                node.metadata.get(HOST_BRIDGE_META_KEY),
                Some(Value::Bool(true))
            );
            if !is_bridge {
                continue;
            }
            let alias = node
                .label
                .as_deref()
                .unwrap_or(node.id.as_str())
                .to_ascii_lowercase();
            let outgoing_types = parse_type_map(node.metadata.get("dynamic_output_types"));
            let incoming_types = parse_type_map(node.metadata.get("dynamic_input_types"));
            // Gather ports where this node is the source (host -> graph).
            let mut outgoing: HashMap<String, EdgePolicyKind> = HashMap::new();
            for (from, from_port, _, _, policy) in plan.edges.iter() {
                if from.0 == idx {
                    outgoing.insert(from_port.to_ascii_lowercase(), policy.clone());
                }
            }
            self.register_bridge(alias.clone(), outgoing.into_iter());
            self.register_port_types(alias, outgoing_types, incoming_types);
        }
    }

    /// Lookup a handle for the given alias, if present.
    pub fn handle(&self, alias: impl AsRef<str>) -> Option<HostBridgeHandle> {
        let alias = alias.as_ref().to_ascii_lowercase();
        let shared = {
            let guard = self.inner.lock().ok()?;
            guard.get(&alias)?.clone()
        };
        let outgoing = {
            let guard = self.outgoing.lock().ok()?;
            guard.get(&alias)?.clone()
        };
        let outgoing_types = {
            let guard = self.outgoing_types.lock().ok()?;
            guard.get(&alias).cloned().unwrap_or_default()
        };
        let incoming_types = {
            let guard = self.incoming_types.lock().ok()?;
            guard.get(&alias).cloned().unwrap_or_default()
        };
        Some(HostBridgeHandle::new(
            alias,
            shared,
            outgoing,
            outgoing_types,
            incoming_types,
            #[cfg(feature = "gpu")]
            self.gpu.clone(),
        ))
    }

    /// Internal helper: record an outbound payload for host consumption.
    fn push_outbound(&self, alias: &str, port: &str, payload: CorrelatedValue) {
        let alias = alias.to_ascii_lowercase();
        let key = port.to_ascii_lowercase();
        let allowed = self
            .outbound_filters
            .lock()
            .ok()
            .and_then(|guard| guard.get(&alias).cloned())
            .flatten();
        if allowed.as_ref().is_some_and(|ports| !ports.contains(&key)) {
            return;
        }
        let image_like_port = self
            .incoming_types
            .lock()
            .ok()
            .and_then(|guard| guard.get(&alias).and_then(|ports| ports.get(&key).cloned()))
            .as_ref()
            .map(|ty| typeexpr_is_image_payload(Some(ty)))
            .unwrap_or(false);
        if let Ok(mut guard) = self.inner.lock()
            && let Some(shared) = guard.get_mut(&alias)
            && let Ok(mut buf) = shared.lock()
        {
            let q = buf.outbound.entry(key.clone()).or_default();
            let cap = outbound_cap_for_value(&payload, image_like_port);
            if cap > 0 && q.len() >= cap {
                q.pop_front();
                let drop_count = HOST_BRIDGE_OUTBOUND_DROPS.fetch_add(1, Ordering::Relaxed) + 1;
                if drop_count <= 5 || drop_count.is_multiple_of(500) {
                    log::warn!(
                        "host-bridge outbound overflow alias={} port={} cap={} dropping-oldest count={}",
                        alias,
                        key,
                        cap,
                        drop_count
                    );
                }
            }
            q.push_back(payload);
            if host_bridge_trace_enabled() {
                let count = HOST_BRIDGE_OUTBOUND_LOGS.fetch_add(1, Ordering::Relaxed);
                if count < 5 || count.is_multiple_of(500) {
                    log::debug!(
                        "host-bridge outbound queued alias={} port={} len={}",
                        alias,
                        key,
                        q.len()
                    );
                }
            }
            if let Some(waker) = buf.wakers.get(&key) {
                waker.wake();
            }
        }
    }

    /// Internal helper: drain inbound payloads enqueued by the host.
    fn take_inbound(&self, alias: &str) -> Vec<(String, CorrelatedValue)> {
        if let Ok(mut guard) = self.inner.lock()
            && let Some(shared) = guard.get_mut(&alias.to_ascii_lowercase())
            && let Ok(mut buf) = shared.lock()
        {
            let mut drained = Vec::new();
            for (port, queue) in buf.inbound.iter_mut() {
                let Some(latest) = queue.pop_back() else {
                    continue;
                };
                let dropped = queue.len();
                queue.clear();
                drained.push((port.clone(), latest));
                if host_bridge_trace_enabled() && dropped > 0 {
                    log::debug!(
                        "host-bridge inbound coalesced alias={} port={} dropped={}",
                        alias,
                        port,
                        dropped
                    );
                }
            }
            return drained;
        }
        Vec::new()
    }
}

/// Build a host bridge handler that moves data between host-managed buffers and runtime edges.
pub fn bridge_handler(
    bridges: HostBridgeManager,
) -> impl FnMut(
    &crate::plan::RuntimeNode,
    &crate::state::ExecutionContext,
    &mut NodeIo,
) -> Result<(), NodeError> {
    move |node, _ctx, io| {
        let alias = node
            .label
            .as_deref()
            .unwrap_or(node.id.as_str())
            .to_ascii_lowercase();

        // Host -> graph: forward anything queued on the host side.
        let inbound = bridges.take_inbound(&alias);
        if host_bridge_trace_enabled() && !inbound.is_empty() {
            let mut entries = Vec::new();
            for (port, payload) in &inbound {
                entries.push(format!("{}#{}", port, describe_runtime_value(payload)));
            }
            log::debug!(
                "host-bridge inbound alias={} node={} ports={}",
                alias,
                node.id,
                entries.join(", ")
            );
        }
        for (port, payload) in inbound {
            io.push_correlated_value(Some(&port), payload);
        }

        // Graph -> host: collect inputs and stash for host consumption.
        if host_bridge_trace_enabled() && !io.inputs().is_empty() {
            let mut entries = Vec::new();
            for (port, payload) in io.inputs() {
                entries.push(format!("{}#{}", port, describe_edge(payload)));
            }
            log::debug!(
                "host-bridge outbound alias={} node={} ports={}",
                alias,
                node.id,
                entries.join(", ")
            );
        }
        for (port, payload) in io.inputs() {
            bridges.push_outbound(&alias, port, payload.clone());
        }
        Ok(())
    }
}

fn host_bridge_trace_enabled() -> bool {
    static FLAG: OnceLock<bool> = OnceLock::new();
    *FLAG.get_or_init(|| {
        std::env::var("DAEDALUS_HOST_BRIDGE_TRACE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

fn host_bridge_outbound_cap() -> usize {
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("DAEDALUS_HOST_BRIDGE_OUTBOUND_CAP")
            .ok()
            .and_then(|raw| raw.trim().parse::<usize>().ok())
            .unwrap_or(4)
            .max(1)
    })
}

fn host_bridge_inbound_cap() -> usize {
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("DAEDALUS_HOST_BRIDGE_INBOUND_CAP")
            .ok()
            .and_then(|raw| raw.trim().parse::<usize>().ok())
            .unwrap_or(1)
            .max(1)
    })
}

fn typeexpr_is_image_payload(ty: Option<&TypeExpr>) -> bool {
    match ty {
        Some(TypeExpr::Opaque(name)) => {
            let lower = name.to_ascii_lowercase();
            lower == "image" || lower.starts_with("image:")
        }
        Some(TypeExpr::Optional(inner)) => typeexpr_is_image_payload(Some(inner.as_ref())),
        _ => false,
    }
}

fn outbound_cap_for_value(_payload: &CorrelatedValue, image_like_port: bool) -> usize {
    if image_like_port {
        return 1;
    }
    let base = host_bridge_outbound_cap();
    #[cfg(feature = "gpu")]
    {
        match &_payload.inner {
            RuntimeValue::Data(ep) if ep.is_gpu() => 1,
            RuntimeValue::Any(any) if any_is_gpu_image_handle(any.as_ref()) => 1,
            _ => base,
        }
    }
    #[cfg(not(feature = "gpu"))]
    {
        base
    }
}

fn describe_runtime_value(p: &CorrelatedValue) -> String {
    match &p.inner {
        #[cfg(feature = "gpu")]
        RuntimeValue::Any(any) if any_is_gpu_image_handle(any.as_ref()) => "gpu_image".to_string(),
        RuntimeValue::Any(_) => "any".to_string(),
        RuntimeValue::Value(v) => format!("value({})", describe_daedalus_value(v)),
        RuntimeValue::Bytes(b) => format!("bytes({}b)", b.len()),
        RuntimeValue::Unit => "unit".to_string(),
        #[cfg(feature = "gpu")]
        RuntimeValue::Data(ep) => if ep.is_gpu() { "gpu_data" } else { "cpu_data" }.to_string(),
    }
}

fn describe_edge(p: &CorrelatedValue) -> String {
    match &p.inner {
        #[cfg(feature = "gpu")]
        RuntimeValue::Any(any) if any_is_gpu_image_handle(any.as_ref()) => "gpu_image".to_string(),
        RuntimeValue::Any(_) => "any".to_string(),
        RuntimeValue::Value(v) => format!("value({})", describe_daedalus_value(v)),
        RuntimeValue::Bytes(b) => format!("bytes({}b)", b.len()),
        RuntimeValue::Unit => "unit".to_string(),
        #[cfg(feature = "gpu")]
        RuntimeValue::Data(ep) => if ep.is_gpu() { "gpu_data" } else { "cpu_data" }.to_string(),
    }
}

fn describe_daedalus_value(v: &Value) -> String {
    match v {
        Value::Int(i) => format!("int({})", i),
        Value::Float(f) => format!("float({})", f),
        Value::Bool(b) => format!("bool({})", b),
        Value::String(s) => format!("string({})", s),
        Value::Enum(ev) => format!("enum({})", ev.name),
        Value::List(_) => "list".to_string(),
        Value::Struct(_) => "struct".to_string(),
        Value::Tuple(_) => "tuple".to_string(),
        Value::Map(_) => "map".to_string(),
        Value::Unit => "unit".to_string(),
        Value::Bytes(b) => format!("bytes({}b)", b.len()),
    }
}

/// Serialized payload representation for host bridge boundaries.
#[derive(Clone, Debug, PartialEq)]
pub enum HostBridgeSerialized {
    Json(String),
    Bytes(Arc<[u8]>),
}

/// Serialized payload with correlation metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct HostBridgeSerializedValue {
    pub correlation_id: u64,
    pub port_type: Option<TypeExpr>,
    pub payload: HostBridgeSerialized,
}

fn deserialize_serialized_payload(
    port: &str,
    port_type: Option<&TypeExpr>,
    payload: HostBridgeSerialized,
) -> Result<RuntimeValue, NodeError> {
    match payload {
        HostBridgeSerialized::Bytes(bytes) => {
            if port_type.is_some_and(is_bytes_type) {
                return Ok(RuntimeValue::Bytes(bytes));
            }
            let text = std::str::from_utf8(&bytes).map_err(|err| {
                NodeError::InvalidInput(format!(
                    "host bridge port {port}: bytes are not utf-8 ({err})"
                ))
            })?;
            let value = parse_json_value(port, text)?;
            Ok(json_runtime_value(value))
        }
        HostBridgeSerialized::Json(json) => {
            let value = parse_json_value(port, &json)?;
            Ok(json_runtime_value(value))
        }
    }
}

fn serialize_outbound_payload(
    port: &str,
    port_type: Option<&TypeExpr>,
    payload: CorrelatedValue,
) -> Result<HostBridgeSerializedValue, NodeError> {
    let export_policy = port_type
        .map(daedalus_data::named_types::export_policy_for)
        .unwrap_or(HostExportPolicy::Value);

    let serialized = match payload.inner {
        RuntimeValue::Unit => {
            HostBridgeSerialized::Json(serialize_value_to_json(port, &Value::Unit)?)
        }
        RuntimeValue::Bytes(bytes) => HostBridgeSerialized::Bytes(bytes),
        RuntimeValue::Value(value) => {
            HostBridgeSerialized::Json(serialize_value_to_json(port, &value)?)
        }
        RuntimeValue::Any(any) => {
            if export_policy == HostExportPolicy::None {
                // Policy says "do not guess": only allow serialization if the payload is already
                // value-like/bytes-like. This prevents surprising runtime costs and confusing
                // "why did my image become JSON?" behavior.
                if any_to_value(any.as_ref()).is_none() && any_to_bytes(any.as_ref()).is_none() {
                    let ty = std::any::type_name_of_val(any.as_ref());
                    let schema = port_type
                        .map(|t| format!("{t:?}"))
                        .unwrap_or_else(|| "<unknown>".to_string());
                    return Err(NodeError::InvalidInput(format!(
                        "host bridge port {port}: schema is non-serializable ({schema}); payload type={ty}. Insert an explicit encoder/converter node to `Value` or `Bytes`."
                    )));
                }
            }
            if port_type.is_some_and(is_bytes_type) {
                if let Some(bytes) = any_to_bytes(any.as_ref()) {
                    HostBridgeSerialized::Bytes(bytes)
                } else if let Some(value) = any_to_value(any.as_ref()) {
                    HostBridgeSerialized::Json(serialize_value_to_json(port, &value)?)
                } else {
                    log::warn!(
                        "host bridge port {}: unsupported Any payload for bytes output",
                        port
                    );
                    return Err(NodeError::InvalidInput(format!(
                        "host bridge port {port}: unsupported Any payload for bytes output"
                    )));
                }
            } else if let Some(value) = any_to_value(any.as_ref()) {
                HostBridgeSerialized::Json(serialize_value_to_json(port, &value)?)
            } else if let Some(bytes) = any_to_bytes(any.as_ref()) {
                HostBridgeSerialized::Bytes(bytes)
            } else {
                log::warn!("host bridge port {}: unsupported Any payload", port);
                return Err(NodeError::InvalidInput(format!(
                    "host bridge port {port}: unsupported Any payload"
                )));
            }
        }
        #[cfg(feature = "gpu")]
        RuntimeValue::Data(_) => {
            log::warn!(
                "host bridge port {}: gpu payloads cannot be serialized",
                port
            );
            return Err(NodeError::InvalidInput(format!(
                "host bridge port {port}: gpu payloads cannot be serialized"
            )));
        }
    };

    Ok(HostBridgeSerializedValue {
        correlation_id: payload.correlation_id,
        port_type: port_type.cloned(),
        payload: serialized,
    })
}

fn is_bytes_type(ty: &TypeExpr) -> bool {
    matches!(ty, TypeExpr::Scalar(ValueType::Bytes))
}

fn parse_json_value(port: &str, json_str: &str) -> Result<Value, NodeError> {
    json::from_json(json_str).map_err(|err| {
        NodeError::InvalidInput(format!(
            "host bridge port {port}: invalid typed json ({err})"
        ))
    })
}

fn serialize_value_to_json(port: &str, value: &Value) -> Result<String, NodeError> {
    json::to_json(value).map_err(|err| {
        NodeError::InvalidInput(format!(
            "host bridge port {port}: failed to serialize value ({err})"
        ))
    })
}

fn json_runtime_value(value: Value) -> RuntimeValue {
    RuntimeValue::Value(value)
}

pub type ValueSerializer = Box<dyn Fn(&dyn Any) -> Option<Value> + Send + Sync + 'static>;
pub type ValueSerializerMap = Arc<RwLock<Vec<ValueSerializer>>>;

fn value_serializers() -> &'static ValueSerializerMap {
    static REGISTRY: OnceLock<ValueSerializerMap> = OnceLock::new();
    REGISTRY.get_or_init(|| Arc::new(RwLock::new(Vec::new())))
}

/// Shared registry for value serializers.
pub fn value_serializer_map() -> ValueSerializerMap {
    value_serializers().clone()
}

fn try_serialize_any_value(any: &dyn Any) -> Option<Value> {
    let guard = value_serializers().read().ok()?;
    for serializer in guard.iter() {
        if let Some(value) = serializer(any) {
            return Some(value);
        }
    }
    None
}

/// Register a conversion from a typed payload `T` into a runtime `Value`.
///
/// This allows host-bridge output serialization to support plugin-defined structured types.
pub fn register_value_serializer_in<T, F>(map: &ValueSerializerMap, serializer: F)
where
    T: Any + Clone + Send + Sync + 'static,
    F: Fn(&T) -> Value + Send + Sync + 'static,
{
    let mut guard = map
        .write()
        .expect("daedalus-runtime host bridge serializer lock poisoned");
    guard.push(Box::new(move |any| {
        if let Some(value) = any.downcast_ref::<T>() {
            return Some(serializer(value));
        }
        None
    }));
}

/// Register a conversion from a typed payload `T` into a runtime `Value`.
///
/// This allows host-bridge output serialization to support plugin-defined structured types.
pub fn register_value_serializer<T, F>(serializer: F)
where
    T: Any + Clone + Send + Sync + 'static,
    F: Fn(&T) -> Value + Send + Sync + 'static,
{
    register_value_serializer_in(&value_serializer_map(), serializer);
}

fn any_to_value(any: &dyn Any) -> Option<Value> {
    if let Some(inner) = any.downcast_ref::<Arc<dyn Any + Send + Sync>>() {
        return any_to_value(inner.as_ref());
    }
    if let Some(inner) = any.downcast_ref::<Box<dyn Any + Send + Sync>>() {
        return any_to_value(inner.as_ref());
    }
    if let Some(inner) = any.downcast_ref::<Arc<Box<dyn Any + Send + Sync>>>() {
        return any_to_value(inner.as_ref());
    }
    if let Some(inner) = any.downcast_ref::<Box<Arc<dyn Any + Send + Sync>>>() {
        return any_to_value(inner.as_ref());
    }
    if let Some(value) = try_serialize_any_value(any) {
        return Some(value);
    }
    let value = any.downcast_ref::<Value>().cloned();
    if let Some(value) = value {
        return Some(value);
    }

    let i = any.downcast_ref::<i64>().copied();
    if let Some(i) = i {
        return Some(Value::Int(i));
    }

    let f = any.downcast_ref::<f64>().copied();
    if let Some(f) = f {
        return Some(Value::Float(f));
    }

    let b = any.downcast_ref::<bool>().copied();
    if let Some(b) = b {
        return Some(Value::Bool(b));
    }

    let s = any.downcast_ref::<String>().cloned();
    if let Some(s) = s {
        return Some(Value::String(s.into()));
    }

    let bytes = any.downcast_ref::<Vec<u8>>().cloned();
    bytes.map(|b| Value::Bytes(b.into()))
}

fn any_to_bytes(any: &dyn Any) -> Option<Arc<[u8]>> {
    if let Some(bytes) = any.downcast_ref::<Vec<u8>>().cloned() {
        return Some(Arc::from(bytes));
    }
    if let Some(bytes) = any.downcast_ref::<Arc<[u8]>>() {
        return Some(bytes.clone());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_policies() -> Vec<(String, EdgePolicyKind)> {
        Vec::new()
    }

    #[test]
    fn push_serialized_json_decodes_to_value() {
        let mgr = HostBridgeManager::new();
        mgr.register_bridge("host", empty_policies());
        mgr.register_port_types(
            "host",
            vec![("config".into(), TypeExpr::Scalar(ValueType::Int))],
            Vec::new(),
        );
        let handle = mgr.handle("host").expect("host handle");

        let json = json::to_json(&Value::Int(5)).expect("json");
        let id = handle
            .push_serialized("config", HostBridgeSerialized::Json(json), None)
            .expect("push");

        let inbound = mgr.take_inbound("host");
        assert_eq!(inbound.len(), 1);
        let (port, payload) = &inbound[0];
        assert_eq!(port, "config");
        assert_eq!(payload.correlation_id, id);

        match &payload.inner {
            RuntimeValue::Value(Value::Int(v)) => assert_eq!(*v, 5),
            other => panic!("unexpected payload {other:?}"),
        }
    }

    #[test]
    fn try_pop_serialized_encodes_value_to_json() {
        let mgr = HostBridgeManager::new();
        mgr.register_bridge("host", empty_policies());
        mgr.register_port_types(
            "host",
            Vec::new(),
            vec![("status".into(), TypeExpr::Scalar(ValueType::Int))],
        );
        let payload = CorrelatedValue::from_edge(json_runtime_value(Value::Int(7)));
        mgr.push_outbound("host", "status", payload);

        let handle = mgr.handle("host").expect("host handle");
        let serialized = handle
            .try_pop_serialized("status")
            .expect("serialize")
            .expect("payload");

        assert_eq!(serialized.port_type, Some(TypeExpr::Scalar(ValueType::Int)));

        match serialized.payload {
            HostBridgeSerialized::Json(json_str) => {
                let value = json::from_json(&json_str).expect("from json");
                assert_eq!(value, Value::Int(7));
            }
            other => panic!("unexpected payload {other:?}"),
        }
    }

    #[test]
    fn bytes_payloads_pass_through_serialization() {
        let mgr = HostBridgeManager::new();
        mgr.register_bridge("host", empty_policies());
        mgr.register_port_types(
            "host",
            Vec::new(),
            vec![("blob".into(), TypeExpr::Scalar(ValueType::Bytes))],
        );
        let bytes: Arc<[u8]> = Arc::from(vec![1_u8, 2, 3, 4]);
        let payload = CorrelatedValue::from_edge(RuntimeValue::Bytes(bytes.clone()));
        mgr.push_outbound("host", "blob", payload);

        let handle = mgr.handle("host").expect("host handle");
        let serialized = handle
            .try_pop_serialized("blob")
            .expect("serialize")
            .expect("payload");

        match serialized.payload {
            HostBridgeSerialized::Bytes(out) => assert_eq!(out.as_ref(), bytes.as_ref()),
            other => panic!("unexpected payload {other:?}"),
        }
    }

    #[test]
    fn try_pop_value_decodes_value_like_any() {
        let mgr = HostBridgeManager::new();
        mgr.register_bridge("host", empty_policies());
        mgr.register_port_types(
            "host",
            Vec::new(),
            vec![("out".into(), TypeExpr::Scalar(ValueType::Int))],
        );
        mgr.push_outbound(
            "host",
            "out",
            CorrelatedValue::from_edge(RuntimeValue::Any(Arc::new(7_i64))),
        );

        let host = mgr.handle("host").expect("host handle");
        let (_corr, value) = host.try_pop_value("out").expect("value").expect("payload");
        assert_eq!(value, Value::Int(7));
    }

    #[test]
    fn try_pop_value_rejects_non_value_any() {
        #[derive(Clone)]
        struct NotValue;

        let mgr = HostBridgeManager::new();
        mgr.register_bridge("host", empty_policies());
        mgr.register_port_types(
            "host",
            Vec::new(),
            vec![("out".into(), TypeExpr::opaque("image:dynamic"))],
        );
        mgr.push_outbound(
            "host",
            "out",
            CorrelatedValue::from_edge(RuntimeValue::Any(Arc::new(NotValue))),
        );

        let host = mgr.handle("host").expect("host handle");
        let err = host.try_pop_value("out").expect_err("expected error");
        match err {
            NodeError::InvalidInput(msg) => assert!(msg.contains("not value-like")),
            other => panic!("unexpected error {other:?}"),
        }
    }

    #[test]
    fn drain_values_decodes_multiple_payloads() {
        let mgr = HostBridgeManager::new();
        mgr.register_bridge("host", empty_policies());
        mgr.register_port_types(
            "host",
            Vec::new(),
            vec![("out".into(), TypeExpr::Scalar(ValueType::Int))],
        );
        mgr.push_outbound(
            "host",
            "out",
            CorrelatedValue::from_edge(json_runtime_value(Value::Int(1))),
        );
        mgr.push_outbound(
            "host",
            "out",
            CorrelatedValue::from_edge(json_runtime_value(Value::Int(2))),
        );

        let host = mgr.handle("host").expect("host handle");
        let vals = host.drain_values("out").expect("drain");
        assert_eq!(vals.len(), 2);
        assert_eq!(vals[0].1, Value::Int(1));
        assert_eq!(vals[1].1, Value::Int(2));
    }

    #[test]
    fn outbound_port_filter_drops_unselected_ports() {
        let mgr = HostBridgeManager::new();
        mgr.register_bridge("host", empty_policies());
        mgr.register_port_types(
            "host",
            Vec::new(),
            vec![
                ("overlay".into(), TypeExpr::opaque("image:gray8")),
                ("clahe".into(), TypeExpr::opaque("image:gray8")),
            ],
        );
        mgr.set_outbound_port_filter(
            "host",
            Some(std::collections::BTreeSet::from(["overlay".to_string()])),
        );

        mgr.push_outbound(
            "host",
            "overlay",
            CorrelatedValue::from_edge(RuntimeValue::Unit),
        );
        mgr.push_outbound(
            "host",
            "clahe",
            CorrelatedValue::from_edge(RuntimeValue::Unit),
        );

        let host = mgr.handle("host").expect("host handle");
        assert!(host.try_pop("overlay").is_some());
        assert!(host.try_pop("clahe").is_none());
    }

    #[test]
    fn iter_ports_can_filter_and_pop_values() {
        let mgr = HostBridgeManager::new();
        mgr.register_bridge("host", empty_policies());
        mgr.register_port_types(
            "host",
            Vec::new(),
            vec![
                ("a".into(), TypeExpr::Scalar(ValueType::Int)),
                ("b".into(), TypeExpr::Scalar(ValueType::Int)),
            ],
        );
        mgr.push_outbound(
            "host",
            "a",
            CorrelatedValue::from_edge(json_runtime_value(Value::Int(42))),
        );

        let host = mgr.handle("host").expect("host handle");
        let ports = vec!["a".to_string(), "b".to_string()];
        let got = host
            .iter_ports(&ports)
            .filter(|p| p.can_poll::<Value>())
            .try_pop_all_values()
            .expect("pop");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, "a");
        assert_eq!(got[0].2, Value::Int(42));
    }

    #[test]
    fn incoming_ports_can_filter_and_pop_values() {
        let mgr = HostBridgeManager::new();
        mgr.register_bridge("host", empty_policies());
        mgr.register_port_types(
            "host",
            Vec::new(),
            vec![
                ("a".into(), TypeExpr::Scalar(ValueType::Int)),
                ("b".into(), TypeExpr::Scalar(ValueType::Int)),
            ],
        );
        mgr.push_outbound(
            "host",
            "a",
            CorrelatedValue::from_edge(json_runtime_value(Value::Int(42))),
        );

        let host = mgr.handle("host").expect("host handle");
        let got = host
            .incoming_ports()
            .filter(|p| p.can_type_to::<Value>())
            .try_pop_all_values()
            .expect("pop");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0, "a");
        assert_eq!(got[0].2, Value::Int(42));
    }

    #[test]
    fn clear_preserves_outbound_queue_for_reuse() {
        let mgr = HostBridgeManager::new();
        mgr.register_bridge("host", empty_policies());
        mgr.register_port_types(
            "host",
            Vec::new(),
            vec![("tv".into(), TypeExpr::Scalar(ValueType::Int))],
        );
        mgr.push_outbound(
            "host",
            "tv",
            CorrelatedValue::from_edge(json_runtime_value(Value::Int(1))),
        );
        mgr.push_outbound(
            "host",
            "tv",
            CorrelatedValue::from_edge(json_runtime_value(Value::Int(2))),
        );

        let host = mgr.handle("host").expect("host handle");
        assert_eq!(host.clear("tv"), 2);

        {
            let guard = mgr.inner.lock().expect("host bridge manager");
            let shared = guard
                .get("host")
                .expect("bridge entry")
                .lock()
                .expect("bridge shared state");
            let queue = shared.outbound.get("tv").expect("queue retained");
            assert!(queue.is_empty());
        }

        mgr.push_outbound(
            "host",
            "tv",
            CorrelatedValue::from_edge(json_runtime_value(Value::Int(3))),
        );
        let (_corr, value) = host.try_pop_value("tv").expect("value").expect("payload");
        assert_eq!(value, Value::Int(3));
    }

    #[test]
    fn image_like_ports_only_retain_latest_outbound_frame() {
        use image::{DynamicImage, ImageBuffer, Luma};

        let mgr = HostBridgeManager::new();
        mgr.register_bridge("host", empty_policies());
        mgr.register_port_types(
            "host",
            Vec::new(),
            vec![("frame".into(), TypeExpr::opaque("image:gray"))],
        );

        for value in [1u8, 2u8, 3u8] {
            let img = DynamicImage::ImageLuma8(ImageBuffer::from_pixel(1, 1, Luma([value])));
            mgr.push_outbound(
                "host",
                "frame",
                CorrelatedValue::from_edge(RuntimeValue::Any(Arc::new(img))),
            );
        }

        {
            let guard = mgr.inner.lock().expect("host bridge manager");
            let shared = guard
                .get("host")
                .expect("bridge entry")
                .lock()
                .expect("bridge shared state");
            let queue = shared.outbound.get("frame").expect("queue retained");
            assert_eq!(
                queue.len(),
                1,
                "image-like ports should only keep the latest frame"
            );
        }

        let host = mgr.handle("host").expect("host handle");
        let (_corr, img) = host
            .iter_ports(&["frame".to_string()])
            .next()
            .expect("port")
            .try_pop::<DynamicImage>()
            .expect("pop")
            .expect("payload");
        assert_eq!(img.to_luma8().get_pixel(0, 0).0[0], 3);
    }

    #[test]
    fn iter_ports_can_poll_dynamic_image_by_resolved_type() {
        use image::{GenericImageView, ImageBuffer, Rgba};

        let mgr = HostBridgeManager::new();
        mgr.register_bridge("host", empty_policies());
        mgr.register_port_types(
            "host",
            Vec::new(),
            vec![("frame".into(), TypeExpr::opaque("image:dynamic"))],
        );

        let img = DynamicImage::ImageRgba8(ImageBuffer::from_pixel(2, 2, Rgba([0, 0, 0, 255])));
        mgr.push_outbound(
            "host",
            "frame",
            CorrelatedValue::from_edge(RuntimeValue::Any(Arc::new(img.clone()))),
        );

        let host = mgr.handle("host").expect("host handle");
        let ports = vec!["frame".to_string()];
        let port = host.iter_ports(&ports).next().expect("port");
        assert!(port.can_poll::<DynamicImage>());
        let (_corr, got) = port
            .try_pop::<DynamicImage>()
            .expect("pop")
            .expect("payload");
        assert_eq!(got.dimensions(), img.dimensions());
    }
}

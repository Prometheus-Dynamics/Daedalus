use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceClass {
    #[default]
    FrameScratch,
    WarmCache,
    PersistentState,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceLifecycleEvent {
    #[default]
    BeforeFrame,
    AfterFrame,
    MemoryPressure,
    Idle,
    Stop,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResourceUsage {
    pub live_bytes: u64,
    pub retained_bytes: u64,
    #[serde(default)]
    pub touched_bytes: u64,
    #[serde(default)]
    pub allocation_events: u64,
}

impl ResourceUsage {
    pub(super) fn new(live_bytes: u64, retained_bytes: u64) -> Self {
        Self {
            live_bytes,
            retained_bytes: retained_bytes.max(live_bytes),
            touched_bytes: live_bytes,
            allocation_events: 0,
        }
    }

    fn with_details(
        live_bytes: u64,
        retained_bytes: u64,
        touched_bytes: u64,
        allocation_events: u64,
    ) -> Self {
        Self {
            live_bytes,
            retained_bytes: retained_bytes.max(live_bytes),
            touched_bytes,
            allocation_events,
        }
    }

    fn clear_live(&mut self) {
        self.live_bytes = 0;
        self.touched_bytes = 0;
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeResourceSnapshot {
    pub frame_scratch: ResourceUsage,
    pub warm_cache: ResourceUsage,
    pub persistent_state: ResourceUsage,
}

impl NodeResourceSnapshot {
    pub(super) fn add_usage(&mut self, class: ResourceClass, usage: ResourceUsage) {
        let target = match class {
            ResourceClass::FrameScratch => &mut self.frame_scratch,
            ResourceClass::WarmCache => &mut self.warm_cache,
            ResourceClass::PersistentState => &mut self.persistent_state,
        };
        target.live_bytes = target.live_bytes.saturating_add(usage.live_bytes);
        target.retained_bytes = target.retained_bytes.saturating_add(usage.retained_bytes);
        target.touched_bytes = target.touched_bytes.saturating_add(usage.touched_bytes);
        target.allocation_events = target
            .allocation_events
            .saturating_add(usage.allocation_events);
    }
}

pub(super) struct ResourceEntry {
    pub(super) class: ResourceClass,
    pub(super) storage: ResourceStorage,
}

impl ResourceEntry {
    pub(super) fn usage(&self) -> ResourceUsage {
        match &self.storage {
            ResourceStorage::Usage(usage) => *usage,
            ResourceStorage::Managed(managed) => managed.usage(),
            ResourceStorage::InUse(usage) => *usage,
        }
    }

    pub(super) fn apply_lifecycle(&mut self, event: ResourceLifecycleEvent) {
        match &mut self.storage {
            ResourceStorage::Usage(usage) => match event {
                ResourceLifecycleEvent::BeforeFrame => {
                    if self.class == ResourceClass::FrameScratch {
                        usage.clear_live();
                    }
                }
                ResourceLifecycleEvent::AfterFrame => {}
                ResourceLifecycleEvent::MemoryPressure => match self.class {
                    ResourceClass::FrameScratch => {
                        *usage = ResourceUsage::default();
                    }
                    ResourceClass::WarmCache => {
                        usage.retained_bytes = usage.live_bytes;
                    }
                    ResourceClass::PersistentState => {
                        usage.retained_bytes = usage.retained_bytes.max(usage.live_bytes);
                    }
                },
                ResourceLifecycleEvent::Idle => match self.class {
                    ResourceClass::FrameScratch => {
                        *usage = ResourceUsage::default();
                    }
                    ResourceClass::WarmCache => {
                        usage.live_bytes = 0;
                        usage.retained_bytes = 0;
                    }
                    ResourceClass::PersistentState => {
                        usage.live_bytes = 0;
                    }
                },
                ResourceLifecycleEvent::Stop => {}
            },
            ResourceStorage::Managed(managed) => match event {
                ResourceLifecycleEvent::BeforeFrame => managed.before_frame(),
                ResourceLifecycleEvent::AfterFrame => managed.after_frame(),
                ResourceLifecycleEvent::MemoryPressure => managed.on_memory_pressure(),
                ResourceLifecycleEvent::Idle => managed.on_idle(),
                ResourceLifecycleEvent::Stop => managed.on_stop(),
            },
            ResourceStorage::InUse(_) => {}
        }
    }
}

pub(super) enum ResourceStorage {
    Usage(ResourceUsage),
    Managed(Box<dyn ManagedResourceBox>),
    InUse(ResourceUsage),
}

pub(super) type NodeResources = HashMap<String, ResourceEntry>;
pub(super) type SharedNodeResources = Arc<std::sync::Mutex<NodeResources>>;

pub trait ManagedResource: Send + Sync + 'static {
    fn live_bytes(&self) -> u64 {
        0
    }

    fn retained_bytes(&self) -> u64 {
        self.live_bytes()
    }

    fn touched_bytes(&self) -> u64 {
        self.live_bytes()
    }

    fn allocation_events(&self) -> u64 {
        0
    }

    fn before_frame(&mut self) {}

    fn after_frame(&mut self) {}

    fn on_memory_pressure(&mut self) {}

    fn on_idle(&mut self) {}

    fn on_stop(&mut self) {}
}

pub(super) trait ManagedResourceBox: Send + Sync {
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn usage(&self) -> ResourceUsage;
    fn before_frame(&mut self);
    fn after_frame(&mut self);
    fn on_memory_pressure(&mut self);
    fn on_idle(&mut self);
    fn on_stop(&mut self);
}

impl<T: ManagedResource> ManagedResourceBox for T {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn usage(&self) -> ResourceUsage {
        ResourceUsage::with_details(
            self.live_bytes(),
            self.retained_bytes(),
            self.touched_bytes(),
            self.allocation_events(),
        )
    }

    fn before_frame(&mut self) {
        ManagedResource::before_frame(self);
    }

    fn after_frame(&mut self) {
        ManagedResource::after_frame(self);
    }

    fn on_memory_pressure(&mut self) {
        ManagedResource::on_memory_pressure(self);
    }

    fn on_idle(&mut self) {
        ManagedResource::on_idle(self);
    }

    fn on_stop(&mut self) {
        ManagedResource::on_stop(self);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
enum ManagedByteBufferPolicy {
    #[default]
    FrameScratch,
    WarmCache,
    PersistentState,
}

/// Runtime-managed reusable byte buffer for scratch, cache, or persistent byte state.
#[derive(Clone, Debug, Default)]
pub struct ManagedByteBuffer {
    bytes: Vec<u8>,
    live_len: usize,
    touched_bytes: u64,
    allocation_events: u64,
    policy: ManagedByteBufferPolicy,
}

impl ManagedByteBuffer {
    pub fn frame_scratch() -> Self {
        Self {
            policy: ManagedByteBufferPolicy::FrameScratch,
            ..Default::default()
        }
    }

    pub fn warm_cache() -> Self {
        Self {
            policy: ManagedByteBufferPolicy::WarmCache,
            ..Default::default()
        }
    }

    pub fn persistent_state() -> Self {
        Self {
            policy: ManagedByteBufferPolicy::PersistentState,
            ..Default::default()
        }
    }

    pub fn prepare(&mut self, len: usize) -> &mut [u8] {
        if self.bytes.capacity() < len {
            self.allocation_events = self.allocation_events.saturating_add(1);
        }
        if self.bytes.len() < len {
            self.bytes.resize(len, 0);
        } else {
            self.bytes[..len].fill(0);
            self.bytes.truncate(len);
        }
        self.live_len = len;
        self.touched_bytes = self.touched_bytes.saturating_add(len as u64);
        &mut self.bytes[..len]
    }

    pub fn reserve_exact(&mut self, capacity: usize) {
        if self.bytes.capacity() < capacity {
            self.bytes
                .reserve_exact(capacity.saturating_sub(self.bytes.capacity()));
            self.allocation_events = self.allocation_events.saturating_add(1);
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.live_len]
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.touched_bytes = self.touched_bytes.saturating_add(self.live_len as u64);
        &mut self.bytes[..self.live_len]
    }

    pub fn capacity(&self) -> usize {
        self.bytes.capacity()
    }

    pub fn len(&self) -> usize {
        self.live_len
    }

    pub fn is_empty(&self) -> bool {
        self.live_len == 0
    }

    pub fn clear_live(&mut self) {
        self.live_len = 0;
        self.bytes.clear();
    }
}

impl ManagedResource for ManagedByteBuffer {
    fn live_bytes(&self) -> u64 {
        self.live_len as u64
    }

    fn retained_bytes(&self) -> u64 {
        self.bytes.capacity() as u64
    }

    fn touched_bytes(&self) -> u64 {
        self.touched_bytes
    }

    fn allocation_events(&self) -> u64 {
        self.allocation_events
    }

    fn before_frame(&mut self) {
        if self.policy == ManagedByteBufferPolicy::FrameScratch {
            self.clear_live();
            self.touched_bytes = 0;
        }
    }

    fn after_frame(&mut self) {
        if self.policy == ManagedByteBufferPolicy::FrameScratch {
            self.touched_bytes = 0;
        }
    }

    fn on_memory_pressure(&mut self) {
        match self.policy {
            ManagedByteBufferPolicy::FrameScratch => {
                self.clear_live();
                self.bytes.shrink_to_fit();
            }
            ManagedByteBufferPolicy::WarmCache => {
                self.bytes.shrink_to(self.live_len);
            }
            ManagedByteBufferPolicy::PersistentState => {}
        }
        self.touched_bytes = 0;
    }

    fn on_idle(&mut self) {
        match self.policy {
            ManagedByteBufferPolicy::FrameScratch | ManagedByteBufferPolicy::WarmCache => {
                self.clear_live();
                self.bytes.shrink_to_fit();
            }
            ManagedByteBufferPolicy::PersistentState => {
                self.live_len = 0;
            }
        }
        self.touched_bytes = 0;
    }

    fn on_stop(&mut self) {
        self.clear_live();
        self.bytes.shrink_to_fit();
        self.touched_bytes = 0;
    }
}

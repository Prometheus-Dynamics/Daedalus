use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{AccessMode, AdaptKind, AdapterId, Layout, Payload, Residency, TypeKey};

/// Planned decision for moving one producer payload into N consumers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FanoutAction {
    Share,
    Move,
    CowBranch,
    Materialize,
    Adapter(AdapterId),
    Error(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FanoutConsumer {
    pub access: AccessMode,
    #[serde(default)]
    pub exclusive: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FanoutPlan {
    pub actions: Vec<FanoutAction>,
}

pub fn plan_fanout(consumers: &[FanoutConsumer]) -> FanoutPlan {
    let writers = consumers
        .iter()
        .filter(|consumer| consumer.access == AccessMode::Modify)
        .count();
    let movers = consumers
        .iter()
        .filter(|consumer| consumer.access == AccessMode::Move)
        .count();
    let total = consumers.len();

    let actions = consumers
        .iter()
        .map(|consumer| match consumer.access {
            AccessMode::Read | AccessMode::View if writers == 0 && movers == 0 => {
                FanoutAction::Share
            }
            AccessMode::Move if total == 1 => FanoutAction::Move,
            AccessMode::Modify if total == 1 || consumer.exclusive => FanoutAction::Move,
            AccessMode::Modify if writers <= 1 => FanoutAction::CowBranch,
            AccessMode::Move | AccessMode::Modify => FanoutAction::Materialize,
            AccessMode::Read | AccessMode::View => FanoutAction::Share,
        })
        .collect();

    FanoutPlan { actions }
}

/// Estimated copy behavior for an adapter.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum CopyCost {
    #[default]
    None,
    HeaderOnly,
    Proportional,
    Exact(u64),
}

impl CopyCost {
    fn score(self) -> u64 {
        match self {
            CopyCost::None => 0,
            CopyCost::HeaderOnly => 1,
            CopyCost::Proportional => 1_000,
            CopyCost::Exact(bytes) => 1_000 + bytes,
        }
    }
}

/// Weighted cost metadata used by planner adapter resolution.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AdaptCost {
    pub kind: AdaptKind,
    #[serde(default)]
    pub cpu_ns: u32,
    #[serde(default)]
    pub bytes_copied: CopyCost,
    #[serde(default)]
    pub allocs: u8,
    #[serde(default)]
    pub device_transfer: bool,
}

impl AdaptCost {
    pub const fn new(kind: AdaptKind) -> Self {
        Self {
            kind,
            cpu_ns: 0,
            bytes_copied: CopyCost::None,
            allocs: 0,
            device_transfer: false,
        }
    }

    pub const fn identity() -> Self {
        Self::new(AdaptKind::Identity)
    }

    pub const fn view() -> Self {
        Self::new(AdaptKind::View)
    }

    pub const fn materialize() -> Self {
        Self {
            kind: AdaptKind::Materialize,
            cpu_ns: 0,
            bytes_copied: CopyCost::Proportional,
            allocs: 1,
            device_transfer: false,
        }
    }

    pub const fn device_transfer() -> Self {
        Self {
            kind: AdaptKind::DeviceTransfer,
            cpu_ns: 0,
            bytes_copied: CopyCost::Proportional,
            allocs: 0,
            device_transfer: true,
        }
    }

    /// Deterministic scalar weight for path ordering.
    ///
    /// The exact coefficients can evolve, but the relative shape should keep identity/view paths
    /// cheaper than materialization and device transfer by default.
    pub fn weight(self) -> u64 {
        let kind_score = match self.kind {
            AdaptKind::Identity => 0,
            AdaptKind::MetadataOnly => 1,
            AdaptKind::View | AdaptKind::SharedView => 2,
            AdaptKind::Reinterpret => 3,
            AdaptKind::Cow | AdaptKind::CowView => 5,
            AdaptKind::Branch => 20,
            AdaptKind::MutateInPlace => 30,
            AdaptKind::Materialize => 100,
            AdaptKind::Serialize | AdaptKind::Deserialize => 200,
            AdaptKind::DeviceTransfer | AdaptKind::DeviceUpload | AdaptKind::DeviceDownload => 500,
            AdaptKind::Custom => 1_000,
        };
        kind_score
            + u64::from(self.cpu_ns)
            + self.bytes_copied.score()
            + u64::from(self.allocs) * 50
            + if self.device_transfer { 10_000 } else { 0 }
    }
}

impl Default for AdaptCost {
    fn default() -> Self {
        Self::identity()
    }
}

impl Ord for AdaptCost {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.weight()
            .cmp(&other.weight())
            .then_with(|| self.kind.cmp(&other.kind))
            .then_with(|| self.cpu_ns.cmp(&other.cpu_ns))
            .then_with(|| self.bytes_copied.cmp(&other.bytes_copied))
            .then_with(|| self.allocs.cmp(&other.allocs))
            .then_with(|| self.device_transfer.cmp(&other.device_transfer))
    }
}

impl PartialOrd for AdaptCost {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Requirements a consumer places on an adapted payload.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptRequest {
    pub target: TypeKey,
    #[serde(default)]
    pub access: AccessMode,
    #[serde(default)]
    pub exclusive: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub residency: Option<Residency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout: Option<Layout>,
}

impl AdaptRequest {
    pub fn new(target: impl Into<TypeKey>) -> Self {
        Self {
            target: target.into(),
            access: AccessMode::Read,
            exclusive: false,
            residency: None,
            layout: None,
        }
    }

    pub fn access(mut self, access: AccessMode) -> Self {
        self.access = access;
        self
    }

    pub fn exclusive(mut self, exclusive: bool) -> Self {
        self.exclusive = exclusive;
        self
    }

    pub fn residency(mut self, residency: Residency) -> Self {
        self.residency = Some(residency);
        self
    }

    pub fn layout(mut self, layout: impl Into<Layout>) -> Self {
        self.layout = Some(layout.into());
        self
    }
}

/// Planned transport operation emitted by planner and executed by runtime.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransportOp {
    Share,
    Move,
    CowBranch,
    Adapter { adapter: AdapterId },
    Upload { adapter: AdapterId },
    Download { adapter: AdapterId },
    Serialize { adapter: AdapterId },
    Deserialize { adapter: AdapterId },
}

/// Executable transport adapter.
pub trait TransportAdapter: Send + Sync {
    fn id(&self) -> &AdapterId;
    fn adapt(&self, payload: Payload, request: &AdaptRequest) -> Result<Payload, TransportError>;
}

type AdapterFn =
    dyn Fn(Payload, &AdaptRequest) -> Result<Payload, TransportError> + Send + Sync + 'static;

/// Runtime adapter function table keyed by stable adapter id.
#[derive(Clone, Default)]
pub struct AdapterTable {
    entries: BTreeMap<AdapterId, Arc<AdapterFn>>,
}

impl fmt::Debug for AdapterTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdapterTable")
            .field("entries", &self.entries.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl AdapterTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_fn<F>(&mut self, id: impl Into<AdapterId>, f: F) -> Result<(), TransportError>
    where
        F: Fn(Payload, &AdaptRequest) -> Result<Payload, TransportError> + Send + Sync + 'static,
    {
        let id = id.into();
        if self.entries.contains_key(&id) {
            return Err(TransportError::DuplicateAdapter { adapter: id });
        }
        self.entries.insert(id, Arc::new(f));
        Ok(())
    }

    pub fn replace_fn<F>(&mut self, id: impl Into<AdapterId>, f: F)
    where
        F: Fn(Payload, &AdaptRequest) -> Result<Payload, TransportError> + Send + Sync + 'static,
    {
        self.entries.insert(id.into(), Arc::new(f));
    }

    pub fn register<A>(&mut self, adapter: A) -> Result<(), TransportError>
    where
        A: TransportAdapter + 'static,
    {
        let adapter = Arc::new(adapter);
        let id = adapter.id().clone();
        self.register_fn(id, move |payload, request| adapter.adapt(payload, request))
    }

    pub fn contains(&self, id: &AdapterId) -> bool {
        self.entries.contains_key(id)
    }

    pub fn adapt(
        &self,
        id: &AdapterId,
        payload: Payload,
        request: &AdaptRequest,
    ) -> Result<Payload, TransportError> {
        let adapter = self
            .entries
            .get(id)
            .ok_or_else(|| TransportError::MissingAdapter {
                adapter: id.clone(),
            })?;
        adapter(payload, request)
    }
}

/// Errors returned by transport operations.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TransportError {
    #[error("payload type mismatch: expected {expected}, found {found}")]
    TypeMismatch { expected: TypeKey, found: TypeKey },
    #[error("duplicate transport adapter: {adapter}")]
    DuplicateAdapter { adapter: AdapterId },
    #[error("missing transport adapter: {adapter}")]
    MissingAdapter { adapter: AdapterId },
    #[error("unsupported transport operation: {0}")]
    Unsupported(String),
}

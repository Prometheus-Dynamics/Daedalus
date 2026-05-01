use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::{SourceId, TypeKey};

/// Domain-level branch strategy used when one produced payload must feed consumers that require
/// ownership or mutation.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum BranchKind {
    /// Immutable consumers can share the same payload handle.
    #[default]
    Shared,
    /// Cheap `Clone`-backed branch into a new payload.
    Clone,
    /// Copy-on-write branch; data is only copied before mutation.
    Cow,
    /// Domain-specific branch behavior supplied by the payload type/plugin.
    Domain,
    /// Full materialization into independent storage.
    Materialize,
}

/// Payload-owned branch behavior for same-type fanout.
pub trait BranchPayload: Send + Sync + 'static {
    const BRANCH_KIND: BranchKind = BranchKind::Domain;

    fn branch_payload(&self) -> Self
    where
        Self: Sized;

    fn estimated_branch_bytes(&self) -> Option<u64> {
        None
    }
}

macro_rules! impl_clone_branch_payload {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl BranchPayload for $ty {
                const BRANCH_KIND: BranchKind = BranchKind::Clone;

                fn branch_payload(&self) -> Self {
                    self.clone()
                }

                fn estimated_branch_bytes(&self) -> Option<u64> {
                    Some(::core::mem::size_of::<Self>() as u64)
                }
            }
        )+
    };
}

impl_clone_branch_payload!((), bool, i32, i64, u32, f32, f64);

impl BranchPayload for String {
    const BRANCH_KIND: BranchKind = BranchKind::Clone;

    fn branch_payload(&self) -> Self {
        self.clone()
    }

    fn estimated_branch_bytes(&self) -> Option<u64> {
        Some(self.len() as u64)
    }
}

impl BranchPayload for Vec<u8> {
    const BRANCH_KIND: BranchKind = BranchKind::Clone;

    fn branch_payload(&self) -> Self {
        self.clone()
    }

    fn estimated_branch_bytes(&self) -> Option<u64> {
        Some(self.len() as u64)
    }
}

/// Payload correlation id shared by lifecycle/profiling events.
pub type CorrelationId = u64;

/// Lineage attached to every payload as it moves through a graph.
#[derive(Clone, Debug)]
pub struct PayloadLineage {
    pub correlation_id: CorrelationId,
    pub parent: Option<CorrelationId>,
    pub source_id: Option<SourceId>,
    pub created_at: Instant,
    pub source_timestamp: Option<u64>,
    pub sequence: Option<u64>,
}

impl PayloadLineage {
    pub fn new() -> Self {
        Self {
            correlation_id: next_payload_correlation_id(),
            parent: None,
            source_id: None,
            created_at: Instant::now(),
            source_timestamp: None,
            sequence: None,
        }
    }

    pub fn with_source(mut self, source_id: impl Into<SourceId>) -> Self {
        self.source_id = Some(source_id.into());
        self
    }

    pub fn with_parent(mut self, parent: CorrelationId) -> Self {
        self.parent = Some(parent);
        self
    }

    pub fn with_sequence(mut self, sequence: u64) -> Self {
        self.sequence = Some(sequence);
        self
    }

    pub fn with_source_timestamp(mut self, source_timestamp: u64) -> Self {
        self.source_timestamp = Some(source_timestamp);
        self
    }
}

impl Default for PayloadLineage {
    fn default() -> Self {
        Self::new()
    }
}

fn next_payload_correlation_id() -> CorrelationId {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// Observable payload lifecycle stages.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PayloadLifecycleStage {
    Created,
    AcceptedBySource,
    Queued,
    Dequeued,
    Adapted,
    Branched,
    Materialized,
    ResidencyCacheHit,
    ResidencyCacheMiss,
    MovedToNode,
    BorrowedByNode,
    MutatedByNode,
    ProducedByNode,
    DeliveredToOutput,
    DroppedByPolicy,
    Released,
    Recycled,
    ReleaseFailed,
}

/// How external payload resources are released.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseMode {
    #[default]
    ImmediateNonBlocking,
    DeferredToOwner,
    DeferredToRuntime,
}

/// Context passed to explicit release hooks.
#[derive(Clone, Debug)]
pub struct ReleaseContext {
    pub correlation_id: CorrelationId,
    pub type_key: TypeKey,
}

/// Explicit release hook for external resources.
pub trait PayloadRelease: Send + Sync {
    fn release_mode(&self) -> ReleaseMode;
    fn release(self: Box<Self>, ctx: ReleaseContext);
}

/// Runtime/owner release queue for payload resources that must not release on user threads.
#[derive(Default)]
pub struct PayloadReleaseQueue {
    pending: Mutex<Vec<Box<dyn PayloadRelease>>>,
}

impl PayloadReleaseQueue {
    pub fn push(&self, release: Box<dyn PayloadRelease>) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.push(release);
        }
    }

    pub fn drain(&self, ctx: ReleaseContext) -> usize {
        let pending = if let Ok(mut pending) = self.pending.lock() {
            pending.drain(..).collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let count = pending.len();
        for release in pending {
            release.release(ctx.clone());
        }
        count
    }

    pub fn len(&self) -> usize {
        self.pending
            .lock()
            .map(|pending| pending.len())
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

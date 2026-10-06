//! Atomic multi-port host pushes.
//!
//! A batch enqueues every value under one bridge lock and wakes inbound waiters once, and a tick
//! takes its host inputs under one lock too, so a drive loop sees a batch whole or not at all: a
//! frame and the context pushed with it always land in the same tick.

use crate::prelude::*;
use core::fmt;
use core::ops::Deref;

use daedalus_core::platform::Clock;
use daedalus_transport::{FeedOutcome, Payload, TypeKeyError};
use smallvec::SmallVec;

use crate::handles::PortId;
use crate::type_index::TypeIndex;

use super::{Direction, HostBridgeHandle, PortKey, enqueue_locked, io_timing};

/// Batches of up to this many values are staged and reported without allocating.
const INLINE: usize = 4;

type Staged = SmallVec<[(PortId, Result<Payload, Box<TypeKeyError>>); INLINE]>;

/// Values staged for one atomic push ([`HostBridgeHandle::batch`]); [`Self::commit`] pushes them.
///
/// ```no_run
/// # fn f(host: &daedalus_runtime::host_bridge::HostBridgeHandle) {
/// let outcomes = host
///     .batch()
///     .push("frame", 7_i64)
///     .push("imu", 0.5_f64)
///     .commit()
///     .expect("every value has the registered type");
/// assert!(outcomes.iter().all(|outcome| !matches!(
///     outcome,
///     daedalus_transport::FeedOutcome::Rejected(_)
/// )));
/// # }
/// ```
#[must_use = "a batch pushes nothing until committed"]
pub struct HostInputBatch<'h> {
    host: &'h HostBridgeHandle,
    types: TypeIndex,
    clock: Clock,
    entries: Staged,
}

impl HostInputBatch<'_> {
    /// Stage `value` under the key the graph's registry gives `T` (as
    /// [`HostBridgeHandle::push`]); a type without a single key rejects the batch on commit.
    pub fn push<T>(mut self, port: impl Into<PortId>, value: T) -> Self
    where
        T: Send + Sync + 'static,
    {
        let payload = self
            .types
            .key_of::<T>()
            .map(|key| Payload::owned(key, value).stamp(&self.clock))
            .map_err(Box::new);
        self.entries.push((port.into(), payload));
        self
    }

    /// Stage a prebuilt payload (as [`HostBridgeHandle::feed_payload`]).
    pub fn push_payload(mut self, port: impl Into<PortId>, payload: Payload) -> Self {
        self.entries.push((port.into(), Ok(payload)));
        self
    }

    /// Push every staged value atomically; see [`HostBridgeHandle::push_batch`].
    pub fn commit(self) -> Result<HostBatchOutcomes, HostBatchRejected> {
        self.host.commit_batch(self.entries)
    }
}

/// Per-value outcomes of a committed batch, in push order.
#[derive(Debug)]
pub struct HostBatchOutcomes(SmallVec<[FeedOutcome; INLINE]>);

impl Deref for HostBatchOutcomes {
    type Target = [FeedOutcome];

    fn deref(&self) -> &[FeedOutcome] {
        &self.0
    }
}

impl IntoIterator for HostBatchOutcomes {
    type Item = FeedOutcome;
    type IntoIter = smallvec::IntoIter<[FeedOutcome; INLINE]>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// A batch refused as a whole because one value failed the type check; nothing was pushed.
#[derive(Debug)]
pub struct HostBatchRejected {
    /// Position of the refused value in the batch.
    pub index: usize,
    pub port: PortId,
    pub error: TypeKeyError,
}

impl fmt::Display for HostBatchRejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "host batch rejected at value {} (port '{}'): {}",
            self.index, self.port, self.error
        )
    }
}

impl core::error::Error for HostBatchRejected {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl HostBridgeHandle {
    /// Start an atomic multi-port push: `host.batch().push("frame", f).push("imu", s).commit()`.
    pub fn batch(&self) -> HostInputBatch<'_> {
        let guard = self.shared.buffers.lock();
        HostInputBatch {
            host: self,
            types: guard.types.clone(),
            clock: guard.clock.clone(),
            entries: SmallVec::new(),
        }
    }

    /// Push several payloads atomically: all are enqueued under one bridge lock and inbound
    /// waiters are woken once, so no tick sees part of the batch.
    ///
    /// All or nothing on the type check: when any payload fails it (see [`Self::feed_payload`]),
    /// nothing is pushed and the first failure is returned. Otherwise each value gets its own
    /// outcome under its port's policy (accepted, replaced, or dropped when stale or closed).
    /// Batches of up to four values neither stage nor report on the heap.
    pub fn push_batch<P: Into<PortId>>(
        &self,
        entries: impl IntoIterator<Item = (P, Payload)>,
    ) -> Result<HostBatchOutcomes, HostBatchRejected> {
        self.commit_batch(
            entries
                .into_iter()
                .map(|(port, payload)| (port.into(), Ok(payload)))
                .collect(),
        )
    }

    /// Commit a batch as one feed: timed into the push row of frame-overhead reports and
    /// attributed to host-scope allocations, like single pushes.
    fn commit_batch(&self, entries: Staged) -> Result<HostBatchOutcomes, HostBatchRejected> {
        let _scope = io_timing::host_alloc_scope();
        if self.shared.io_timing.enabled() {
            return io_timing::timed(&self.shared.io_timing.push_ns, || {
                self.commit_locked(entries)
            });
        }
        self.commit_locked(entries)
    }

    fn commit_locked(&self, entries: Staged) -> Result<HostBatchOutcomes, HostBatchRejected> {
        let mut guard = self.shared.buffers.lock();
        let refused = entries
            .iter()
            .enumerate()
            .find_map(|(index, (_, payload))| {
                match payload {
                    Ok(payload) => guard.types.check_payload(payload).err(),
                    Err(error) => Some((**error).clone()),
                }
                .map(|error| (index, error))
            });
        if let Some((index, error)) = refused {
            return Err(HostBatchRejected {
                index,
                port: entries[index].0.clone(),
                error,
            });
        }
        let mut outcomes = SmallVec::new();
        let mut wake = false;
        for (port, payload) in entries {
            let Ok(payload) = payload else { continue };
            let (outcome, queued) = enqueue_locked(
                &mut guard,
                Direction::Inbound,
                self.alias.as_str(),
                PortKey::Id(port),
                payload,
            );
            wake |= queued;
            outcomes.push(outcome);
        }
        if wake {
            self.wake_after_feed(guard);
        }
        Ok(HostBatchOutcomes(outcomes))
    }
}

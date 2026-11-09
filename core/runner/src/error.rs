//! Errors carry no text. A fault's detail is written through `StepCx` (C12).

/// Why a plan was refused at load.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoadError {
    /// A slice is shorter than the plan needs, or the step and meta counts differ.
    Length,
    /// A span points outside the table it indexes.
    Span,
    /// A slot index is outside the slot list.
    SlotRange,
    /// A wake target is outside the step list.
    WakeRange,
    /// A wake target does not come after its producer, so the graph has a cycle or a back edge.
    BackEdge { from: u32, to: u32 },
    /// A slot has no capacity, or the queues do not fit in `u32`.
    Capacity,
}

/// A host's activity mask has the wrong length for the plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaskError {
    /// How many words the plan needs.
    pub expected: u32,
}

/// Why a tick could not finish. The step that stopped it is named, and the tick state is kept.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunError {
    /// The plan was refused at load.
    Load(LoadError),
    /// A step finished with a value its output slot could not take.
    Overflow { slot: u32 },
    /// A step became pending and no pending entry is free.
    PendingFull { step: u32 },
    /// A value was pushed to a slot that the plan does not have.
    NoSlot { slot: u32 },
    /// A value that owns its payload was pushed raw. Use the owning feed instead.
    NotRaw { slot: u32 },
    /// A typed payload was fed as a plain value. Its concrete type is needed: use `feed_typed`.
    Untyped { slot: u32 },
    /// A step faulted under a policy that stops the run (`fail_fast`). The tick's other steps keep
    /// their dirty bits, and the faulted step's tick was dropped.
    Fault { step: u32 },
    /// A cancel stopped the tick before `step` ran. That step and the ones after it are still
    /// dirty, and run on the next tick once the word is cleared.
    Cancelled { step: u32 },
    /// A host named a step the plan does not have.
    NoStep { step: u32 },
}

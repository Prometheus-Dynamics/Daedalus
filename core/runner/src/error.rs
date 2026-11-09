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
}

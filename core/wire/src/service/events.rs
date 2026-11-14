//! The events a watch delivers.

use super::fields::{bad, flag, get_opt_str, get_strs, put_opt_str, put_strs};
use super::requests::RunId;
use crate::WireError;
use crate::codec::{Reader, Writer};

/// Why a watch, or a run it follows, ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndReason {
    /// The run was stopped.
    Stopped,
    /// The run was cancelled.
    Cancelled,
    /// The client asked to unwatch.
    Unwatched,
    /// The server is shutting down.
    Shutdown,
}

impl EndReason {
    pub const ALL: [EndReason; 4] = [
        EndReason::Stopped,
        EndReason::Cancelled,
        EndReason::Unwatched,
        EndReason::Shutdown,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            EndReason::Stopped => "stopped",
            EndReason::Cancelled => "cancelled",
            EndReason::Unwatched => "unwatched",
            EndReason::Shutdown => "shutdown",
        }
    }
}

/// A step that failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FaultEvent {
    pub run: RunId,
    pub tick: u32,
    pub step: u32,
    /// The node at fault, when the body said or the step runs only one.
    pub node: Option<String>,
    /// Every node the step runs.
    pub covers: Vec<String>,
    /// Failures in a row.
    pub consecutive: u32,
    pub reason: String,
    /// The body's reason was longer than a fault keeps, and `reason` is only its start.
    pub reason_cut: bool,
    /// What the step gave the host beyond its reason, before it failed. Empty when it gave
    /// nothing, or the host keeps no detail.
    pub detail: String,
    /// The step wrote more detail than the host keeps, and `detail` is only its start.
    pub detail_cut: bool,
    /// The code of the failure: the node's own, unless `own_code`. Zero when the node gave
    /// none. Added after the first version: a server built before it sends none, and it reads
    /// as zero.
    pub code: u32,
    /// The code is one of Daedalus's own (`daedalus_abi::own`), and not the node's. Added with
    /// `code`.
    pub own_code: bool,
}

/// What one batch of ticks did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TelemetryEvent {
    pub run: RunId,
    pub tick: u64,
    pub step_calls: u64,
    pub faults: u64,
    pub skipped: u64,
}

/// What a watch delivers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventBody {
    Fault(FaultEvent),
    Telemetry(TelemetryEvent),
    /// The watcher was too slow: this many events were dropped, oldest first.
    Lagged {
        dropped: u64,
    },
    /// A run this watch follows ended, or the watch itself did. A watch on one run delivers
    /// nothing after this. `run` is zero when the watch ended without a run ending.
    Ended {
        run: RunId,
        reason: EndReason,
    },
    /// An event of a kind this side doesn't have: the server is newer. A watcher sees that
    /// something happened and what kind it was, and nothing is taken to be something else.
    Unknown {
        kind: u8,
    },
}

impl EventBody {
    pub(crate) fn encode(&self, out: &mut Writer) -> Result<(), WireError> {
        match self {
            EventBody::Fault(fault) => {
                out.u8(0);
                out.u64(fault.run);
                out.u32(fault.tick);
                out.u32(fault.step);
                put_opt_str(out, &fault.node)?;
                put_strs(out, &fault.covers)?;
                out.u32(fault.consecutive);
                out.str(&fault.reason)?;
                out.bool(fault.reason_cut);
                out.str(&fault.detail)?;
                out.bool(fault.detail_cut);
                out.u32(fault.code);
                out.bool(fault.own_code);
            }
            EventBody::Telemetry(event) => {
                out.u8(1);
                out.u64(event.run);
                out.u64(event.tick);
                out.u64(event.step_calls);
                out.u64(event.faults);
                out.u64(event.skipped);
            }
            EventBody::Lagged { dropped } => {
                out.u8(2);
                out.u64(*dropped);
            }
            EventBody::Ended { run, reason } => {
                out.u8(3);
                out.u64(*run);
                out.u8(*reason as u8);
            }
            EventBody::Unknown { kind } => out.u8(*kind),
        }
        Ok(())
    }

    pub(crate) fn decode(input: &mut Reader<'_>) -> Result<EventBody, WireError> {
        Ok(match input.u8()? {
            0 => {
                let mut fault = FaultEvent {
                    run: input.u64()?,
                    tick: input.u32()?,
                    step: input.u32()?,
                    node: get_opt_str(input)?,
                    covers: get_strs(input)?,
                    consecutive: input.u32()?,
                    reason: input.str()?,
                    reason_cut: flag(input, "reason cut")?,
                    detail: input.str()?,
                    detail_cut: flag(input, "detail cut")?,
                    code: 0,
                    own_code: false,
                };
                // Added together, after the first version: both are there, or neither is. A
                // code without its marker could be the host's own read as a node's.
                if input.more() {
                    fault.code = input.u32()?;
                    fault.own_code = flag(input, "own code")?;
                }
                EventBody::Fault(fault)
            }
            1 => EventBody::Telemetry(TelemetryEvent {
                run: input.u64()?,
                tick: input.u64()?,
                step_calls: input.u64()?,
                faults: input.u64()?,
                skipped: input.u64()?,
            }),
            2 => EventBody::Lagged {
                dropped: input.u64()?,
            },
            3 => {
                let run = input.u64()?;
                let tag = input.u8()?;
                let reason = EndReason::ALL
                    .into_iter()
                    .find(|reason| *reason as u8 == tag)
                    .ok_or(bad("end reason", u32::from(tag)))?;
                EventBody::Ended { run, reason }
            }
            kind => EventBody::Unknown { kind },
        })
    }
}

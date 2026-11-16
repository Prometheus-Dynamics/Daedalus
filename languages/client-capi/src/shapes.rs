//! The fixed-layout structs of the surface, and the checks that turn a caller's numbers into the
//! protocol's options. Every struct is `repr(C)` and its layout is in the header.

use daedalus_wire::service::{PlanForm, StartConfig, StopMode, TelemetryLevel};

use crate::codes::{FORM_BLOB, FORM_SOURCE, FORM_STEPS, STOP_DRAIN, STOP_IMMEDIATE};
use crate::codes::{TELEMETRY_BASIC, TELEMETRY_DETAILED, TELEMETRY_OFF, TELEMETRY_TRACE};
use crate::guard::{Text, fail, non_null, read_list};
use crate::status::Status;

/// The surface's version: the protocol's version and the hash of its layout. A caller built
/// against another layout should not trust what it reads.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Version {
    pub protocol: u32,
    pub layout_hash: u64,
}

/// How a start or a made plan is configured: the protocol's start options. `priority` names the
/// backends in the order ties go to; a null list with zero count leaves the server's order.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct Config {
    pub fifo_capacity: u32,
    pub fault_reports: u32,
    /// `DAEDALUS_CLIENT_TELEMETRY_*`.
    pub telemetry: u32,
    /// Non-zero runs every node as its own step.
    pub unmerged: u8,
    pub priority: *const Text,
    pub priority_len: usize,
}

/// How a plan is asked for.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PlanOptions {
    /// `DAEDALUS_CLIENT_FORM_*`.
    pub form: u32,
    /// Non-zero adds the planner's explanation to the reply.
    pub inspect: u8,
    pub priority: *const Text,
    pub priority_len: usize,
}

/// What a batch of ticks did. Written by `daedalus_client_step`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Step {
    pub tick: u64,
    pub step_calls: u64,
    pub faults: u64,
    pub skipped: u64,
    /// Ticks run by this call.
    pub ticks: u32,
    /// Non-zero when the last tick had nothing to do.
    pub idle: u8,
}

/// How a stop went. Written by `daedalus_client_stop`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stopped {
    /// The run's last tick.
    pub tick: u64,
    /// Values thrown away by an immediate stop.
    pub dropped: u32,
}

/// What a feed did. Written by the feed calls.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Fed {
    /// How many consumer steps were woken.
    pub woke: u32,
    /// Non-zero when the port's policy gave a value up for this one.
    pub dropped: u8,
}

/// How many of each thing a reload carried, started fresh, added, removed, and dropped. The
/// names themselves are not in the surface.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Carry {
    pub carried: u32,
    pub fresh: u32,
    pub added: u32,
    pub removed: u32,
    pub removed_ports: u32,
    pub dropped_values: u32,
}

fn telemetry(level: u32) -> Result<TelemetryLevel, Status> {
    Ok(match level {
        TELEMETRY_OFF => TelemetryLevel::Off,
        TELEMETRY_BASIC => TelemetryLevel::Basic,
        TELEMETRY_DETAILED => TelemetryLevel::Detailed,
        TELEMETRY_TRACE => TelemetryLevel::Trace,
        other => {
            return Err(fail(
                Status::BAD_ARGUMENT,
                format!("{other} is not a telemetry level"),
            ));
        }
    })
}

pub(crate) fn stop_mode(mode: u32) -> Result<StopMode, Status> {
    Ok(match mode {
        STOP_DRAIN => StopMode::Drain,
        STOP_IMMEDIATE => StopMode::Immediate,
        other => {
            return Err(fail(
                Status::BAD_ARGUMENT,
                format!("{other} is not a stop mode"),
            ));
        }
    })
}

fn form(form: u32) -> Result<PlanForm, Status> {
    Ok(match form {
        FORM_STEPS => PlanForm::Steps,
        FORM_BLOB => PlanForm::Blob,
        FORM_SOURCE => PlanForm::Source,
        other => {
            return Err(fail(
                Status::BAD_ARGUMENT,
                format!("{other} is not a plan form"),
            ));
        }
    })
}

/// The start options a caller gave, checked.
///
/// # Safety
/// `config` is not null, and its `priority` is as [`read_list`] requires.
pub(crate) unsafe fn start_config(config: *const Config) -> Result<StartConfig, Status> {
    if config.is_null() {
        return Err(fail(Status::NULL_POINTER, "the start options are null"));
    }
    // SAFETY: `config` is not null, and the caller says it points to a `Config`.
    let config = unsafe { *config };
    // SAFETY: the caller's contract for the list is this function's.
    let priority = unsafe { read_list(config.priority, config.priority_len) }?;
    Ok(StartConfig {
        fifo_capacity: config.fifo_capacity,
        fault_reports: config.fault_reports,
        telemetry: telemetry(config.telemetry)?,
        unmerged: config.unmerged != 0,
        priority,
    })
}

/// The plan options a caller gave, checked: the form, whether to explain, and the priority.
///
/// # Safety
/// `options` is not null, and its `priority` is as [`read_list`] requires.
pub(crate) unsafe fn plan_options(
    options: *const PlanOptions,
) -> Result<(PlanForm, bool, Vec<String>), Status> {
    non_null(options.cast_mut(), "the plan options")?;
    // SAFETY: `options` is not null, and the caller says it points to a `PlanOptions`.
    let options = unsafe { *options };
    // SAFETY: the caller's contract for the list is this function's.
    let priority = unsafe { read_list(options.priority, options.priority_len) }?;
    Ok((form(options.form)?, options.inspect != 0, priority))
}

#[cfg(test)]
mod tests {
    use super::{Version, form, stop_mode, telemetry};
    use crate::status::Status;

    #[test]
    fn a_number_that_names_no_option_is_a_bad_argument() {
        assert_eq!(form(9).err(), Some(Status::BAD_ARGUMENT));
        assert_eq!(telemetry(9).err(), Some(Status::BAD_ARGUMENT));
        assert_eq!(stop_mode(2).err(), Some(Status::BAD_ARGUMENT));
        assert_eq!(Version::default().protocol, 0);
    }
}

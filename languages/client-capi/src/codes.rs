//! The small numbers a call takes: which plan form, which telemetry level, which stop mode,
//! which kind of inspect, which events a watch asks for. Each is named in the header.

/// Plan forms: the plan is returned as its steps, as a made blob, or as its source text.
pub const FORM_STEPS: u32 = 0;
pub const FORM_BLOB: u32 = 1;
pub const FORM_SOURCE: u32 = 2;

/// Telemetry levels a run records at.
pub const TELEMETRY_OFF: u32 = 0;
pub const TELEMETRY_BASIC: u32 = 1;
pub const TELEMETRY_DETAILED: u32 = 2;
pub const TELEMETRY_TRACE: u32 = 3;

/// How a run stops: what is waiting is finished first, or dropped.
pub const STOP_DRAIN: u32 = 0;
pub const STOP_IMMEDIATE: u32 = 1;

/// What the inspect calls look at. Each has a function of its own.
pub const INSPECT_RUN: u32 = 0;
pub const INSPECT_GRAPH: u32 = 1;
pub const INSPECT_CACHE: u32 = 2;
pub const INSPECT_TELEMETRY: u32 = 3;
pub const INSPECT_HOST: u32 = 4;

/// The bits of the events a watch asks for.
pub const EVENT_FAULT: u32 = 1;
pub const EVENT_TELEMETRY: u32 = 2;
pub const EVENT_ALL: u32 = 3;

/// The run a watch follows when it follows every run. Run numbers start at one.
pub const ALL_RUNS: u64 = 0;

/// What `daedalus_client_watch_next` found.
pub const WATCH_IDLE: u32 = 0;
pub const WATCH_EVENT: u32 = 1;
pub const WATCH_ENDED: u32 = 2;

/// Every named number with its header name, for the header test.
pub const CONSTANTS: &[(&str, u64)] = &[
    ("DAEDALUS_CLIENT_FORM_STEPS", FORM_STEPS as u64),
    ("DAEDALUS_CLIENT_FORM_BLOB", FORM_BLOB as u64),
    ("DAEDALUS_CLIENT_FORM_SOURCE", FORM_SOURCE as u64),
    ("DAEDALUS_CLIENT_TELEMETRY_OFF", TELEMETRY_OFF as u64),
    ("DAEDALUS_CLIENT_TELEMETRY_BASIC", TELEMETRY_BASIC as u64),
    (
        "DAEDALUS_CLIENT_TELEMETRY_DETAILED",
        TELEMETRY_DETAILED as u64,
    ),
    ("DAEDALUS_CLIENT_TELEMETRY_TRACE", TELEMETRY_TRACE as u64),
    ("DAEDALUS_CLIENT_STOP_DRAIN", STOP_DRAIN as u64),
    ("DAEDALUS_CLIENT_STOP_IMMEDIATE", STOP_IMMEDIATE as u64),
    ("DAEDALUS_CLIENT_INSPECT_RUN", INSPECT_RUN as u64),
    ("DAEDALUS_CLIENT_INSPECT_GRAPH", INSPECT_GRAPH as u64),
    ("DAEDALUS_CLIENT_INSPECT_CACHE", INSPECT_CACHE as u64),
    (
        "DAEDALUS_CLIENT_INSPECT_TELEMETRY",
        INSPECT_TELEMETRY as u64,
    ),
    ("DAEDALUS_CLIENT_INSPECT_HOST", INSPECT_HOST as u64),
    ("DAEDALUS_CLIENT_EVENT_FAULT", EVENT_FAULT as u64),
    ("DAEDALUS_CLIENT_EVENT_TELEMETRY", EVENT_TELEMETRY as u64),
    ("DAEDALUS_CLIENT_EVENT_ALL", EVENT_ALL as u64),
    ("DAEDALUS_CLIENT_ALL_RUNS", ALL_RUNS),
    ("DAEDALUS_CLIENT_WATCH_IDLE", WATCH_IDLE as u64),
    ("DAEDALUS_CLIENT_WATCH_EVENT", WATCH_EVENT as u64),
    ("DAEDALUS_CLIENT_WATCH_ENDED", WATCH_ENDED as u64),
];

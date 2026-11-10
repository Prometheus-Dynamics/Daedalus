//! Every status the library returns, as an error.

use std::fmt;

use daedalus_capi_sys::{Api, Code, Version};

/// Why a call failed. One variant for each status of the surface, with the library's reason
/// where it gives one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostError {
    /// The shared library couldn't be loaded, or isn't the surface.
    Library(String),
    /// The library is another version than this crate was built for.
    Version {
        found: Version,
        wanted_major: u32,
        wanted_layout_hash: u64,
    },
    NullPointer,
    BadHandle,
    /// The host or graph was already released.
    StaleHandle,
    /// The host still has graphs.
    Busy,
    BadText,
    BufferTooSmall,
    /// The graph has no host port of that name or index.
    UnknownPort,
    /// A push to an output, or a take from an input.
    WrongDirection,
    /// The port's type doesn't take, or give, this kind of value.
    TypeMismatch,
    /// The number doesn't fit the port's type.
    OutOfRange,
    PluginFailed(String),
    /// The graph's text couldn't be read.
    GraphInvalid(String),
    /// The graph can't be planned. The text says why.
    PlanFailed(String),
    LoadFailed(String),
    /// The graph refused a value, or a tick failed.
    RunFailed(String),
    /// An entry of a batch can't be pushed, and nothing was.
    BadBatch {
        entry: Option<usize>,
        reason: String,
    },
    /// A call panicked inside the library. The handles are still good.
    Panic(String),
    /// A status this crate doesn't know: the library is newer.
    Unknown {
        code: Code,
        name: String,
    },
}

impl HostError {
    /// The error for a status the library returned. Reads the library's reason for it.
    pub(crate) fn from_code(api: &Api, code: Code) -> HostError {
        let reason = || api.last_error();
        match code {
            1 => HostError::NullPointer,
            2 => HostError::BadHandle,
            3 => HostError::StaleHandle,
            4 => HostError::Busy,
            5 => HostError::BadText,
            6 => HostError::BufferTooSmall,
            7 => HostError::UnknownPort,
            8 => HostError::WrongDirection,
            9 => HostError::TypeMismatch,
            10 => HostError::OutOfRange,
            11 => HostError::PluginFailed(reason()),
            12 => HostError::GraphInvalid(reason()),
            13 => HostError::PlanFailed(reason()),
            14 => HostError::LoadFailed(reason()),
            15 => HostError::RunFailed(reason()),
            16 => HostError::BadBatch {
                entry: None,
                reason: reason(),
            },
            17 => HostError::Panic(reason()),
            code => HostError::Unknown {
                code,
                name: api.status_name(code),
            },
        }
    }

    /// The status number this error stands for, or `None` for one that isn't a status.
    pub fn code(&self) -> Option<Code> {
        Some(match self {
            HostError::Library(_) | HostError::Version { .. } => return None,
            HostError::NullPointer => 1,
            HostError::BadHandle => 2,
            HostError::StaleHandle => 3,
            HostError::Busy => 4,
            HostError::BadText => 5,
            HostError::BufferTooSmall => 6,
            HostError::UnknownPort => 7,
            HostError::WrongDirection => 8,
            HostError::TypeMismatch => 9,
            HostError::OutOfRange => 10,
            HostError::PluginFailed(_) => 11,
            HostError::GraphInvalid(_) => 12,
            HostError::PlanFailed(_) => 13,
            HostError::LoadFailed(_) => 14,
            HostError::RunFailed(_) => 15,
            HostError::BadBatch { .. } => 16,
            HostError::Panic(_) => 17,
            HostError::Unknown { code, .. } => *code,
        })
    }
}

impl fmt::Display for HostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HostError::Library(why) => write!(f, "{why}"),
            HostError::Version {
                found,
                wanted_major,
                wanted_layout_hash,
            } => write!(
                f,
                "the library is version {}.{} with layout {:#x}, and this was built for major {wanted_major} with layout {wanted_layout_hash:#x}",
                found.major, found.minor, found.layout_hash
            ),
            HostError::NullPointer => f.write_str("a null pointer"),
            HostError::BadHandle => f.write_str("not a handle"),
            HostError::StaleHandle => f.write_str("the handle was released"),
            HostError::Busy => f.write_str("the host still has graphs"),
            HostError::BadText => f.write_str("text that isn't UTF-8"),
            HostError::BufferTooSmall => f.write_str("the buffer is too small"),
            HostError::UnknownPort => f.write_str("the graph has no such host port"),
            HostError::WrongDirection => {
                f.write_str("a push to an output, or a take from an input")
            }
            HostError::TypeMismatch => {
                f.write_str("the port's type doesn't hold this kind of value")
            }
            HostError::OutOfRange => f.write_str("the number doesn't fit the port's type"),
            HostError::PluginFailed(why) => write!(f, "the plugin couldn't be installed: {why}"),
            HostError::GraphInvalid(why) => write!(f, "the graph couldn't be read: {why}"),
            HostError::PlanFailed(why) => write!(f, "the graph can't be planned: {why}"),
            HostError::LoadFailed(why) => write!(f, "the plan couldn't be loaded: {why}"),
            HostError::RunFailed(why) => write!(f, "the graph refused: {why}"),
            HostError::BadBatch { entry, reason } => match entry {
                Some(entry) => write!(f, "nothing was pushed, entry {entry} can't be: {reason}"),
                None => write!(f, "nothing was pushed: {reason}"),
            },
            HostError::Panic(why) => write!(f, "{why}"),
            HostError::Unknown { code, name } => write!(f, "status {code} ({name})"),
        }
    }
}

impl std::error::Error for HostError {}

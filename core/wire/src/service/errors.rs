//! The service's errors: one closed set of codes, a message, and the planner's diagnostics when
//! planning failed.

use core::fmt;

use super::fields::{get_list, get_opt_str, put_list, put_opt_str};
use crate::codec::{Reader, Writer};
use crate::{PacketError, WireError};

/// Why a request was refused. The set is closed: a response names one of these.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    /// The request could not be read, or names something that makes no sense.
    InvalidRequest,
    UnknownGraph,
    UnknownRun,
    UnknownPort,
    /// A value does not have the type its port carries.
    ValueType,
    /// A plugin could not be loaded.
    PluginError,
    /// The planner refused the graph. The diagnostics say why.
    PlanningFailed,
    /// The host is at a limit, or the run is doing something that excludes the request.
    Busy,
    Cancelled,
    Stopped,
    /// A packet over the cap.
    FrameTooLarge,
    /// Something this server does not do, or a value whose type has no wire codec.
    Unsupported,
}

impl ErrorCode {
    pub const ALL: [ErrorCode; 12] = [
        ErrorCode::InvalidRequest,
        ErrorCode::UnknownGraph,
        ErrorCode::UnknownRun,
        ErrorCode::UnknownPort,
        ErrorCode::ValueType,
        ErrorCode::PluginError,
        ErrorCode::PlanningFailed,
        ErrorCode::Busy,
        ErrorCode::Cancelled,
        ErrorCode::Stopped,
        ErrorCode::FrameTooLarge,
        ErrorCode::Unsupported,
    ];

    pub const fn code(self) -> u16 {
        match self {
            ErrorCode::InvalidRequest => 1,
            ErrorCode::UnknownGraph => 2,
            ErrorCode::UnknownRun => 3,
            ErrorCode::UnknownPort => 4,
            ErrorCode::ValueType => 5,
            ErrorCode::PluginError => 6,
            ErrorCode::PlanningFailed => 7,
            ErrorCode::Busy => 8,
            ErrorCode::Cancelled => 9,
            ErrorCode::Stopped => 10,
            ErrorCode::FrameTooLarge => 11,
            ErrorCode::Unsupported => 12,
        }
    }

    pub fn from_code(code: u16) -> Result<ErrorCode, WireError> {
        ErrorCode::ALL
            .into_iter()
            .find(|known| known.code() == code)
            .ok_or(WireError::BadTag {
                what: "error code",
                tag: u32::from(code),
            })
    }

    /// The name a log or a command line shows.
    pub const fn name(self) -> &'static str {
        match self {
            ErrorCode::InvalidRequest => "invalid_request",
            ErrorCode::UnknownGraph => "unknown_graph",
            ErrorCode::UnknownRun => "unknown_run",
            ErrorCode::UnknownPort => "unknown_port",
            ErrorCode::ValueType => "value_type",
            ErrorCode::PluginError => "plugin_error",
            ErrorCode::PlanningFailed => "planning_failed",
            ErrorCode::Busy => "busy",
            ErrorCode::Cancelled => "cancelled",
            ErrorCode::Stopped => "stopped",
            ErrorCode::FrameTooLarge => "frame_too_large",
            ErrorCode::Unsupported => "unsupported",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// How serious a diagnostic is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
}

/// One thing the planner or a lint found in a graph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Diagnostic {
    pub severity: Severity,
    /// The planner's code for it, as text.
    pub code: String,
    pub message: String,
    pub node: Option<String>,
    pub port: Option<String>,
}

impl Diagnostic {
    pub(crate) fn encode(&self, out: &mut Writer) -> Result<(), WireError> {
        out.u8(match self.severity {
            Severity::Error => 0,
            Severity::Warning => 1,
        });
        out.str(&self.code)?;
        out.str(&self.message)?;
        put_opt_str(out, &self.node)?;
        put_opt_str(out, &self.port)
    }

    pub(crate) fn decode(input: &mut Reader<'_>) -> Result<Diagnostic, WireError> {
        let severity = match input.u8()? {
            0 => Severity::Error,
            1 => Severity::Warning,
            tag => {
                return Err(WireError::BadTag {
                    what: "severity",
                    tag: u32::from(tag),
                });
            }
        };
        Ok(Diagnostic {
            severity,
            code: input.str()?,
            message: input.str()?,
            node: get_opt_str(input)?,
            port: get_opt_str(input)?,
        })
    }
}

/// A refused request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceError {
    pub code: ErrorCode,
    pub message: String,
    /// What the planner found. Only `planning_failed` fills this.
    pub diagnostics: Vec<Diagnostic>,
}

impl ServiceError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> ServiceError {
        ServiceError {
            code,
            message: message.into(),
            diagnostics: Vec::new(),
        }
    }

    pub fn planning(message: impl Into<String>, diagnostics: Vec<Diagnostic>) -> ServiceError {
        ServiceError {
            code: ErrorCode::PlanningFailed,
            message: message.into(),
            diagnostics,
        }
    }

    /// The error a body that could not be decoded gets. A value with no codec is `unsupported`,
    /// and everything else is `invalid_request`.
    pub fn from_wire(error: &WireError) -> ServiceError {
        let code = match error {
            WireError::NoCodec(_) => ErrorCode::Unsupported,
            WireError::Protocol { .. } | WireError::Layout { .. } => ErrorCode::Unsupported,
            WireError::ValueShape { .. } => ErrorCode::ValueType,
            _ => ErrorCode::InvalidRequest,
        };
        ServiceError::new(code, error.to_string())
    }

    /// The error a packet that could not be read gets, when the channel is still usable for one.
    pub fn from_packet(error: &PacketError) -> ServiceError {
        let code = match error {
            PacketError::TooLarge { .. } => ErrorCode::FrameTooLarge,
            _ => ErrorCode::InvalidRequest,
        };
        ServiceError::new(code, error.to_string())
    }

    pub(crate) fn encode(&self, out: &mut Writer) -> Result<(), WireError> {
        out.u16(self.code.code());
        out.str(&self.message)?;
        put_list(out, &self.diagnostics, |out, item| item.encode(out))
    }

    pub(crate) fn decode(input: &mut Reader<'_>) -> Result<ServiceError, WireError> {
        Ok(ServiceError {
            code: ErrorCode::from_code(input.u16()?)?,
            message: input.str()?,
            diagnostics: get_list(input, Diagnostic::decode)?,
        })
    }
}

impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ServiceError {}

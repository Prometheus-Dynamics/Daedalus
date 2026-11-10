//! The messages between the host and a worker. A message is one kind byte, then its fields in
//! fixed order. Values are typed by the schema: the host encodes what it sends as the worker's
//! declared port types, and the worker does the same for what it returns.

use crate::WireError;
use crate::codec::{Reader, Writer};
use crate::hello::Hello;
use crate::schema::{Dir, Schema};
use crate::value::{WireValue, decode_value, encode_value};

/// Most bytes a fault's reason keeps. Longer text is cut at a character boundary.
pub const REASON_CAP: usize = 512;

/// The kind bytes, one per message. Changing one changes [`crate::LAYOUT_HASH`].
pub mod kind {
    pub const RUN: u8 = 0x01;
    pub const CANCEL: u8 = 0x02;
    pub const STATE_EXPORT: u8 = 0x03;
    pub const STATE_IMPORT: u8 = 0x04;
    pub const PING: u8 = 0x05;
    pub const SHUTDOWN: u8 = 0x06;

    pub const HELLO: u8 = 0x40;
    pub const OUTPUTS: u8 = 0x41;
    pub const PROGRESS: u8 = 0x42;
    pub const LOG: u8 = 0x43;
    pub const FAULT: u8 = 0x44;
    pub const STATE_VALUE: u8 = 0x45;
    pub const PONG: u8 = 0x46;
}

/// One boundary value: which node's port it is for, and the value.
#[derive(Clone, Debug, PartialEq)]
pub struct BoundaryValue {
    pub node: String,
    pub port: String,
    pub value: WireValue,
}

/// Why a fault happened. The set is closed: a fault names one of these.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultCode {
    /// A node returned an error.
    NodeError,
    /// A node panicked. The worker kept serving.
    NodePanic,
    /// An input did not match the node's declaration.
    BadInput,
    /// The node named is not one the worker serves.
    UnknownNode,
    /// The request asks for something this worker does not do.
    Unsupported,
    /// The worker failed for a reason of its own.
    Internal,
    /// The host sent a packet the worker could not read.
    MalformedPacket,
}

impl FaultCode {
    pub const ALL: [FaultCode; 7] = [
        FaultCode::NodeError,
        FaultCode::NodePanic,
        FaultCode::BadInput,
        FaultCode::UnknownNode,
        FaultCode::Unsupported,
        FaultCode::Internal,
        FaultCode::MalformedPacket,
    ];

    pub const fn code(self) -> u16 {
        match self {
            FaultCode::NodeError => 1,
            FaultCode::NodePanic => 2,
            FaultCode::BadInput => 3,
            FaultCode::UnknownNode => 4,
            FaultCode::Unsupported => 5,
            FaultCode::Internal => 6,
            FaultCode::MalformedPacket => 7,
        }
    }

    /// The text name a log or report shows.
    pub const fn name(self) -> &'static str {
        match self {
            FaultCode::NodeError => "node_error",
            FaultCode::NodePanic => "node_panic",
            FaultCode::BadInput => "bad_input",
            FaultCode::UnknownNode => "unknown_node",
            FaultCode::Unsupported => "unsupported",
            FaultCode::Internal => "internal",
            FaultCode::MalformedPacket => "malformed_packet",
        }
    }

    pub fn from_code(code: u16) -> Result<FaultCode, WireError> {
        FaultCode::ALL
            .into_iter()
            .find(|known| known.code() == code)
            .ok_or(WireError::BadTag {
                what: "fault code",
                tag: u32::from(code),
            })
    }
}

/// How a run ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunStatus {
    Ok,
    Cancelled,
    Fault,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    fn tag(self) -> u8 {
        self as u8
    }

    fn from_tag(tag: u8) -> Result<LogLevel, WireError> {
        Ok(match tag {
            0 => LogLevel::Error,
            1 => LogLevel::Warn,
            2 => LogLevel::Info,
            3 => LogLevel::Debug,
            4 => LogLevel::Trace,
            _ => {
                return Err(WireError::BadTag {
                    what: "log level",
                    tag: u32::from(tag),
                });
            }
        })
    }
}

/// Cuts text to at most `cap` bytes, at a character boundary.
pub fn truncate_reason(text: &str, cap: usize) -> &str {
    if text.len() <= cap {
        return text;
    }
    let mut end = cap;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

#[derive(Clone, Debug, PartialEq)]
pub struct Run {
    pub run: u64,
    pub region: u32,
    /// The nodes to run, in order.
    pub nodes: Vec<String>,
    /// Each value is typed by its node's input port.
    pub inputs: Vec<BoundaryValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum HostMsg {
    Run(Run),
    Cancel {
        run: u64,
    },
    StateExport {
        node: String,
    },
    /// Typed by the node's state.
    StateImport {
        node: String,
        value: WireValue,
    },
    Ping,
    Shutdown,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Outputs {
    pub run: u64,
    pub status: RunStatus,
    /// Each value is typed by its node's output port.
    pub outputs: Vec<BoundaryValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Progress {
    pub run: u64,
    pub node_index: u32,
    pub message: String,
    pub fraction: Option<f32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Fault {
    pub run: u64,
    pub node: Option<String>,
    pub code: FaultCode,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum WorkerMsg {
    Hello(Hello),
    Outputs(Outputs),
    Progress(Progress),
    Log { level: LogLevel, text: String },
    Fault(Fault),
    StateValue { node: String, value: WireValue },
    Pong,
}

fn put_boundary(
    values: &[BoundaryValue],
    dir: Dir,
    schema: &Schema,
    out: &mut Writer,
) -> Result<(), WireError> {
    out.len(values.len())?;
    for entry in values {
        out.str(&entry.node)?;
        out.str(&entry.port)?;
        encode_value(
            schema.lookup(&entry.node, dir, &entry.port)?,
            &entry.value,
            schema,
            out,
        )?;
    }
    Ok(())
}

fn take_boundary(
    dir: Dir,
    schema: &Schema,
    input: &mut Reader<'_>,
) -> Result<Vec<BoundaryValue>, WireError> {
    let count = input.count()?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let node = input.str()?;
        let port = input.str()?;
        let ty = schema.lookup(&node, dir, &port)?;
        let value = decode_value(ty, input, schema)?;
        values.push(BoundaryValue { node, port, value });
    }
    Ok(values)
}

impl HostMsg {
    pub fn encode(&self, schema: &Schema, out: &mut Writer) -> Result<(), WireError> {
        match self {
            HostMsg::Run(run) => {
                out.u8(kind::RUN);
                out.u64(run.run);
                out.u32(run.region);
                out.len(run.nodes.len())?;
                for node in &run.nodes {
                    out.str(node)?;
                }
                put_boundary(&run.inputs, Dir::Input, schema, out)?;
            }
            HostMsg::Cancel { run } => {
                out.u8(kind::CANCEL);
                out.u64(*run);
            }
            HostMsg::StateExport { node } => {
                out.u8(kind::STATE_EXPORT);
                out.str(node)?;
            }
            HostMsg::StateImport { node, value } => {
                out.u8(kind::STATE_IMPORT);
                out.str(node)?;
                encode_value(schema.lookup(node, Dir::State, "")?, value, schema, out)?;
            }
            HostMsg::Ping => out.u8(kind::PING),
            HostMsg::Shutdown => out.u8(kind::SHUTDOWN),
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8], schema: &Schema) -> Result<HostMsg, WireError> {
        let mut input = Reader::new(bytes);
        let msg = match input.u8()? {
            kind::RUN => {
                let run = input.u64()?;
                let region = input.u32()?;
                let count = input.count()?;
                let mut nodes = Vec::with_capacity(count);
                for _ in 0..count {
                    nodes.push(input.str()?);
                }
                let inputs = take_boundary(Dir::Input, schema, &mut input)?;
                HostMsg::Run(Run {
                    run,
                    region,
                    nodes,
                    inputs,
                })
            }
            kind::CANCEL => HostMsg::Cancel { run: input.u64()? },
            kind::STATE_EXPORT => HostMsg::StateExport { node: input.str()? },
            kind::STATE_IMPORT => {
                let node = input.str()?;
                let value =
                    decode_value(schema.lookup(&node, Dir::State, "")?, &mut input, schema)?;
                HostMsg::StateImport { node, value }
            }
            kind::PING => HostMsg::Ping,
            kind::SHUTDOWN => HostMsg::Shutdown,
            other => return Err(WireError::UnknownKind(other)),
        };
        input.finish()?;
        Ok(msg)
    }
}

impl WorkerMsg {
    pub fn encode(&self, schema: &Schema, out: &mut Writer) -> Result<(), WireError> {
        match self {
            WorkerMsg::Hello(hello) => {
                out.u8(kind::HELLO);
                hello.encode(out)?;
            }
            WorkerMsg::Outputs(outputs) => {
                out.u8(kind::OUTPUTS);
                out.u64(outputs.run);
                out.u8(match outputs.status {
                    RunStatus::Ok => 0,
                    RunStatus::Cancelled => 1,
                    RunStatus::Fault => 2,
                });
                put_boundary(&outputs.outputs, Dir::Output, schema, out)?;
            }
            WorkerMsg::Progress(progress) => {
                out.u8(kind::PROGRESS);
                out.u64(progress.run);
                out.u32(progress.node_index);
                out.str(&progress.message)?;
                match progress.fraction {
                    None => out.u8(0),
                    Some(fraction) => {
                        out.u8(1);
                        out.f32(fraction);
                    }
                }
            }
            WorkerMsg::Log { level, text } => {
                out.u8(kind::LOG);
                out.u8(level.tag());
                out.str(text)?;
            }
            WorkerMsg::Fault(fault) => {
                out.u8(kind::FAULT);
                out.u64(fault.run);
                match &fault.node {
                    None => out.u8(0),
                    Some(node) => {
                        out.u8(1);
                        out.str(node)?;
                    }
                }
                out.u16(fault.code.code());
                out.str(truncate_reason(&fault.reason, REASON_CAP))?;
            }
            WorkerMsg::StateValue { node, value } => {
                out.u8(kind::STATE_VALUE);
                out.str(node)?;
                encode_value(schema.lookup(node, Dir::State, "")?, value, schema, out)?;
            }
            WorkerMsg::Pong => out.u8(kind::PONG),
        }
        Ok(())
    }

    pub fn decode(bytes: &[u8], schema: &Schema) -> Result<WorkerMsg, WireError> {
        let mut input = Reader::new(bytes);
        let msg = match input.u8()? {
            kind::HELLO => WorkerMsg::Hello(Hello::decode(&mut input)?),
            kind::OUTPUTS => {
                let run = input.u64()?;
                let status = match input.u8()? {
                    0 => RunStatus::Ok,
                    1 => RunStatus::Cancelled,
                    2 => RunStatus::Fault,
                    tag => {
                        return Err(WireError::BadTag {
                            what: "run status",
                            tag: u32::from(tag),
                        });
                    }
                };
                let outputs = take_boundary(Dir::Output, schema, &mut input)?;
                WorkerMsg::Outputs(Outputs {
                    run,
                    status,
                    outputs,
                })
            }
            kind::PROGRESS => {
                let run = input.u64()?;
                let node_index = input.u32()?;
                let message = input.str()?;
                let fraction = match input.u8()? {
                    0 => None,
                    1 => Some(input.f32()?),
                    tag => {
                        return Err(WireError::BadTag {
                            what: "fraction flag",
                            tag: u32::from(tag),
                        });
                    }
                };
                WorkerMsg::Progress(Progress {
                    run,
                    node_index,
                    message,
                    fraction,
                })
            }
            kind::LOG => {
                let level = LogLevel::from_tag(input.u8()?)?;
                WorkerMsg::Log {
                    level,
                    text: input.str()?,
                }
            }
            kind::FAULT => {
                let run = input.u64()?;
                let node = match input.u8()? {
                    0 => None,
                    1 => Some(input.str()?),
                    tag => {
                        return Err(WireError::BadTag {
                            what: "node flag",
                            tag: u32::from(tag),
                        });
                    }
                };
                let code = FaultCode::from_code(input.u16()?)?;
                let reason = truncate_reason(&input.str()?, REASON_CAP).to_owned();
                WorkerMsg::Fault(Fault {
                    run,
                    node,
                    code,
                    reason,
                })
            }
            kind::STATE_VALUE => {
                let node = input.str()?;
                let value =
                    decode_value(schema.lookup(&node, Dir::State, "")?, &mut input, schema)?;
                WorkerMsg::StateValue { node, value }
            }
            kind::PONG => WorkerMsg::Pong,
            other => return Err(WireError::UnknownKind(other)),
        };
        input.finish()?;
        Ok(msg)
    }
}

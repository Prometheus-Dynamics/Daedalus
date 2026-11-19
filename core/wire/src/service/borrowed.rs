//! A request read in place: what a server serves on every tick (feed, step, take) keeps its names
//! as slices of the packet it was read from, so serving it copies nothing. Every other op is
//! decoded as it always was, into an owned [`Op`].

use crate::Schema;
use crate::WireError;
use crate::codec::Reader;

use super::{ClientMsg, Op, OpKind, Request, RunId, SESSION_CAP, ServiceHello, TypedValue, kind};

/// A request whose names point into the packet it was read from.
#[derive(Clone, Debug, PartialEq)]
pub struct RequestRef<'a> {
    pub id: u64,
    pub session: &'a str,
    pub op: OpRef<'a>,
}

/// The ops a client sends on every tick, read in place, and every other op, owned.
#[derive(Clone, Debug, PartialEq)]
pub enum OpRef<'a> {
    Feed {
        run: RunId,
        port: &'a str,
        value: TypedValue,
    },
    Step {
        run: RunId,
        count: u32,
    },
    Take {
        run: RunId,
        port: &'a str,
    },
    Owned(Op),
}

impl OpRef<'_> {
    pub fn kind(&self) -> OpKind {
        match self {
            OpRef::Feed { .. } => OpKind::Feed,
            OpRef::Step { .. } => OpKind::Step,
            OpRef::Take { .. } => OpKind::Take,
            OpRef::Owned(op) => op.kind(),
        }
    }
}

impl RequestRef<'_> {
    /// The same request, owned.
    pub fn to_owned(&self) -> Request {
        let op = match &self.op {
            OpRef::Feed { run, port, value } => Op::Feed {
                run: *run,
                port: (*port).to_owned(),
                value: value.clone(),
            },
            OpRef::Step { run, count } => Op::Step {
                run: *run,
                count: *count,
            },
            OpRef::Take { run, port } => Op::Take {
                run: *run,
                port: (*port).to_owned(),
            },
            OpRef::Owned(op) => op.clone(),
        };
        Request {
            id: self.id,
            session: self.session.to_owned(),
            op,
        }
    }
}

/// A client's message, with a request read in place.
#[derive(Clone, Debug, PartialEq)]
pub enum ClientMsgRef<'a> {
    Hello(ServiceHello),
    Request(RequestRef<'a>),
}

impl ClientMsg {
    /// Reads a message as [`ClientMsg::decode`] does, keeping a feed's, step's or take's names
    /// in `body`. Nothing is served until the whole request has been read, so a request that
    /// fails to decode changes nothing.
    pub fn decode_ref<'a>(body: &'a [u8], codecs: &Schema) -> Result<ClientMsgRef<'a>, WireError> {
        let mut input = Reader::new(body);
        match input.u8()? {
            kind::CLIENT_HELLO => Ok(ClientMsgRef::Hello(ServiceHello::decode(&mut input)?)),
            kind::REQUEST => {
                let id = input.u64()?;
                let session = input.str_ref()?;
                if session.len() > SESSION_CAP {
                    return Err(WireError::TooLong(session.len()));
                }
                let op = match OpKind::from_byte(input.peek_u8()?)? {
                    OpKind::Feed => {
                        input.u8()?;
                        OpRef::Feed {
                            run: input.u64()?,
                            port: input.str_ref()?,
                            value: TypedValue::decode(&mut input, codecs)?,
                        }
                    }
                    OpKind::Step => {
                        input.u8()?;
                        OpRef::Step {
                            run: input.u64()?,
                            count: input.u32()?,
                        }
                    }
                    OpKind::Take => {
                        input.u8()?;
                        OpRef::Take {
                            run: input.u64()?,
                            port: input.str_ref()?,
                        }
                    }
                    _ => OpRef::Owned(Op::decode(&mut input, codecs)?),
                };
                // What is left is fields from a later version, at the end of the message.
                Ok(ClientMsgRef::Request(RequestRef { id, session, op }))
            }
            other => Err(WireError::UnknownKind(other)),
        }
    }
}

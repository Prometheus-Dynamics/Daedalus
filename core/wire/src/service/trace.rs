//! The trace of the last [`TRACE_RING`] served requests, kept so that recording one allocates
//! nothing: the ring is sized when it is made, and a feed's, step's or take's names are copied into
//! the entry itself. A name longer than an entry holds, which only the ops that are not served on
//! every tick have (a plugin's path, a graph's name), is kept as a string.

use std::collections::VecDeque;

use super::{ErrorCode, Op, OpKind, OpRef, Request, RequestRef, RunId, TRACE_RING, TraceEntry};

/// A port name of up to this many bytes is kept in the entry.
const PORT_INLINE: usize = 48;
/// A session name is at most [`super::SESSION_CAP`] bytes, and is kept in the entry.
const SESSION_INLINE: usize = super::SESSION_CAP;

#[derive(Debug)]
enum Name<const N: usize> {
    Inline { len: u8, bytes: [u8; N] },
    Owned(String),
}

impl<const N: usize> Name<N> {
    fn new(text: &str) -> Name<N> {
        match text.len() <= N && N <= u8::MAX as usize {
            true => {
                let mut bytes = [0u8; N];
                bytes[..text.len()].copy_from_slice(text.as_bytes());
                Name::Inline {
                    len: text.len() as u8,
                    bytes,
                }
            }
            false => Name::Owned(text.to_owned()),
        }
    }

    fn as_str(&self) -> &str {
        match self {
            // Copied whole from a `&str`, so it is UTF-8.
            Name::Inline { len, bytes } => {
                core::str::from_utf8(&bytes[..*len as usize]).unwrap_or("")
            }
            Name::Owned(text) => text,
        }
    }
}

#[derive(Debug)]
enum Target {
    Run(RunId),
    RunPort(RunId, Name<PORT_INLINE>),
    Text(Name<PORT_INLINE>),
}

impl Target {
    fn of(op: &OpRef<'_>) -> Target {
        match op {
            OpRef::Feed { run, port, .. } | OpRef::Take { run, port } => {
                Target::RunPort(*run, Name::new(port))
            }
            OpRef::Step { run, .. } => Target::Run(*run),
            OpRef::Owned(op) => Target::of_owned(op),
        }
    }

    fn of_owned(op: &Op) -> Target {
        match op {
            Op::Feed { run, port, .. } | Op::Take { run, port } => {
                Target::RunPort(*run, Name::new(port))
            }
            Op::Step { run, .. } => Target::Run(*run),
            op => Target::Text(Name::new(&op.target())),
        }
    }

    /// As [`Op::target`] writes it.
    fn render(&self) -> String {
        match self {
            Target::Run(run) => run.to_string(),
            Target::RunPort(run, port) => format!("{run}:{}", port.as_str()),
            Target::Text(text) => text.as_str().to_owned(),
        }
    }
}

#[derive(Debug)]
struct Entry {
    seq: u64,
    id: u64,
    op: OpKind,
    error: Option<ErrorCode>,
    session: Name<SESSION_INLINE>,
    target: Target,
}

/// The last [`TRACE_RING`] served requests.
#[derive(Debug)]
pub struct TraceRing {
    entries: VecDeque<Entry>,
    next_seq: u64,
}

impl Default for TraceRing {
    fn default() -> TraceRing {
        TraceRing::new()
    }
}

impl TraceRing {
    /// Sized for [`TRACE_RING`] entries now, so recording never grows it.
    pub fn new() -> TraceRing {
        TraceRing {
            entries: VecDeque::with_capacity(TRACE_RING),
            next_seq: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Records one served request and gives it the next sequence number.
    pub fn record(&mut self, request: &Request, error: Option<ErrorCode>) {
        let target = Target::of_owned(&request.op);
        self.push(
            request.id,
            &request.session,
            request.op.kind(),
            target,
            error,
        );
    }

    /// Records a request read in place. A feed, step or take allocates nothing.
    pub fn record_ref(&mut self, request: &RequestRef<'_>, error: Option<ErrorCode>) {
        let target = Target::of(&request.op);
        self.push(
            request.id,
            request.session,
            request.op.kind(),
            target,
            error,
        );
    }

    fn push(
        &mut self,
        id: u64,
        session: &str,
        op: OpKind,
        target: Target,
        error: Option<ErrorCode>,
    ) {
        if self.entries.len() == TRACE_RING {
            self.entries.pop_front();
        }
        self.entries.push_back(Entry {
            seq: self.next_seq,
            id,
            op,
            error,
            session: Name::new(session),
            target,
        });
        self.next_seq += 1;
    }

    /// The newest `limit` entries, oldest first. A limit of zero is all of them.
    pub fn recent(&self, limit: u32) -> Vec<TraceEntry> {
        let keep = match limit {
            0 => self.entries.len(),
            limit => (limit as usize).min(self.entries.len()),
        };
        self.entries
            .iter()
            .skip(self.entries.len() - keep)
            .map(|entry| TraceEntry {
                seq: entry.seq,
                id: entry.id,
                session: entry.session.as_str().to_owned(),
                op: entry.op,
                target: entry.target.render(),
                error: entry.error,
            })
            .collect()
    }
}

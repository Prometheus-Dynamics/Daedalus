//! One connection. A reader reads requests and dispatches them. A writer sends what is queued:
//! answers first, then watch events. Nothing that runs a graph ever writes to a client, so a
//! client that stops reading holds up only itself, and is disconnected once its queue is full.

use std::boxed::Box;
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::vec::Vec;

use daedalus_wire::service::{
    ClientMsg, ErrorCode, Event, EventBody, Response, ServerMsg, ServiceError, SubId, WatchQueue,
};
use daedalus_wire::{DEFAULT_CAP, PacketError, Schema, read_packet, write_packet};

use super::ServiceHost;

/// Both halves of a connection, and how to close it from another thread.
pub struct Channel {
    pub reader: Box<dyn Read + Send>,
    pub writer: Box<dyn Write + Send>,
    /// Closes the connection, so a read or write blocked on it returns.
    pub close: Box<dyn Fn() + Send + Sync>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Open,
    /// The client is done sending. What is queued is still written.
    Draining,
    /// Disconnected. Nothing more is written.
    Closed,
}

struct Watch {
    queue: WatchQueue,
    /// The last event is queued: the watch goes once the queue is empty.
    ended: bool,
}

struct Outbound {
    phase: Phase,
    /// Encoded answers, in the order the requests were served.
    answers: VecDeque<Vec<u8>>,
    watches: Vec<Watch>,
    /// Where the next event is looked for first, so no watch starves another.
    turn: usize,
}

enum Item {
    Packet(Vec<u8>),
    Event(Event),
}

/// One client's side of the host.
pub(crate) struct Session {
    pub(crate) id: u64,
    out: Mutex<Outbound>,
    ready: Condvar,
    close: Box<dyn Fn() + Send + Sync>,
    /// Most answers that may wait to be written.
    limit: usize,
}

impl Session {
    fn new(id: u64, limit: usize, close: Box<dyn Fn() + Send + Sync>) -> Session {
        Session {
            id,
            out: Mutex::new(Outbound {
                phase: Phase::Open,
                answers: VecDeque::new(),
                watches: Vec::new(),
                turn: 0,
            }),
            ready: Condvar::new(),
            close,
            limit,
        }
    }

    fn out(&self) -> MutexGuard<'_, Outbound> {
        self.out.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Queues an answer. False when the queue is full: the client is not reading, and it is
    /// disconnected.
    fn answer(&self, packet: Vec<u8>) -> bool {
        let mut out = self.out();
        if out.phase == Phase::Closed {
            return false;
        }
        if out.answers.len() >= self.limit {
            drop(out);
            self.disconnect();
            return false;
        }
        out.answers.push_back(packet);
        self.ready.notify_all();
        true
    }

    pub(crate) fn add_watch(&self, sub: SubId) {
        self.out().watches.push(Watch {
            queue: WatchQueue::new(sub),
            ended: false,
        });
    }

    /// Queues an event of one watch. It never waits: a full queue drops its oldest.
    pub(crate) fn push_event(&self, sub: SubId, body: EventBody) {
        let mut out = self.out();
        if let Some(watch) = out.watches.iter_mut().find(|w| w.queue.sub() == sub) {
            watch.queue.push(body);
            self.ready.notify_all();
        }
    }

    /// Queues the last event of a watch.
    pub(crate) fn end_watch(&self, sub: SubId, body: EventBody) {
        let mut out = self.out();
        if let Some(watch) = out.watches.iter_mut().find(|w| w.queue.sub() == sub) {
            watch.queue.push(body);
            watch.ended = true;
            self.ready.notify_all();
        }
    }

    /// Drops the connection now.
    pub(crate) fn disconnect(&self) {
        self.out().phase = Phase::Closed;
        self.ready.notify_all();
        (self.close)();
    }

    /// Nothing more will be queued: write what is, then close.
    pub(crate) fn finish(&self) {
        let mut out = self.out();
        if out.phase == Phase::Open {
            out.phase = Phase::Draining;
        }
        self.ready.notify_all();
    }

    /// The next thing to write, waiting for one. `None` when the session is over.
    fn next(&self) -> Option<Item> {
        let mut out = self.out();
        loop {
            if out.phase == Phase::Closed {
                return None;
            }
            if let Some(packet) = out.answers.pop_front() {
                return Some(Item::Packet(packet));
            }
            let count = out.watches.len();
            for offset in 0..count {
                let at = (out.turn + offset) % count;
                if let Some(event) = out.watches[at].queue.pop() {
                    out.turn = at + 1;
                    return Some(Item::Event(event));
                }
            }
            out.watches.retain(|watch| !watch.ended);
            if out.phase == Phase::Draining {
                return None;
            }
            out = self.ready.wait(out).unwrap_or_else(PoisonError::into_inner);
        }
    }
}

fn error_packet(id: u64, error: ServiceError) -> Vec<u8> {
    let response = ServerMsg::Response(Response {
        id,
        result: Err(error),
    });
    // An error holds no value, so it encodes without any codec.
    response.encode(&Schema::new()).unwrap_or_default()
}

/// Reads the client's hello and answers it. Returns the largest packet the client reads, or
/// `None` when the connection was refused or lost.
fn hello(
    host: &ServiceHost,
    channel: &mut Channel,
    refuse: Option<ServiceError>,
    buf: &mut Vec<u8>,
) -> Option<u32> {
    read_packet(&mut channel.reader, DEFAULT_CAP, buf).ok()?;
    let refusal = match ClientMsg::decode(buf, &Schema::new()) {
        Ok(ClientMsg::Hello(hello)) => match (hello.check(), refuse) {
            (Ok(()), None) => {
                let body = ServerMsg::Hello(host.hello()).encode(&Schema::new()).ok()?;
                write_packet(&mut channel.writer, &body, DEFAULT_CAP).ok()?;
                return Some(hello.packet_cap.min(DEFAULT_CAP));
            }
            (Ok(()), Some(refusal)) => refusal,
            (Err(error), _) => ServiceError::from_wire(&error),
        },
        Ok(ClientMsg::Request(_)) => ServiceError::new(
            ErrorCode::InvalidRequest,
            "the first packet must be a hello",
        ),
        Err(error) => ServiceError::from_wire(&error),
    };
    // A refused client is told why, in place of the hello it expected.
    let _ = write_packet(&mut channel.writer, &error_packet(0, refusal), DEFAULT_CAP);
    None
}

/// Serves one connection until it ends. Runs the reader on this thread and the writer on its own.
/// `refuse` turns the connection away after its hello, with that error.
pub(crate) fn serve(host: &Arc<ServiceHost>, mut channel: Channel, refuse: Option<ServiceError>) {
    let mut buf = Vec::new();
    let Some(cap) = hello(host, &mut channel, refuse, &mut buf) else {
        (channel.close)();
        return;
    };
    let Channel {
        mut reader,
        mut writer,
        close,
    } = channel;
    let session = Arc::new(Session::new(
        host.next_session(),
        host.options().outbound_queue,
        close,
    ));
    host.session_opened(&session);
    let codecs = host.codec_schema();
    let writing = {
        let (session, codecs) = (Arc::clone(&session), codecs.clone());
        std::thread::Builder::new()
            .name("daedalus-session-write".into())
            .spawn(move || {
                while let Some(item) = session.next() {
                    let packet = match item {
                        Item::Packet(packet) => packet,
                        Item::Event(event) => match ServerMsg::Event(event).encode(&codecs) {
                            Ok(packet) => packet,
                            Err(_) => continue,
                        },
                    };
                    if write_packet(&mut writer, &packet, DEFAULT_CAP).is_err() {
                        break;
                    }
                }
                // Dropping the writer flushes it. Closing wakes a reader still blocked.
                drop(writer);
                session.disconnect();
            })
    };
    loop {
        match read_packet(&mut reader, DEFAULT_CAP, &mut buf) {
            Ok(()) => {}
            Err(error @ PacketError::TooLarge { .. }) => {
                // The body was not read, so the stream is out of step: say why, then drop it.
                session.answer(error_packet(0, ServiceError::from_packet(&error)));
                break;
            }
            Err(_) => break,
        }
        let packet = match ClientMsg::decode(&buf, &codecs) {
            Ok(ClientMsg::Request(request)) => {
                let id = request.id;
                let response = ServerMsg::Response(host.dispatch(&session, request));
                match response.encode(&codecs) {
                    Ok(packet) if packet.len() <= cap as usize => packet,
                    Ok(packet) => error_packet(
                        id,
                        ServiceError::new(
                            ErrorCode::FrameTooLarge,
                            std::format!(
                                "the answer is {} bytes, the client reads {cap}",
                                packet.len()
                            ),
                        ),
                    ),
                    Err(error) => error_packet(id, ServiceError::from_wire(&error)),
                }
            }
            Ok(ClientMsg::Hello(_)) => error_packet(
                0,
                ServiceError::new(ErrorCode::InvalidRequest, "a second hello"),
            ),
            // The id is read from what did decode, so the client's pipeline stays matched.
            Err(error) => error_packet(
                ClientMsg::request_id(&buf).unwrap_or(0),
                ServiceError::from_wire(&error),
            ),
        };
        if !session.answer(packet) {
            break;
        }
    }
    host.session_closed(&session);
    session.finish();
    if let Ok(writing) = writing {
        let _ = writing.join();
    } else {
        session.disconnect();
    }
}

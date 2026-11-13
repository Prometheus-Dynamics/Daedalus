//! The service protocol: round trips of every op, reply, event and error code, the refusals, and
//! the bounded watch queue and trace ring.

mod service_support;

use std::collections::HashMap;

use daedalus_wire::service::{
    Capabilities, ClientMsg, ErrorCode, Event, EventBody, Op, OpKind, Reply, Request, Response,
    SERVICE_LAYOUT_HASH, SERVICE_VERSION, SESSION_CAP, ServerMsg, ServiceError, ServiceHello,
    TRACE_RING, TelemetryEvent, TraceRing, TypedValue, WATCH_QUEUE, WatchQueue,
};
use daedalus_wire::{
    DEFAULT_CAP, PacketError, Schema, WireError, WireType, WireValue, read_packet, write_packet,
};
use service_support::{client_messages, events, ops, server_messages};

#[test]
fn every_op_round_trips() {
    let schema = Schema::new();
    let ops = ops();
    for kind in OpKind::ALL {
        assert!(
            ops.iter().any(|op| op.kind() == kind),
            "no round trip for {}",
            kind.name()
        );
    }
    for message in client_messages() {
        let body = message.encode(&schema).expect("encode");
        assert_eq!(ClientMsg::decode(&body, &schema).expect("decode"), message);
    }
}

#[test]
fn every_response_error_code_and_event_round_trips() {
    let schema = Schema::new();
    let messages = server_messages();
    for code in ErrorCode::ALL {
        assert_eq!(ErrorCode::from_code(code.code()), code);
        assert!(messages.iter().any(|message| matches!(
            message,
            ServerMsg::Response(Response { result: Err(error), .. }) if error.code == code
        )));
    }
    for message in messages {
        let body = message.encode(&schema).expect("encode");
        assert_eq!(ServerMsg::decode(&body, &schema).expect("decode"), message);
    }
}

#[test]
fn a_version_or_layout_mismatch_is_refused_naming_both() {
    assert_eq!(ServiceHello::client().check(), Ok(()));
    let mut old = ServiceHello::client();
    old.version = 2;
    assert_eq!(
        old.check(),
        Err(WireError::Protocol {
            expected: SERVICE_VERSION,
            found: 2
        })
    );
    let mut other = ServiceHello::server(Capabilities::all_ops());
    other.layout ^= 1;
    assert_eq!(
        other.check(),
        Err(WireError::Layout {
            expected: SERVICE_LAYOUT_HASH,
            found: SERVICE_LAYOUT_HASH ^ 1
        })
    );
    // A refused hello still decodes: the refusal is the check, so the error can name both sides.
    let body = ServerMsg::Hello(other.clone())
        .encode(&Schema::new())
        .expect("encode");
    assert_eq!(
        ServerMsg::decode(&body, &Schema::new()),
        Ok(ServerMsg::Hello(other))
    );
    // The service layout is not the worker layout, and the packet cap is the worker protocol's.
    assert_ne!(SERVICE_LAYOUT_HASH, daedalus_wire::LAYOUT_HASH);
    assert_eq!(ServiceHello::client().packet_cap, DEFAULT_CAP);
}

#[test]
fn two_pipelined_requests_with_interleaved_events_are_matched_by_id() {
    let schema = Schema::new();
    // The client writes both requests before it reads anything.
    let first = Request::new(41, Op::Step { run: 1, count: 100 });
    let second = Request::new(42, Op::Ping);
    let mut to_server = Vec::new();
    for request in [&first, &second] {
        let body = ClientMsg::Request(request.clone())
            .encode(&schema)
            .expect("encode");
        write_packet(&mut to_server, &body, DEFAULT_CAP).expect("write");
    }

    // The server reads both, answers the quick one first, and sends events in between.
    let mut input = &to_server[..];
    let mut buf = Vec::new();
    let mut seen = Vec::new();
    while read_packet(&mut input, DEFAULT_CAP, &mut buf).is_ok() {
        match ClientMsg::decode(&buf, &schema).expect("decode") {
            ClientMsg::Request(request) => seen.push(request),
            ClientMsg::Hello(_) => panic!("no hello was sent"),
        }
    }
    assert_eq!(seen, [first, second]);
    let outgoing = [
        ServerMsg::Event(Event {
            sub: 5,
            body: events()[0].clone(),
        }),
        ServerMsg::Response(Response {
            id: 42,
            result: Ok(Reply::Pong {
                version: SERVICE_VERSION,
            }),
        }),
        ServerMsg::Event(Event {
            sub: 5,
            body: events()[1].clone(),
        }),
        ServerMsg::Response(Response {
            id: 41,
            result: Err(ServiceError::new(ErrorCode::UnknownRun, "no run 1")),
        }),
    ];
    let mut to_client = Vec::new();
    for message in &outgoing {
        let body = message.encode(&schema).expect("encode");
        write_packet(&mut to_client, &body, DEFAULT_CAP).expect("write");
    }

    // The client sorts what arrives: responses by request id, events by subscription.
    let mut waiting: HashMap<u64, &str> = HashMap::from([(41, "step"), (42, "ping")]);
    let mut answered = Vec::new();
    let mut delivered = Vec::new();
    let mut input = &to_client[..];
    while read_packet(&mut input, DEFAULT_CAP, &mut buf).is_ok() {
        match ServerMsg::decode(&buf, &schema).expect("decode") {
            ServerMsg::Response(response) => {
                let what = waiting.remove(&response.id).expect("a request is waiting");
                answered.push((what, response.result.is_ok()));
            }
            ServerMsg::Event(event) => delivered.push(event),
            ServerMsg::Hello(_) => panic!("no hello was sent"),
        }
    }
    assert!(waiting.is_empty());
    assert_eq!(answered, [("ping", true), ("step", false)]);
    assert_eq!(delivered.len(), 2);
    assert!(delivered.iter().all(|event| event.sub == 5));
}

#[test]
fn a_watcher_that_does_not_read_gets_lagged_and_the_run_keeps_stepping() {
    let mut queue = WatchQueue::new(3);
    let mut ticks = 0u64;
    // The run steps 200 times. Each step pushes an event, and no push waits for the watcher.
    for tick in 1..=200u64 {
        queue.push(EventBody::Telemetry(TelemetryEvent {
            run: 1,
            tick,
            step_calls: 1,
            faults: 0,
            skipped: 0,
        }));
        ticks = tick;
        assert!(queue.len() <= WATCH_QUEUE);
    }
    assert_eq!(ticks, 200);

    // The watcher wakes up: first how much it missed, then the newest events in order.
    let first = queue.pop().expect("lagged");
    assert_eq!(first.sub, 3);
    assert_eq!(
        first.body,
        EventBody::Lagged {
            dropped: 200 - WATCH_QUEUE as u64
        }
    );
    let mut next_tick = 200 - WATCH_QUEUE as u64 + 1;
    while let Some(event) = queue.pop() {
        match event.body {
            EventBody::Telemetry(event) => assert_eq!(event.tick, next_tick),
            other => panic!("unexpected {other:?}"),
        }
        next_tick += 1;
    }
    assert_eq!(next_tick, 201);
    assert!(queue.is_empty());

    // A watcher that keeps up is never told it lagged.
    for tick in 0..500 {
        queue.push(EventBody::Lagged { dropped: tick });
        assert_eq!(
            queue.pop().expect("event").body,
            EventBody::Lagged { dropped: tick }
        );
    }
}

#[test]
fn the_trace_is_a_ring_of_1024_entries() {
    let mut ring = TraceRing::new();
    assert!(ring.is_empty());
    for id in 0..3000u64 {
        let error = (id % 2 == 1).then_some(ErrorCode::Busy);
        ring.record(&Request::new(id, Op::Step { run: id, count: 1 }), error);
    }
    assert_eq!(ring.len(), TRACE_RING);
    let all = ring.recent(0);
    assert_eq!(all.len(), TRACE_RING);
    assert_eq!(all[0].seq, 3000 - TRACE_RING as u64);
    assert_eq!(all.last().expect("entry").seq, 2999);
    let last = ring.recent(2);
    assert_eq!(last.len(), 2);
    assert_eq!((last[0].id, last[1].id), (2998, 2999));
    assert_eq!(last[1].error, Some(ErrorCode::Busy));
    assert_eq!(last[1].op, OpKind::Step);
    assert_eq!(last[1].target, "2999");
    assert_eq!(ring.recent(u32::MAX).len(), TRACE_RING);
}

#[test]
fn a_value_whose_type_has_no_codec_is_refused_as_unsupported() {
    let opaque = TypedValue::new(
        WireType::Opaque("acme.blob".to_owned()),
        WireValue::Opaque(vec![1, 2, 3]),
    );
    let feed = ClientMsg::Request(Request::new(
        1,
        Op::Feed {
            run: 1,
            port: "in".to_owned(),
            value: opaque.clone(),
        },
    ));
    // Without the codec the client cannot even send it.
    let error = feed.encode(&Schema::new()).expect_err("no codec");
    assert_eq!(error, WireError::NoCodec("acme.blob".to_owned()));
    assert_eq!(ServiceError::from_wire(&error).code, ErrorCode::Unsupported);

    // A client that has the codec sends it, and a server without it refuses it as unsupported,
    // still knowing which request to answer.
    let hello = ServiceHello::server(Capabilities {
        codecs: vec!["acme.blob".to_owned()],
        ..Capabilities::all_ops()
    });
    let body = feed.encode(&hello.codecs()).expect("encode");
    assert_eq!(ClientMsg::decode(&body, &hello.codecs()), Ok(feed));
    let error = ClientMsg::decode(&body, &Schema::new()).expect_err("no codec");
    assert_eq!(ServiceError::from_wire(&error).code, ErrorCode::Unsupported);
    assert_eq!(ClientMsg::request_id(&body), Some(1));

    // The same holds for a value the server returns.
    let reply = ServerMsg::Response(Response {
        id: 1,
        result: Ok(Reply::Value(Some(opaque))),
    });
    assert!(matches!(
        reply.encode(&Schema::new()),
        Err(WireError::NoCodec(_))
    ));
}

#[test]
fn errors_map_onto_the_closed_code_set() {
    let cases = [
        (WireError::InvalidUtf8, ErrorCode::InvalidRequest),
        (WireError::UnknownKind(9), ErrorCode::InvalidRequest),
        (WireError::TrailingBytes(1), ErrorCode::InvalidRequest),
        (
            WireError::ValueShape {
                expected: "u32",
                found: "string",
            },
            ErrorCode::ValueType,
        ),
        (
            WireError::Protocol {
                expected: 1,
                found: 2,
            },
            ErrorCode::Unsupported,
        ),
    ];
    for (error, code) in cases {
        assert_eq!(ServiceError::from_wire(&error).code, code);
    }
    let large = PacketError::TooLarge { len: 9, cap: 8 };
    assert_eq!(
        ServiceError::from_packet(&large).code,
        ErrorCode::FrameTooLarge
    );
    assert_eq!(
        ServiceError::from_packet(&PacketError::Truncated).code,
        ErrorCode::InvalidRequest
    );
    assert_eq!(
        ServiceError::new(ErrorCode::Busy, "64 runs").to_string(),
        "busy: 64 runs"
    );
    let names: Vec<&str> = ErrorCode::ALL.iter().map(|code| code.name()).collect();
    assert_eq!(
        names,
        [
            "invalid_request",
            "unknown_graph",
            "unknown_run",
            "unknown_port",
            "value_type",
            "plugin_error",
            "planning_failed",
            "busy",
            "cancelled",
            "stopped",
            "frame_too_large",
            "unsupported"
        ]
    );
}

/// A request body: the kind, the id 1, the session `s`, then `op`.
fn request_body(op: &[u8]) -> Vec<u8> {
    let mut body = vec![0x81];
    body.extend_from_slice(&1u64.to_le_bytes());
    body.extend_from_slice(&1u32.to_le_bytes());
    body.push(b's');
    body.extend_from_slice(op);
    body
}

#[test]
fn a_corpus_of_malformed_requests_gives_typed_errors() {
    let schema = Schema::new();
    let long_session = {
        let mut body = vec![0x81];
        body.extend_from_slice(&1u64.to_le_bytes());
        body.extend_from_slice(&((SESSION_CAP + 1) as u32).to_le_bytes());
        body.extend_from_slice(&[b'a'; SESSION_CAP + 1]);
        body.push(17);
        body
    };
    let corpus: Vec<(&str, Vec<u8>, WireError)> = vec![
        (
            "empty body",
            Vec::new(),
            WireError::Truncated {
                needed: 1,
                remaining: 0,
            },
        ),
        ("a worker kind", vec![0x01], WireError::UnknownKind(0x01)),
        ("a server kind", vec![0xc1], WireError::UnknownKind(0xc1)),
        (
            "an op that does not exist",
            request_body(&[0]),
            WireError::UnknownOp(0),
        ),
        (
            "an op past the last",
            request_body(&[18]),
            WireError::UnknownOp(18),
        ),
        (
            "a session over the bound",
            long_session,
            WireError::TooLong(SESSION_CAP + 1),
        ),
        (
            "a path that is not UTF-8",
            request_body(&[1, 2, 0, 0, 0, 0xff, 0xfe]),
            WireError::InvalidUtf8,
        ),
        (
            "a string longer than the body",
            request_body(&[1, 0xff, 0xff, 0xff, 0xff]),
            WireError::CountTooLarge {
                count: u32::MAX,
                remaining: 0,
            },
        ),
        (
            "a stop mode that does not exist",
            request_body(&[13, 1, 0, 0, 0, 0, 0, 0, 0, 2]),
            WireError::BadTag {
                what: "stop mode",
                tag: 2,
            },
        ),
        (
            "event kinds that do not exist",
            request_body(&[9, 0, 0x80]),
            WireError::BadTag {
                what: "event kinds",
                tag: 0x80,
            },
        ),
        (
            "a step cut short",
            request_body(&[7, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0]),
            WireError::Truncated {
                needed: 4,
                remaining: 2,
            },
        ),
        (
            "a feed whose value type nests too deep",
            {
                let mut op = vec![6];
                op.extend_from_slice(&1u64.to_le_bytes());
                op.extend_from_slice(&0u32.to_le_bytes());
                op.extend_from_slice(&[2u8; 64]);
                request_body(&op)
            },
            WireError::TooDeep,
        ),
    ];
    for (name, body, expected) in corpus {
        let error = ClientMsg::decode(&body, &schema).expect_err(name);
        assert_eq!(error, expected, "{name}");
        // Every one of them has a code in the closed set.
        assert!(ErrorCode::ALL.contains(&ServiceError::from_wire(&error).code));
    }
    // A packet over the cap is refused before its body is read.
    let mut packet = (DEFAULT_CAP + 1).to_le_bytes().to_vec();
    packet.push(0x81);
    let mut buf = Vec::new();
    let error = read_packet(&mut &packet[..], DEFAULT_CAP, &mut buf).expect_err("too large");
    assert_eq!(
        ServiceError::from_packet(&error).code,
        ErrorCode::FrameTooLarge
    );
    assert!(buf.capacity() < 1024);
}

/// A small generator, so the noise is the same on every run.
struct Noise(u64);

impl Noise {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

#[test]
fn every_truncation_and_noise_gives_an_error_and_never_a_panic() {
    let schema = Schema::new();
    for message in client_messages() {
        let body = message.encode(&schema).expect("encode");
        for cut in 0..body.len() {
            assert!(ClientMsg::decode(&body[..cut], &schema).is_err());
        }
    }
    for message in server_messages() {
        let body = message.encode(&schema).expect("encode");
        for cut in 0..body.len() {
            assert!(ServerMsg::decode(&body[..cut], &schema).is_err());
        }
    }
    let mut noise = Noise(0x9e37_79b9_7f4a_7c15);
    let kinds = [0x80u8, 0x81, 0xc0, 0xc1, 0xc2];
    for round in 0..20_000 {
        let len = (noise.next() % 96) as usize;
        let mut body: Vec<u8> = (0..len).map(|_| noise.next() as u8).collect();
        if let Some(first) = body.first_mut() {
            // Start most of them with a real kind, so the noise reaches the field decoders.
            if round % 8 != 0 {
                *first = kinds[round % kinds.len()];
            }
        }
        // Either result is fine. What matters is that both calls return.
        let _ = ClientMsg::decode(&body, &schema);
        let _ = ServerMsg::decode(&body, &schema);
        let _ = ClientMsg::request_id(&body);
    }
}

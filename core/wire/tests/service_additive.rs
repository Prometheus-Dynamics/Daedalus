//! What may be added to the service protocol without a side built before it being refused, or
//! taking a message to mean something else.

mod service_support;

use daedalus_wire::codec::Writer;
use daedalus_wire::service::{
    Capabilities, ClientMsg, ErrorCode, Event, EventBody, KNOWN, Op, OpKind, Request, Response,
    SERVICE_LAYOUT_HASH, ServerMsg, ServiceError, ServiceHello,
};
use daedalus_wire::{Schema, WireError};
use service_support::{client_messages, server_messages};

/// A message as a later version would send it: fields this version doesn't know at the end of
/// every record in it, and at the end of the message.
fn from_the_future(body: Vec<u8>) -> Vec<u8> {
    let mut body = body;
    body.extend_from_slice(&[0xEE; 5]);
    body
}

#[test]
fn fields_added_to_the_end_of_any_record_are_skipped() {
    let schema = Schema::new();
    for message in client_messages() {
        let plain = message.encode(&schema).expect("encode");
        let padded = message
            .encode_into(Writer::padded(7), &schema)
            .expect("encode");
        let body = from_the_future(padded);
        assert!(body.len() > plain.len());
        assert_eq!(
            ClientMsg::decode(&body, &schema).expect("decode"),
            message,
            "{message:?}"
        );
    }
    let mut sized = 0;
    for message in server_messages() {
        let plain = message.encode(&schema).expect("encode");
        let padded = message
            .encode_into(Writer::padded(7), &schema)
            .expect("encode");
        sized += usize::from(padded.len() > plain.len());
        assert_eq!(
            ServerMsg::decode(&from_the_future(padded), &schema).expect("decode"),
            message,
            "{message:?}"
        );
    }
    assert!(sized >= 8, "the samples have records nested in them");
}

/// The samples as this version first encoded them, kept as files. A side built then sends these
/// bytes, and every later version has to read them as the same messages. A field added since has
/// its default in what they decode to, which the two `as_first_sent` functions give.
fn corpus(name: &str, bodies: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/service");
    let path = dir.join(name);
    // One file: each body with its length before it.
    if std::env::var_os("DAEDALUS_WRITE_SERVICE_CORPUS").is_some() {
        let mut all = Vec::new();
        for body in bodies {
            all.extend_from_slice(&(body.len() as u32).to_le_bytes());
            all.extend_from_slice(body);
        }
        std::fs::write(&path, all).expect("write the corpus");
    }
    let all = std::fs::read(&path).expect("the corpus is in the tree");
    let mut out = Vec::new();
    let mut rest = &all[..];
    while !rest.is_empty() {
        let len = u32::from_le_bytes(rest[..4].try_into().expect("a length")) as usize;
        out.push(rest[4..4 + len].to_vec());
        rest = &rest[4 + len..];
    }
    out
}

/// A client's sample as a side built at the first version sent it: with nothing added since.
/// Nothing has been added to what a client sends.
fn client_as_first_sent(message: ClientMsg) -> ClientMsg {
    message
}

/// A server's sample as a side built at the first version sent it. Added since: a fault
/// event's `code` and `own_code`, which that side doesn't send, and which read as zero and no.
fn server_as_first_sent(mut message: ServerMsg) -> ServerMsg {
    if let ServerMsg::Event(Event {
        body: EventBody::Fault(fault),
        ..
    }) = &mut message
    {
        fault.code = 0;
        fault.own_code = false;
    }
    message
}

#[test]
fn what_the_first_version_sent_is_still_read_as_the_same_messages() {
    let schema = Schema::new();
    let clients = client_messages();
    let now: Vec<Vec<u8>> = clients
        .iter()
        .map(|message| message.encode(&schema).expect("encode"))
        .collect();
    let kept = corpus("client.bin", &now);
    assert!(kept.len() <= clients.len(), "a sample was taken away");
    for (body, message) in kept.iter().zip(clients) {
        assert_eq!(
            ClientMsg::decode(body, &schema).expect("decode"),
            client_as_first_sent(message)
        );
    }
    let servers = server_messages();
    let now: Vec<Vec<u8>> = servers
        .iter()
        .map(|message| message.encode(&schema).expect("encode"))
        .collect();
    let kept = corpus("server.bin", &now);
    assert!(kept.len() <= servers.len(), "a sample was taken away");
    for (body, message) in kept.iter().zip(servers) {
        assert_eq!(
            ServerMsg::decode(body, &schema).expect("decode"),
            server_as_first_sent(message)
        );
    }
}

/// The layout hash covers only what every version shares. A value that changes here is a
/// breaking change: every client and server built before it is refused at the hello.
#[test]
fn the_layout_hash_moves_only_on_a_breaking_change() {
    assert_eq!(SERVICE_LAYOUT_HASH, PINNED, "the service layout changed");
}

const PINNED: u64 = 18420071168406881561;

#[test]
fn a_peer_that_relies_on_something_unknown_is_refused_by_its_name() {
    let mut hello = ServiceHello::server(Capabilities::all_ops());
    assert_eq!(hello.check(), Ok(()));
    assert_eq!(hello.knows, KNOWN);
    assert!(!hello.knows("run.priority"));
    hello.knows.push("run.priority".to_owned());
    assert!(hello.knows("run.priority"), "a side asks before it sends");
    // Knowing more than this side is no reason to refuse. Relying on it is.
    assert_eq!(hello.check(), Ok(()));
    hello.needs.push("run.priority".to_owned());
    let refused = hello.check().expect_err("needs something unknown");
    assert_eq!(
        refused,
        WireError::Needs {
            name: "run.priority".to_owned()
        }
    );
    assert_eq!(
        refused.to_string(),
        "the other side needs `run.priority`, which this side doesn't have: this side has to \
         be updated"
    );
    // The hello itself round-trips, so the refusal can name what it found.
    let schema = Schema::new();
    let body = ServerMsg::Hello(hello.clone())
        .encode(&schema)
        .expect("encode");
    assert_eq!(
        ServerMsg::decode(&body, &schema),
        Ok(ServerMsg::Hello(hello))
    );
}

#[test]
fn a_newer_server_that_serves_more_ops_is_no_reason_to_refuse() {
    let schema = Schema::new();
    let hello = ServiceHello::server(Capabilities::all_ops());
    let mut body = ServerMsg::Hello(hello.clone())
        .encode(&schema)
        .expect("encode");
    // The ops are a count and a byte each, after the kind, version, layout and packet cap. Add
    // one this side doesn't have.
    let at = 1 + 4 + 8 + 4;
    let count = u32::from_le_bytes(body[at..at + 4].try_into().expect("a count"));
    body[at..at + 4].copy_from_slice(&(count + 1).to_le_bytes());
    body.insert(at + 4, 200);
    let ServerMsg::Hello(read) = ServerMsg::decode(&body, &schema).expect("decode") else {
        panic!("a hello");
    };
    assert_eq!(
        read, hello,
        "the op it doesn't have is left out, and the rest stands"
    );
    assert_eq!(read.check(), Ok(()));
}

#[test]
fn an_op_this_server_doesnt_have_is_answered_unsupported_and_named() {
    let schema = Schema::new();
    let mut body = ClientMsg::Request(Request::new(77, Op::Ping))
        .encode(&schema)
        .expect("encode");
    let op = body.len() - 1;
    assert_eq!(body[op], OpKind::Ping as u8);
    body[op] = 200;
    body.extend_from_slice(&[1, 2, 3, 4]); // whatever fields that op has
    let refused = ClientMsg::decode(&body, &schema).expect_err("an op from the future");
    assert_eq!(refused, WireError::UnknownOp(200));
    // The server can still answer it by id, and the answer says what it was.
    assert_eq!(ClientMsg::request_id(&body), Some(77));
    let error = ServiceError::from_wire(&refused);
    assert_eq!(error.code, ErrorCode::Unsupported);
    assert!(error.message.contains("op 200"), "{}", error.message);
}

#[test]
fn an_event_kind_or_error_code_from_a_newer_server_is_seen_as_unknown() {
    let schema = Schema::new();
    // An event of kind 77 with fields of its own.
    let mut body = vec![0xc2];
    body.extend_from_slice(&9u64.to_le_bytes());
    body.push(77);
    body.extend_from_slice(&[5, 6, 7]);
    assert_eq!(
        ServerMsg::decode(&body, &schema),
        Ok(ServerMsg::Event(Event {
            sub: 9,
            body: EventBody::Unknown { kind: 77 },
        }))
    );
    // An error with a code this side doesn't have keeps its message.
    let known = ServerMsg::Response(Response {
        id: 3,
        result: Err(ServiceError::new(ErrorCode::Busy, "the pool is full")),
    });
    let mut body = known.encode(&schema).expect("encode");
    let code = 1 + 8 + 1;
    body[code..code + 2].copy_from_slice(&900u16.to_le_bytes());
    let ServerMsg::Response(read) = ServerMsg::decode(&body, &schema).expect("decode") else {
        panic!("a response");
    };
    let error = read.result.expect_err("an error");
    assert_eq!(error.code, ErrorCode::Other);
    assert_eq!(error.message, "the pool is full");
    assert_eq!(ErrorCode::Other.name(), "other");
    assert_eq!(OpKind::named(200), OpKind::Other);
    assert!(!OpKind::ALL.contains(&OpKind::Other));
    assert!(!ErrorCode::ALL.contains(&ErrorCode::Other));
}

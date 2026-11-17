//! The client when the server refuses a request it could not read. The server answers under id 0,
//! which is not any request's id, and closes the connection.

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::Duration;

use daedalus_client::protocol::{
    Capabilities, ClientMsg, ErrorCode, Op, Reply, Response, ServerMsg, ServiceError, ServiceHello,
};
use daedalus_client::{Client, ClientError, Endpoint, Options};
use daedalus_wire::{DEFAULT_CAP, Schema, read_packet, write_packet};

/// A server that takes the hello, reads one request, refuses it under id 0 and closes. It does
/// this for every connection it is given.
fn refusing_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            thread::spawn(move || refuse(stream));
        }
    });
    addr
}

fn refuse(mut stream: TcpStream) {
    let mut buf = Vec::new();
    if read_packet(&mut stream, DEFAULT_CAP, &mut buf).is_err() {
        return;
    }
    let hello = ServerMsg::Hello(ServiceHello::server(Capabilities::all_ops()));
    let body = hello.encode(&Schema::new()).expect("encode");
    if write_packet(&mut stream, &body, DEFAULT_CAP).is_err() {
        return;
    }
    if read_packet(&mut stream, DEFAULT_CAP, &mut buf).is_err() {
        return;
    }
    assert!(matches!(
        ClientMsg::decode(&buf, &Schema::new()),
        Ok(ClientMsg::Request(_))
    ));
    let refusal = ServiceError::new(
        ErrorCode::FrameTooLarge,
        "the request is over the 16777216 bytes the host reads",
    );
    let answer = ServerMsg::Response(Response {
        id: 0,
        result: Err::<Reply, _>(refusal),
    });
    let body = answer.encode(&Schema::new()).expect("encode");
    let _ = write_packet(&mut stream, &body, DEFAULT_CAP);
}

fn endpoint(addr: SocketAddr) -> Endpoint {
    Endpoint::tcp(addr).expect("loopback")
}

#[test]
fn a_connection_refusal_under_id_zero_is_the_typed_error_for_the_request_in_flight() {
    let addr = refusing_server();
    let client = Client::connect(endpoint(addr), Options::default()).expect("connect");
    let ping = client.send(Op::Ping).expect("send");
    match ping.wait_timeout(Duration::from_secs(20)) {
        Ok(Err(ClientError::Service(error))) => assert_eq!(error.code, ErrorCode::FrameTooLarge),
        Ok(other) => panic!("expected frame_too_large, got {other:?}"),
        Err(_) => panic!("the caller was left waiting"),
    }
}

#[test]
fn a_reconnecting_client_does_not_resend_into_a_refusal_and_wait_forever() {
    let addr = refusing_server();
    let client = Client::connect(endpoint(addr), Options::reconnecting()).expect("connect");
    let ping = client.send(Op::Ping).expect("send");
    match ping.wait_timeout(Duration::from_secs(20)) {
        Ok(Err(ClientError::Service(error))) => assert_eq!(error.code, ErrorCode::FrameTooLarge),
        Ok(other) => panic!("expected frame_too_large, got {other:?}"),
        Err(_) => panic!("the caller was left waiting"),
    }
}

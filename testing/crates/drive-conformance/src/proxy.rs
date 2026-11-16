//! A proxy that makes a current server look like a newer one, for the additive-growth rules in
//! the service section of the project docs. It sits between the driver and the listener and changes
//! only what the server sends:
//! - five unknown bytes at the end of every message, which a reader skips;
//! - an op this side doesn't have in the hello, which the client leaves out;
//! - after every event, an event of a kind this side doesn't have, which a watch delivers as
//!   `unknown`;
//! - an error code this side doesn't have, which arrives as `other` with its message.
//!
//! The other mode, `SmallFrames`, lowers the packet cap the client announces in its hello. The
//! server then answers with `frame_too_large` any answer that is bigger, under the request's id,
//! which is a refusal the client can match. Nothing else in the exchange changes.
//!
//! The proxy takes one connection, which is all a scenario makes. It is TCP only.

use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;

use daedalus_wire::service::{ClientMsg, Event, EventBody, ServerMsg};
use daedalus_wire::{DEFAULT_CAP, Schema, read_packet, write_packet};

/// An op number the server does not have, and an event kind it does not have.
const NEWER_OP: u8 = 200;
const NEWER_KIND: u8 = 77;
/// An error code no version has.
const NEWER_CODE: u16 = 900;

/// The bytes a newer server adds at the end of each message.
const TRAILING: [u8; 5] = [0xEE; 5];

/// What the proxy changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// The server looks like a newer one.
    Newer,
    /// The client announces this packet cap in its hello.
    SmallFrames(u32),
}

/// Starts the proxy in front of `upstream`. Returns the address the driver connects to.
pub fn start(upstream: SocketAddr, mode: Mode) -> io::Result<SocketAddr> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let addr = listener.local_addr()?;
    thread::spawn(move || {
        let Ok((client, _)) = listener.accept() else {
            return;
        };
        let _ = relay(client, upstream, mode);
    });
    Ok(addr)
}

fn relay(client: TcpStream, upstream: SocketAddr, mode: Mode) -> io::Result<()> {
    let server = TcpStream::connect(upstream)?;
    let mut requests_in = client.try_clone()?;
    let mut requests_out = server.try_clone()?;
    thread::spawn(move || {
        let _ = forward_requests(&mut requests_in, &mut requests_out, mode);
    });
    let mut answers_in = server;
    let mut answers_out = client;
    let schema = Schema::new();
    let mut buf = Vec::new();
    loop {
        if read_packet(&mut answers_in, DEFAULT_CAP, &mut buf).is_err() {
            break;
        }
        let answers = match mode {
            Mode::Newer => newer(&buf, &schema)?,
            Mode::SmallFrames(_) => vec![buf.clone()],
        };
        for body in answers {
            if write_packet(&mut answers_out, &body, DEFAULT_CAP).is_err() {
                return Ok(());
            }
        }
    }
    Ok(())
}

/// The driver's requests go through unchanged, except the hello in `SmallFrames` mode.
fn forward_requests(input: &mut TcpStream, output: &mut TcpStream, mode: Mode) -> io::Result<()> {
    if let Mode::SmallFrames(cap) = mode {
        let mut buf = Vec::new();
        read_packet(input, DEFAULT_CAP, &mut buf)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
        let schema = Schema::new();
        let body = match ClientMsg::decode(&buf, &schema) {
            Ok(ClientMsg::Hello(mut hello)) => {
                hello.packet_cap = cap;
                ClientMsg::Hello(hello).encode(&schema).map_err(|error| {
                    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
                })?
            }
            _ => buf,
        };
        write_packet(output, &body, DEFAULT_CAP)
            .map_err(|error| io::Error::new(io::ErrorKind::BrokenPipe, error.to_string()))?;
    }
    io::copy(input, output).map(|_| ())
}

/// The packets a newer server would send in place of `body`. An error means the server sent what
/// this side can't read, which the proxy reports by ending the relay.
fn newer(body: &[u8], schema: &Schema) -> io::Result<Vec<Vec<u8>>> {
    let invalid = |what: &str| io::Error::new(io::ErrorKind::InvalidData, what.to_owned());
    let message = ServerMsg::decode(body, schema).map_err(|error| invalid(&error.to_string()))?;
    let trailing = |mut bytes: Vec<u8>| {
        bytes.extend_from_slice(&TRAILING);
        bytes
    };
    Ok(match message {
        ServerMsg::Hello(_) => {
            // The op is added to the list of ops, as the first of the list; the count goes up.
            let mut hello = body.to_vec();
            let at = 1 + 4 + 8 + 4;
            let count = u32::from_le_bytes(
                hello[at..at + 4]
                    .try_into()
                    .map_err(|_| invalid("a hello without an op count"))?,
            );
            hello[at..at + 4].copy_from_slice(&(count + 1).to_le_bytes());
            hello.insert(at + 4, NEWER_OP);
            let read =
                ServerMsg::decode(&hello, schema).map_err(|error| invalid(&error.to_string()))?;
            if read != message {
                return Err(invalid("the hello with a newer op reads differently"));
            }
            vec![hello]
        }
        ServerMsg::Event(event) => {
            let unknown = ServerMsg::Event(Event {
                sub: event.sub,
                body: EventBody::Unknown { kind: NEWER_KIND },
            })
            .encode(schema)
            .map_err(|error| invalid(&error.to_string()))?;
            vec![trailing(body.to_vec()), trailing(unknown)]
        }
        ServerMsg::Response(response) => {
            let mut bytes = body.to_vec();
            if response.result.is_err() {
                // The code follows the id and the result's tag: one byte, then two.
                let at = 1 + 8 + 1;
                bytes[at..at + 2].copy_from_slice(&NEWER_CODE.to_le_bytes());
            }
            vec![trailing(bytes)]
        }
    })
}

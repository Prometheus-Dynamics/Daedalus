//! The Rust reference driver: the canonical output of the contract, produced over `core/client`.
//! The binaries `drive-reference` and `drive-faulty` are thin wrappers around this module.

mod exec;
mod show;
mod syntax;

use std::io::{Read, Write};

use daedalus_client::protocol::{ClientMsg, ServerMsg};
use daedalus_client::{Client, Endpoint, Options};
use daedalus_wire::Schema;

use crate::corpus;

/// What a mutated driver gets wrong. The reference is `None`; the others exist so the suite can be
/// shown to fail a driver that is wrong.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mutation {
    None,
    /// A taken `u32` is one more than it is.
    WrongValue,
    /// The first event of each watch is dropped, and its line says the stream ended.
    DropEvent,
    /// Every error is answered `other`, whatever its code.
    SwallowCode,
    /// Recode mode: the last byte of each re-encoded body is wrong.
    WrongRecode,
}

impl Mutation {
    pub fn named(name: &str) -> Option<Mutation> {
        Some(match name {
            "value" => Mutation::WrongValue,
            "event" => Mutation::DropEvent,
            "code" => Mutation::SwallowCode,
            "recode" => Mutation::WrongRecode,
            _ => return None,
        })
    }
}

/// Connects to `endpoint` (`tcp:127.0.0.1:PORT` or `unix:PATH`), runs every command line read from
/// stdin, and writes one answer line per command to `out`.
pub fn drive(
    endpoint: &str,
    mutation: Mutation,
    input: &mut impl Read,
    out: &mut impl Write,
) -> Result<(), String> {
    let endpoint = parse_endpoint(endpoint)?;
    let root = Client::connect(endpoint, Options::default()).map_err(|error| error.to_string())?;
    let mut driver = exec::Driver::new(root, mutation);
    let mut text = String::new();
    input
        .read_to_string(&mut text)
        .map_err(|error| error.to_string())?;
    for line in text.lines() {
        let answer = driver.answer(line);
        writeln!(out, "{answer}").map_err(|error| error.to_string())?;
    }
    out.flush().map_err(|error| error.to_string())
}

fn parse_endpoint(text: &str) -> Result<Endpoint, String> {
    if let Some(addr) = text.strip_prefix("tcp:") {
        let addr = addr
            .parse()
            .map_err(|_| format!("`{addr}` is no address"))?;
        return Endpoint::tcp(addr).map_err(|error| error.to_string());
    }
    #[cfg(unix)]
    if let Some(path) = text.strip_prefix("unix:") {
        return Ok(Endpoint::unix(path));
    }
    Err(format!("`{text}` is no endpoint"))
}

/// Codec mode. Each body of the corpus file is decoded as a client or a server message, then
/// encoded again, and the bytes of the result are written as one lowercase hex line per body. A
/// body that does not decode is `error decode`.
pub fn recode(
    kind: &str,
    path: &str,
    mutation: Mutation,
    out: &mut impl Write,
) -> Result<(), String> {
    let bytes = std::fs::read(path).map_err(|error| error.to_string())?;
    let schema = Schema::new();
    for body in corpus::bodies(&bytes)? {
        let encoded = match kind {
            "client" => ClientMsg::decode(&body, &schema)
                .and_then(|message| message.encode(&schema))
                .ok(),
            "server" => ServerMsg::decode(&body, &schema)
                .and_then(|message| message.encode(&schema))
                .ok(),
            other => return Err(format!("no message kind `{other}`")),
        };
        let line = match encoded {
            None => "error decode".to_owned(),
            Some(mut bytes) => {
                if mutation == Mutation::WrongRecode
                    && let Some(last) = bytes.last_mut()
                {
                    *last ^= 1;
                }
                show::hex(&bytes)
            }
        };
        writeln!(out, "{line}").map_err(|error| error.to_string())?;
    }
    out.flush().map_err(|error| error.to_string())
}

/// The command line both binaries share: `ENDPOINT`, or `--recode client|server FILE`. The faulty
/// driver also takes `--mutate NAME` first.
pub fn cli(args: &[String], allow_mutation: bool) -> Result<(), String> {
    let usage = "usage: [--mutate NAME] ENDPOINT | --recode client|server FILE";
    let (mutation, rest) = match args {
        [flag, name, rest @ ..] if flag == "--mutate" => {
            if !allow_mutation {
                return Err("this driver has no mutations".to_owned());
            }
            let mutation = Mutation::named(name).ok_or_else(|| format!("no mutation `{name}`"))?;
            (mutation, rest)
        }
        _ => (Mutation::None, args),
    };
    match rest {
        [flag, kind, path] if flag == "--recode" => {
            recode(kind, path, mutation, &mut std::io::stdout().lock())
        }
        [endpoint] => drive(
            endpoint,
            mutation,
            &mut std::io::stdin().lock(),
            &mut std::io::stdout().lock(),
        ),
        _ => Err(usage.to_owned()),
    }
}

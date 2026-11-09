//! A JSON snapshot of the encoded state. Std only. The file is an array of objects, each with
//! `node`, `instance`, `tag` and `data` (the encoded value, as a string). Restore reads back
//! what snapshot wrote, and it refuses a node or tag it does not know.

use std::fmt::{self, Write};

use daedalus_types::NodeId;

use crate::state::{Codec, StateStore};

/// Why a snapshot could not be restored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotError {
    /// The text is not the JSON this module writes.
    Syntax { at: usize },
    /// The snapshot names a node the caller did not list.
    UnknownNode(String),
    /// The snapshot names a type tag the caller did not list.
    UnknownTag(String),
    /// The value under this tag would not decode.
    BadValue { tag: String },
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SnapshotError::Syntax { at } => write!(f, "snapshot is not valid at byte {at}"),
            SnapshotError::UnknownNode(node) => write!(f, "snapshot names unknown node {node}"),
            SnapshotError::UnknownTag(tag) => write!(f, "snapshot names unknown type {tag}"),
            SnapshotError::BadValue { tag } => {
                write!(f, "snapshot value for {tag} would not decode")
            }
        }
    }
}

impl std::error::Error for SnapshotError {}

/// Writes every encodable entry of `store` as JSON.
pub fn snapshot_json(store: &StateStore) -> String {
    let mut out = String::from("[");
    for (index, (node, instance, codec, data)) in store.encoded().enumerate() {
        if index > 0 {
            out.push(',');
        }
        let _ = write!(
            out,
            "{{\"node\":{},\"instance\":{instance},\"tag\":{},\"data\":{}}}",
            quote(node.as_str()),
            quote(codec.tag),
            quote(&data),
        );
    }
    out.push(']');
    out
}

/// Reads a snapshot back into `store`. `nodes` and `codecs` list what may be restored. It
/// returns how many entries were restored.
pub fn restore_json(
    store: &mut StateStore,
    text: &str,
    nodes: &[NodeId],
    codecs: &[Codec],
) -> Result<usize, SnapshotError> {
    let records = Parser::new(text).records()?;
    let mut restored = 0;
    for record in records {
        let node = nodes
            .iter()
            .copied()
            .find(|node| node.as_str() == record.node)
            .ok_or_else(|| SnapshotError::UnknownNode(record.node.clone()))?;
        let codec = codecs
            .iter()
            .copied()
            .find(|codec| codec.tag == record.tag)
            .ok_or_else(|| SnapshotError::UnknownTag(record.tag.clone()))?;
        let value = (codec.decode)(&record.data).ok_or_else(|| SnapshotError::BadValue {
            tag: record.tag.clone(),
        })?;
        store.insert_decoded(node, record.instance, codec, value);
        restored += 1;
    }
    Ok(restored)
}

fn quote(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if (ch as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", ch as u32);
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

struct Record {
    node: String,
    instance: u32,
    tag: String,
    data: String,
}

/// A reader for the one shape snapshot writes: an array of flat objects.
struct Parser<'a> {
    bytes: &'a [u8],
    text: &'a str,
    at: usize,
}

impl<'a> Parser<'a> {
    fn new(text: &'a str) -> Parser<'a> {
        Parser {
            bytes: text.as_bytes(),
            text,
            at: 0,
        }
    }

    fn fail<T>(&self) -> Result<T, SnapshotError> {
        Err(SnapshotError::Syntax { at: self.at })
    }

    fn skip_space(&mut self) {
        while matches!(self.bytes.get(self.at), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.at += 1;
        }
    }

    fn eat(&mut self, byte: u8) -> Result<(), SnapshotError> {
        self.skip_space();
        if self.bytes.get(self.at) == Some(&byte) {
            self.at += 1;
            Ok(())
        } else {
            self.fail()
        }
    }

    fn peek_is(&mut self, byte: u8) -> bool {
        self.skip_space();
        self.bytes.get(self.at) == Some(&byte)
    }

    fn records(mut self) -> Result<Vec<Record>, SnapshotError> {
        let mut records = Vec::new();
        self.eat(b'[')?;
        if self.peek_is(b']') {
            self.at += 1;
            return Ok(records);
        }
        loop {
            records.push(self.record()?);
            if self.peek_is(b',') {
                self.at += 1;
            } else {
                self.eat(b']')?;
                return Ok(records);
            }
        }
    }

    fn record(&mut self) -> Result<Record, SnapshotError> {
        self.eat(b'{')?;
        let mut node = None;
        let mut instance = None;
        let mut tag = None;
        let mut data = None;
        loop {
            let key = self.string()?;
            self.eat(b':')?;
            match key.as_str() {
                "node" => node = Some(self.string()?),
                "tag" => tag = Some(self.string()?),
                "data" => data = Some(self.string()?),
                "instance" => instance = Some(self.number()?),
                _ => return self.fail(),
            }
            if self.peek_is(b',') {
                self.at += 1;
            } else {
                self.eat(b'}')?;
                break;
            }
        }
        match (node, instance, tag, data) {
            (Some(node), Some(instance), Some(tag), Some(data)) => Ok(Record {
                node,
                instance,
                tag,
                data,
            }),
            _ => self.fail(),
        }
    }

    fn number(&mut self) -> Result<u32, SnapshotError> {
        self.skip_space();
        let start = self.at;
        while matches!(self.bytes.get(self.at), Some(b'0'..=b'9')) {
            self.at += 1;
        }
        self.text[start..self.at].parse().or_else(|_| self.fail())
    }

    fn string(&mut self) -> Result<String, SnapshotError> {
        self.eat(b'"')?;
        let mut out = String::new();
        loop {
            let Some(&byte) = self.bytes.get(self.at) else {
                return self.fail();
            };
            self.at += 1;
            match byte {
                b'"' => return Ok(out),
                b'\\' => {
                    let Some(&escape) = self.bytes.get(self.at) else {
                        return self.fail();
                    };
                    self.at += 1;
                    match escape {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let hex = self.text.get(self.at..self.at + 4);
                            let code = hex.and_then(|hex| u32::from_str_radix(hex, 16).ok());
                            let Some(ch) = code.and_then(char::from_u32) else {
                                return self.fail();
                            };
                            self.at += 4;
                            out.push(ch);
                        }
                        _ => return self.fail(),
                    }
                }
                _ => {
                    // A multi-byte character: copy its whole UTF-8 sequence.
                    let start = self.at - 1;
                    let mut end = self.at;
                    while end < self.bytes.len() && (self.bytes[end] & 0xC0) == 0x80 {
                        end += 1;
                    }
                    let Some(piece) = self.text.get(start..end) else {
                        return self.fail();
                    };
                    out.push_str(piece);
                    self.at = end;
                }
            }
        }
    }
}

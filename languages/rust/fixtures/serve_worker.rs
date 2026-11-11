//! The worker the serve tests run: nodes that panic, emit events, wait for a cancel, fail, or add
//! one. Each is written by hand, because a node that takes the context or fails cannot be declared
//! with `#[node]` yet.

#![forbid(unsafe_code)]

use std::time::Duration;

use daedalus_lang_rust::codec::input;
use daedalus_lang_rust::{Context, NodeError, Pack, RustBody, serve};
use daedalus_registry::{BodyRecord, Fire, NodeDecl, NodeRecord, PortDecl, SourceAt, TypeRef};
use daedalus_types::Scalar;
use daedalus_wire::{FaultCode, LogLevel, WireValue};

type Inputs<'a> = &'a [(&'static str, WireValue)];

fn panics(_: Inputs, _: &mut Context) -> Result<Vec<WireValue>, NodeError> {
    panic!("the node panicked on purpose");
}

/// Emits a progress event, a log line, and another progress event, then returns its input.
fn events(inputs: Inputs, cx: &mut Context) -> Result<Vec<WireValue>, NodeError> {
    let value: i64 = input(inputs, "x")?;
    cx.progress(0.5, "half");
    cx.log(LogLevel::Info, "after the progress");
    cx.progress(1.0, "done");
    Ok(value.pack())
}

/// Loops until the host asks it to stop, then returns a value.
fn waits(_: Inputs, cx: &mut Context) -> Result<Vec<WireValue>, NodeError> {
    while !cx.cancelled() {
        std::thread::sleep(Duration::from_millis(2));
    }
    Ok(0i64.pack())
}

/// Fails with a node error and a reason.
fn fails(_: Inputs, _: &mut Context) -> Result<Vec<WireValue>, NodeError> {
    Err(NodeError::new(
        FaultCode::NodeError,
        "the node failed on purpose",
    ))
}

/// Adds one to its input.
fn adds(inputs: Inputs, _: &mut Context) -> Result<Vec<WireValue>, NodeError> {
    let value: i64 = input(inputs, "x")?;
    Ok((value + 1).pack())
}

static IN: [PortDecl; 1] = [PortDecl::new("x", TypeRef::Key(Scalar::Int.key()))];
static OUT: [PortDecl; 1] = [PortDecl::new("y", TypeRef::Key(Scalar::Int.key()))];

const fn decl(id: &'static str) -> NodeDecl {
    NodeDecl {
        id,
        at: SourceAt(concat!(file!(), ":", line!())),
        summary: "",
        description: "",
        inputs: &IN,
        outputs: &OUT,
        config: &[],
        fanin: &[],
        fire: Fire::All,
        reusable: false,
        shareable: false,
        conversion: None,
        group: None,
    }
}

const fn record(decl: &'static NodeDecl, body: &'static RustBody) -> NodeRecord {
    NodeRecord {
        decl,
        body: BodyRecord {
            backend: "rust",
            at: decl.at,
            needs: &[],
            state: None,
            ports: &[],
            memory: &[],
            code: body,
            text: "",
        },
    }
}

static PANICS_BODY: RustBody = RustBody {
    run: panics,
    state: None,
};
static EVENTS_BODY: RustBody = RustBody {
    run: events,
    state: None,
};
static WAITS_BODY: RustBody = RustBody {
    run: waits,
    state: None,
};
static FAILS_BODY: RustBody = RustBody {
    run: fails,
    state: None,
};
static ADDS_BODY: RustBody = RustBody {
    run: adds,
    state: None,
};

static PANICS_DECL: NodeDecl = decl("serve.panics");
static EVENTS_DECL: NodeDecl = decl("serve.events");
static WAITS_DECL: NodeDecl = decl("serve.waits");
static FAILS_DECL: NodeDecl = decl("serve.fails");
static ADDS_DECL: NodeDecl = decl("serve.adds");

static PANICS: NodeRecord = record(&PANICS_DECL, &PANICS_BODY);
static EVENTS: NodeRecord = record(&EVENTS_DECL, &EVENTS_BODY);
static WAITS: NodeRecord = record(&WAITS_DECL, &WAITS_BODY);
static FAILS: NodeRecord = record(&FAILS_DECL, &FAILS_BODY);
static ADDS: NodeRecord = record(&ADDS_DECL, &ADDS_BODY);

fn main() {
    let nodes: &[&NodeRecord] = &[&PANICS, &EVENTS, &WAITS, &FAILS, &ADDS];
    if let Err(error) = serve(nodes) {
        let _ = std::io::Write::write_fmt(&mut std::io::stderr(), format_args!("{error}\n"));
        std::process::exit(1);
    }
}

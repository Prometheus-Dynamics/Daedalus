//! The drive conformance suite. Every language's client for the daemon's service protocol is held
//! to one behaviour: a driver program for the client is given a scenario, performs it against a
//! live host, and prints one canonical line per command. The suite compares those lines with
//! golden transcripts made by the Rust reference driver (`drive-reference`, over `core/client`).
//!
//! The host is `daedalus_host::listen::ServiceHost`, in this process, with the dylib fixture
//! plugin (`testing/fixtures/dylib`) loaded by scenarios that ask for it. It listens on loopback
//! TCP, and on a unix socket where the platform has them. No daemon binary is needed.
//!
//! # The driver contract
//!
//! A driver is any program that can be run from a command line. It is written in the language
//! of the client it tests. The rules below are complete: a driver author needs this document and
//! the scenario files, nothing else.
//!
//! ## Invocation
//!
//! The suite runs the driver as `DRIVER ENDPOINT`, with the endpoint as the last argument.
//!
//! - `ENDPOINT` is `tcp:127.0.0.1:PORT` (loopback TCP) or `unix:PATH` (a unix socket). A driver
//!   connects to it as a client, with reconnect off.
//! - The driver reads the whole of standard input, then writes its answers to standard output.
//! - It exits with status 0 once every line is answered. An error the host gives is an answer,
//!   not a failure of the driver. Anything else written on standard error is ignored.
//!
//! The codec mode is `DRIVER --recode KIND FILE`, where `KIND` is `client` or `server`. It makes
//! no connection (see "Codec mode").
//!
//! ## Input
//!
//! Standard input is UTF-8 text: one command per line, each line ended by `\n`. The suite has
//! already removed blank lines, comment lines (starting with `#`) and harness directives (starting
//! with `@`), and it has put the real paths in place of `{plugin}` and `{graphs}`. A driver may
//! assume every line is a command. Paths never contain spaces.
//!
//! Tokens are separated by one space. A line never has a leading or trailing space.
//!
//! ## Output
//!
//! For each input line, in order, the driver writes exactly one output line: UTF-8, `\n` ended,
//! and nothing else on standard output. A command that fails writes an error line instead. So the
//! output has as many lines as the input, and line N answers command N. The suite reports a
//! difference by the scenario line of the command.
//!
//! A driver prints an event only when `next` asks for one (see "Events"). An event that arrives
//! while a command runs is kept for the next `next`, so the output does not depend on timing.
//!
//! ## Names, numbers and strings
//!
//! - A run is the number the host gave it. A watch is a number the scenario chose, and the driver
//!   keeps the watch under it. A port, a graph name and a blob name are text with no spaces.
//! - An integer is decimal, with a leading `-` when it is negative.
//! - `true` and `false` are the two boolean words.
//! - A quoted string is `"`, then the text, then `"`. Inside the text: `"` is `\"`, `\` is `\\`,
//!   newline is `\n`, carriage return is `\r`, tab is `\t`. A code point from U+0020 to U+007E is
//!   itself. Any other code point is `\u{H}`, where `H` is the code point in lowercase hex without
//!   leading zeros.
//! - A list is `[`, the quoted items separated by `,` with no space, then `]`. An empty list is
//!   `[]`.
//! - A bare word is a name from a fixed set, written as it is in the answer below.
//! - A record field is `name=value`, fields separated by one space, in the order given below.
//!
//! ## Values
//!
//! A value is `TYPE:VALUE`, the syntax `daedalus-ctl` reads (`u32:41`). The suite uses these types:
//!
//! | TYPE | VALUE in input and output | Example |
//! |---|---|---|
//! | `unit` | nothing | `unit:` |
//! | `bool` | `true` or `false` | `bool:true` |
//! | `i8`, `i16`, `i32`, `i64`, `isize` | decimal, with `-` | `i64:-3` |
//! | `u8`, `u16`, `u32`, `u64`, `usize` | decimal | `u32:41` |
//! | `string` | the rest of the line, raw (not quoted) | `string:hi there` |
//! | `bytes` | lowercase hex, two digits a byte; empty is no digits | `bytes:00ff` |
//!
//! In input the value is the last token of `feed`, and in output it is the last of `value`, so
//! the rest of the line is its text. Floats and compound values (lists, structs and so on) are not
//! part of the suite: a driver never reads them, and writes `other` for one it is given.
//!
//! ## Commands and their answers
//!
//! Each row gives a command line and the one answer line it produces. `N` is a decimal, `S` is a
//! quoted string, `L` is a list, `B` is `true` or `false`, `A` is a name word.
//!
//! | Command | Answer |
//! |---|---|
//! | `ping` | `pong version=N` |
//! | `session` | `session name=S`, for the default session |
//! | `session NAME` | `session name=S`, and later commands are in session `NAME` |
//! | `load_plugin PATH` | `plugin name=S generation=N nodes=L backends=L` |
//! | `put_graph NAME PATH` | `graph name=S diagnostics=L`. `PATH` is a file the driver reads as text, and the text is the graph document. |
//! | `patch_graph NAME PATH` | `graph name=S diagnostics=L`. `PATH` is read as a patch document. |
//! | `plan NAME FORM [inspect] [priority=A,B]` | `plan graph=S hash=N form=A nodes=N edges=N regions=N steps=N cached=B diagnostics=L explain=S-or-none` |
//! | `blob NAME HEX` | `blob name=S bytes=N`. The driver keeps the bytes of `HEX` under `NAME`. |
//! | `make_plan GRAPH BLOB [OPTIONS]` | `made hash=N bytes=N`. The made plan's bytes are kept under `BLOB`. |
//! | `put_plan NAME BLOB` | `ack`. The bytes kept under `BLOB` are stored under `NAME`. |
//! | `start GRAPH [OPTIONS]` | `started run=N` |
//! | `feed RUN PORT TYPE:VALUE` | `fed woke=N dropped=B` |
//! | `step RUN COUNT` | `stepped ticks=N tick=N step_calls=N faults=N skipped=N idle=B` |
//! | `take RUN PORT` | `value none`, or `value TYPE:VALUE` |
//! | `watch W TARGET [KINDS]` | `watching W`. `TARGET` is `all` or a run number. `KINDS` is `fault`, `telemetry` or `all` (the default). |
//! | `next W` | one event line (see "Events"), or `stream W ended` |
//! | `unwatch W` | `unwatched W` |
//! | `reload RUN GRAPH` | `reloaded carried=NL fresh=NL added=NL removed=NL removed_ports=NL dropped_values=N`, where `NL` is a list of names, `/`, and the total count |
//! | `cancel RUN` | `ack` |
//! | `stop RUN drain` or `stop RUN immediate` | `stopped tick=N dropped=N` |
//! | `inspect run RUN` | `inspect run run=N session=S graph=S state=A tick=N faults=N ports=L nodes=L` |
//! | `inspect telemetry RUN` | `inspect telemetry run=N level=A tick=N step_calls=N faults=N skipped=N rows=L` |
//! | `inspect graph NAME` | `inspect graph name=S json=S` |
//! | `inspect cache` | `inspect cache entries=N hits=N misses=N` |
//! | `inspect host` | `inspect host sessions=L graphs=L runs=L plugins=L` |
//! | `cache clear` | `ack` |
//! | `trace LIMIT` | `trace entries=L` |
//!
//! Option words for `start` and `make_plan` (`OPTIONS`): `fifo=N`, `faults=N`,
//! `telemetry=off|basic|detailed|trace`, `unmerged`, `priority=A,B`. With none of them the start
//! config is the protocol's default: `fifo` and `faults` are 0 (the host's own), `telemetry` is
//! `basic`, `unmerged` is false, and there is no priority. For `plan`, the words after `FORM` are
//! `inspect` (also return the explanation) and `priority=A,B`.
//!
//! Words in answers:
//! - `FORM`: `steps`, `blob` or `source`.
//! - `state`: `running`, `cancelled` or `stopped`.
//! - `level`: `off`, `basic`, `detailed` or `trace`.
//! - `reason` (in an ended event): `stopped`, `cancelled`, `unwatched` or `shutdown`.
//!
//! Field words in answers:
//! - `diagnostics` is a list of `severity|code`, where severity is `error` or `warning`.
//! - `explain` is a quoted string, or the word `none` when the answer has no explanation.
//! - `ports` is a list of `name|direction|type`, where direction is `input` or `output`.
//! - `runs` in `inspect host` is a list of `run|session|graph|state`.
//! - `rows` in `inspect telemetry` is a list of `step|node+node|calls|failures|backing_off|disabled|auto_off`
//!   (the numbers are decimal and the flags are `true` or `false`). Timings and byte counts are
//!   measured, so they are not in the line.
//! - `trace` entries are a list of `seq|op|target|error`, where `op` is the op's name (`ping`,
//!   `put_graph` and so on), `target` is the op's target as the host names it, and `error` is an
//!   error code name or `none`.
//!
//! ## Events
//!
//! `watch W` makes a watch the driver keeps under `W`. The host sends events for it, and the
//! driver does not print them when they arrive. Only `next W` prints one. It takes the next event
//! of the watch, in the order the host sent them, and waits for one if none has come yet. When
//! the watch has no more events (it ended, or was unwatched and its queue is read), it answers
//! `stream W ended`.
//!
//! An event line is one of:
//!
//! - `event W fault run=N tick=N step=N node=S-or-none covers=L consecutive=N reason=S reason_cut=B detail=S detail_cut=B code=N own_code=B`
//! - `event W telemetry run=N tick=N step_calls=N faults=N skipped=N`
//! - `event W lagged dropped=N`
//! - `event W ended run=N reason=A`
//! - `event W unknown kind=N`
//!
//! `unknown` is an event of a kind this client does not have. A driver writes it as such and does
//! not guess what it is. `unwatch W` ends the watch: events already queued can still be read with
//! `next`, and an event that arrives after the unwatch may be dropped by the driver, so a scenario
//! reads every event it expects before it unwatches.
//!
//! The order rule: events of one watch come out in the order the host sent them. Events of
//! different watches are not ordered against each other, and a scenario does not depend on it.
//! `next` waits until the next event of its watch has arrived, so a scenario reads only the
//! events it knows the host will send.
//!
//! ## Errors
//!
//! A command that the host refuses writes `error CODE`, where `CODE` is the code's name from
//! the protocol: `invalid_request`, `unknown_graph`, `unknown_run`, `unknown_port`, `value_type`,
//! `plugin_error`, `planning_failed`, `busy`, `cancelled`, `stopped`, `frame_too_large`,
//! `unsupported`, or `other`. A code the driver does not know is `other`, and the message text
//! is never printed.
//!
//! A failure that is not a host refusal writes `error local`. A line the driver cannot read, or
//! a name it was never given (a watch or blob not made), writes `error usage`.
//!
//! ## Codec mode
//!
//! A pure-language client (one with no native code) is also checked on the bytes it reads and
//! writes. `DRIVER --recode KIND FILE` reads a corpus file: `core/wire/tests/corpus/service/client.bin`
//! for `client`, `server.bin` for `server`. The file is a sequence of bodies, each a `u32` length
//! in little-endian bytes followed by that many body bytes. For each body in order, the driver
//! decodes the body as a message of that kind, encodes the decoded message again, and writes the
//! result on one output line as lowercase hex, two digits a byte. A body that does not decode
//! writes `error decode`.
//!
//! The lines must equal the golden lines `golden/codec-KIND.out`, which the reference driver made,
//! one per body. The corpus holds the bytes the first version sent. A field added since is encoded
//! at its default on re-encoding, so a body that has one gets longer. For the client corpus every
//! re-encoding is the body itself. For the server corpus two are longer: a fault event (its `code`
//! and `own_code`) and a telemetry inspection (its row timings). The golden holds the longer form.
//!
//! ## Not covered
//!
//! Two cases have no scenario, and one refusal path is not run:
//!
//! - `lagged` (a watch that fell behind): a watch keeps 64 events and is drained by the session's
//!   writer. Overflow needs the socket to stop taking data, and then the number of events that
//!   fit depends on the socket buffers of the machine. No fixed scenario can pin it. A test build
//!   option would change the listener, so none was added.
//! - `stopped`: the host answers it only in the window inside one `stop`, after the run is
//!   marked stopped and before it leaves the table. A second stop finds no run (`unknown_run`).
//!   No sequence of one driver's commands reaches it.
//! - An inbound frame bigger than the host's 16 MiB read cap: the client refuses to send it, and
//!   the host's answer to it carries id 0, which the Rust client cannot match to a request (read
//!   from the listener's code; not run here).
//!
//! ## Golden transcripts
//!
//! The file `golden/NAME.out` holds the answers of the reference driver to `scenarios/NAME.scn`,
//! one line per command. A driver passes a scenario when its answers equal those lines. The line
//! numbers in a report are the line numbers in the `.scn` file. The codec goldens are
//! `golden/codec-client.out` and `golden/codec-server.out` (see "Codec mode").
//!
//! The reference driver writes all of them with `BLESS=1`, which is how a change to the protocol
//! or the scenarios is accepted: the diff of the golden files is the review.
//!
//! Before a driver's answers are compared, the suite replaces the fixture library's path with
//! `{plugin}` and the graph directory's path with `{graphs}`, wherever they appear. A driver
//! prints them as it was given them, and the golden holds the placeholders.
//!
//! ## Scenario syntax
//!
//! A scenario is UTF-8 text, one command per line, plus these lines the suite reads itself:
//!
//! - `#` starts a comment line.
//! - `@host plugins` (the default) or `@host none`: whether the host is given a plugin loader. With
//!   `none`, `load_plugin` is answered `error unsupported`.
//! - `@proxy newer`: the driver is put behind a proxy that makes the host look like a newer one.
//!   The proxy adds bytes the reader must skip, an op the client does not have, an unknown event
//!   after every event, and an unknown error code. Such a scenario runs over TCP only.
//! - `@proxy frames N`: the proxy lowers the packet cap the driver announces in its hello to N
//!   bytes. An answer bigger than that comes back as `error frame_too_large`, under the id of the
//!   request. Such a scenario runs over TCP only.
//!
//! Each scenario runs on a fresh host with no plugin loaded. A scenario that needs the fixture's
//! nodes loads them with `load_plugin {plugin}`.

#![forbid(unsafe_code)]

mod corpus;
mod proxy;
pub mod reference;
mod run;
mod scenario;
mod serve;

use std::fmt;

pub use corpus::{bodies as corpus_bodies, path as corpus_path};
pub use run::{
    bless, build_fixture, codec_golden as codec_golden_path, run_codec_check, run_driver,
    transcript,
};
pub use scenario::{Scenario, golden_dir, graphs_dir, load_all as load_scenarios};
pub use serve::Transport;

/// A difference between a driver and the golden transcript, or a driver that could not be run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    /// The scenario's name (`faults`), or for the codec check the corpus file (`client.bin`).
    pub scenario: String,
    /// The transport (`tcp`, `unix`, `tcp+newer`), or `codec`.
    pub transport: String,
    /// The line of the scenario file, or for the codec check the body number. None when the
    /// difference is not on one line.
    pub line: Option<usize>,
    /// What the golden transcript says. Empty when the failure is a run that did not end.
    pub expected: String,
    /// What the driver printed, or why it did not finish.
    pub got: String,
}

impl Failure {
    /// A failure with no line and no expectation: a scenario that could not be loaded or a driver
    /// that could not run.
    pub(crate) fn plain(scenario: &str, got: String) -> Failure {
        Failure {
            scenario: scenario.to_owned(),
            transport: String::new(),
            line: None,
            expected: String::new(),
            got,
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let place = match (self.line, self.transport.is_empty()) {
            (Some(line), _) => format!("{} line {line} [{}]", self.scenario, self.transport),
            (None, true) => self.scenario.clone(),
            (None, false) => format!("{} [{}]", self.scenario, self.transport),
        };
        if self.expected.is_empty() {
            write!(f, "{place}: {}", self.got)
        } else {
            write!(
                f,
                "{place}: expected `{}`, got `{}`",
                self.expected, self.got
            )
        }
    }
}

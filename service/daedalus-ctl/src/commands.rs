//! The commands: each is parsed from its words, sent through the client, and gives back one
//! JSON object.

use std::io::Write;

use daedalus_client::protocol::{
    EventBody, EventKinds, InspectTarget, PlanForm, PlanOptions, RunId, StartConfig, StopMode,
    TelemetryLevel, TypedValue, WatchTarget,
};
use daedalus_client::{Client, ClientError, WireType, WireValue};
use daedalus_types::Scalar;
use serde_json::{Value, json};

use crate::output;

pub(crate) const COMMANDS: &str = "\
commands:
  ping
  load-plugin PATH
  put-graph NAME FILE
  patch-graph GRAPH FILE
  plan GRAPH [--form steps|blob|source] [--inspect] [--priority ID,ID]
  start GRAPH [--fifo N] [--fault-reports N] [--telemetry off|basic|detailed|trace]
              [--unmerged] [--priority ID,ID]
  feed RUN PORT TYPE:VALUE      for example u32:5, f64:1.5, bool:true, string:hello, bytes:00ff
  step RUN [COUNT]
  take RUN PORT
  watch RUN|all [--kinds fault,telemetry] [--count N]
  reload RUN GRAPH
  cancel RUN
  stop RUN [--immediate]
  inspect run RUN | graph NAME | telemetry RUN | host
  trace [LIMIT]
  cache inspect | clear
";

/// One command, ready to send.
#[derive(Debug, PartialEq)]
pub enum Command {
    Ping,
    LoadPlugin {
        path: String,
    },
    PutGraph {
        name: String,
        file: String,
    },
    PatchGraph {
        graph: String,
        file: String,
    },
    Plan {
        graph: String,
        options: PlanOptions,
    },
    Start {
        graph: String,
        config: StartConfig,
    },
    Feed {
        run: RunId,
        port: String,
        value: TypedValue,
    },
    Step {
        run: RunId,
        count: u32,
    },
    Take {
        run: RunId,
        port: String,
    },
    Watch {
        target: WatchTarget,
        kinds: EventKinds,
        count: Option<u64>,
    },
    Reload {
        run: RunId,
        graph: String,
    },
    Cancel {
        run: RunId,
    },
    Stop {
        run: RunId,
        mode: StopMode,
    },
    Inspect {
        target: InspectTarget,
    },
    Trace {
        limit: u32,
    },
    CacheInspect,
    CacheClear,
}

/// Why a command did not give a result.
#[derive(Debug)]
pub enum Failure {
    /// The command line, or a file it names, is wrong. Nothing was sent.
    Usage(String),
    /// The daemon refused, or the connection failed after it was made.
    Client(ClientError),
}

fn number<T: std::str::FromStr>(what: &str, text: &str) -> Result<T, String> {
    text.parse()
        .map_err(|_| format!("{what} must be a number, not `{text}`"))
}

/// The words of a command: its positionals in order, and its flags.
struct Words<'a> {
    command: &'a str,
    positional: Vec<&'a str>,
    flags: Vec<(&'a str, Option<&'a str>)>,
}

impl<'a> Words<'a> {
    /// Splits `words` into positionals and flags. `valued` lists the flags that take a value.
    fn new(
        command: &'a str,
        words: &'a [String],
        valued: &[&str],
        bare: &[&str],
    ) -> Result<Words<'a>, String> {
        let mut parsed = Words {
            command,
            positional: Vec::new(),
            flags: Vec::new(),
        };
        let mut words = words.iter().map(String::as_str);
        while let Some(word) = words.next() {
            if valued.contains(&word) {
                let value = words
                    .next()
                    .ok_or_else(|| format!("{command}: {word} needs a value"))?;
                parsed.flags.push((word, Some(value)));
            } else if bare.contains(&word) {
                parsed.flags.push((word, None));
            } else if word.starts_with("--") {
                return Err(format!("{command}: unknown flag `{word}`"));
            } else {
                parsed.positional.push(word);
            }
        }
        Ok(parsed)
    }

    fn arity(&self, names: &[&str], optional: usize) -> Result<(), String> {
        let (most, least) = (names.len(), names.len() - optional);
        if self.positional.len() < least || self.positional.len() > most {
            return Err(format!("usage: {} {}", self.command, names.join(" ")));
        }
        Ok(())
    }

    fn at(&self, index: usize) -> &'a str {
        self.positional.get(index).copied().unwrap_or("")
    }

    fn flag(&self, name: &str) -> Option<&'a str> {
        self.flags
            .iter()
            .find(|(flag, _)| *flag == name)
            .and_then(|(_, value)| *value)
    }

    fn has(&self, name: &str) -> bool {
        self.flags.iter().any(|(flag, _)| *flag == name)
    }

    fn run(&self, index: usize) -> Result<RunId, String> {
        number("RUN", self.at(index))
    }
}

fn priority(words: &Words<'_>) -> Vec<String> {
    words
        .flag("--priority")
        .map(|list| {
            list.split(',')
                .filter(|id| !id.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) || !text.is_ascii() {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).ok())
        .collect()
}

/// A scalar value written as `TYPE:VALUE`.
pub(crate) fn parse_value(text: &str) -> Result<TypedValue, String> {
    let (name, rest) = text.split_once(':').unwrap_or((text, ""));
    let scalar = output::SCALAR_NAMES
        .iter()
        .find(|(known, _)| *known == name)
        .map(|(_, scalar)| *scalar)
        .ok_or_else(|| {
            let names: Vec<&str> = output::SCALAR_NAMES.iter().map(|(name, _)| *name).collect();
            format!(
                "a value is TYPE:VALUE, and `{name}` is not one of {}",
                names.join(", ")
            )
        })?;
    let bad = || format!("`{rest}` is not a {name}");
    let value = match scalar {
        Scalar::Unit => WireValue::Unit,
        Scalar::Bool => WireValue::Bool(rest.parse().map_err(|_| bad())?),
        Scalar::I8 => WireValue::I8(rest.parse().map_err(|_| bad())?),
        Scalar::I16 => WireValue::I16(rest.parse().map_err(|_| bad())?),
        Scalar::I32 => WireValue::I32(rest.parse().map_err(|_| bad())?),
        Scalar::Int => WireValue::I64(rest.parse().map_err(|_| bad())?),
        Scalar::ISize => WireValue::ISize(rest.parse().map_err(|_| bad())?),
        Scalar::U8 => WireValue::U8(rest.parse().map_err(|_| bad())?),
        Scalar::U16 => WireValue::U16(rest.parse().map_err(|_| bad())?),
        Scalar::U32 => WireValue::U32(rest.parse().map_err(|_| bad())?),
        Scalar::U64 => WireValue::U64(rest.parse().map_err(|_| bad())?),
        Scalar::USize => WireValue::USize(rest.parse().map_err(|_| bad())?),
        Scalar::F32 => WireValue::F32(rest.parse().map_err(|_| bad())?),
        Scalar::Float => WireValue::F64(rest.parse().map_err(|_| bad())?),
        Scalar::String => WireValue::String(rest.to_owned()),
        Scalar::Bytes => WireValue::Bytes(unhex(rest).ok_or_else(bad)?),
    };
    Ok(TypedValue::new(WireType::Scalar(scalar), value))
}

impl Command {
    /// Parses a command and its words. The error is a usage message.
    pub fn parse(command: &str, rest: &[String]) -> Result<Command, String> {
        let words = |valued: &[&str], bare: &[&str]| Words::new(command, rest, valued, bare);
        Ok(match command {
            "ping" => {
                words(&[], &[])?.arity(&[], 0)?;
                Command::Ping
            }
            "load-plugin" => {
                let words = words(&[], &[])?;
                words.arity(&["PATH"], 0)?;
                Command::LoadPlugin {
                    path: words.at(0).to_owned(),
                }
            }
            "put-graph" => {
                let words = words(&[], &[])?;
                words.arity(&["NAME", "FILE"], 0)?;
                Command::PutGraph {
                    name: words.at(0).to_owned(),
                    file: words.at(1).to_owned(),
                }
            }
            "patch-graph" => {
                let words = words(&[], &[])?;
                words.arity(&["GRAPH", "FILE"], 0)?;
                Command::PatchGraph {
                    graph: words.at(0).to_owned(),
                    file: words.at(1).to_owned(),
                }
            }
            "plan" => {
                let words = words(&["--form", "--priority"], &["--inspect"])?;
                words.arity(&["GRAPH"], 0)?;
                let form = match words.flag("--form") {
                    None => PlanForm::Steps,
                    Some(name) => PlanForm::ALL
                        .into_iter()
                        .find(|form| form.name() == name)
                        .ok_or_else(|| {
                            format!("plan: --form is steps, blob or source, not `{name}`")
                        })?,
                };
                Command::Plan {
                    graph: words.at(0).to_owned(),
                    options: PlanOptions {
                        form,
                        inspect: words.has("--inspect"),
                        priority: priority(&words),
                    },
                }
            }
            "start" => {
                let words = words(
                    &["--fifo", "--fault-reports", "--telemetry", "--priority"],
                    &["--unmerged"],
                )?;
                words.arity(&["GRAPH"], 0)?;
                let telemetry = match words.flag("--telemetry") {
                    None => TelemetryLevel::default(),
                    Some(name) => TelemetryLevel::ALL
                        .into_iter()
                        .find(|level| level.name() == name)
                        .ok_or_else(|| {
                            format!(
                                "start: --telemetry is off, basic, detailed or trace, not `{name}`"
                            )
                        })?,
                };
                Command::Start {
                    graph: words.at(0).to_owned(),
                    config: StartConfig {
                        fifo_capacity: words
                            .flag("--fifo")
                            .map_or(Ok(0), |text| number("--fifo", text))?,
                        fault_reports: words
                            .flag("--fault-reports")
                            .map_or(Ok(0), |text| number("--fault-reports", text))?,
                        telemetry,
                        unmerged: words.has("--unmerged"),
                        priority: priority(&words),
                    },
                }
            }
            "feed" => {
                let words = words(&[], &[])?;
                words.arity(&["RUN", "PORT", "TYPE:VALUE"], 0)?;
                Command::Feed {
                    run: words.run(0)?,
                    port: words.at(1).to_owned(),
                    value: parse_value(words.at(2)).map_err(|error| format!("feed: {error}"))?,
                }
            }
            "step" => {
                let words = words(&[], &[])?;
                words.arity(&["RUN", "[COUNT]"], 1)?;
                Command::Step {
                    run: words.run(0)?,
                    count: match words.positional.get(1) {
                        None => 1,
                        Some(text) => number("COUNT", text)?,
                    },
                }
            }
            "take" => {
                let words = words(&[], &[])?;
                words.arity(&["RUN", "PORT"], 0)?;
                Command::Take {
                    run: words.run(0)?,
                    port: words.at(1).to_owned(),
                }
            }
            "watch" => {
                let words = words(&["--kinds", "--count"], &[])?;
                words.arity(&["RUN|all"], 0)?;
                let target = match words.at(0) {
                    "all" => WatchTarget::All,
                    run => WatchTarget::Run(number("RUN", run)?),
                };
                let mut kinds = EventKinds(0);
                for kind in words
                    .flag("--kinds")
                    .unwrap_or("fault,telemetry")
                    .split(',')
                {
                    kinds = kinds.with(match kind {
                        "fault" => EventKinds::FAULT,
                        "telemetry" => EventKinds::TELEMETRY,
                        other => {
                            return Err(format!(
                                "watch: --kinds is fault or telemetry, not `{other}`"
                            ));
                        }
                    });
                }
                Command::Watch {
                    target,
                    kinds,
                    count: words
                        .flag("--count")
                        .map(|text| number("--count", text))
                        .transpose()?,
                }
            }
            "reload" => {
                let words = words(&[], &[])?;
                words.arity(&["RUN", "GRAPH"], 0)?;
                Command::Reload {
                    run: words.run(0)?,
                    graph: words.at(1).to_owned(),
                }
            }
            "cancel" => {
                let words = words(&[], &[])?;
                words.arity(&["RUN"], 0)?;
                Command::Cancel { run: words.run(0)? }
            }
            "stop" => {
                let words = words(&[], &["--immediate"])?;
                words.arity(&["RUN"], 0)?;
                Command::Stop {
                    run: words.run(0)?,
                    mode: match words.has("--immediate") {
                        true => StopMode::Immediate,
                        false => StopMode::Drain,
                    },
                }
            }
            "inspect" => {
                let words = words(&[], &[])?;
                let usage = "usage: inspect run RUN | graph NAME | telemetry RUN | host";
                let target = match (words.at(0), words.positional.len()) {
                    ("run", 2) => InspectTarget::Run(words.run(1)?),
                    ("telemetry", 2) => InspectTarget::Telemetry(words.run(1)?),
                    ("graph", 2) => InspectTarget::Graph(words.at(1).to_owned()),
                    ("host", 1) => InspectTarget::Host,
                    _ => return Err(usage.to_owned()),
                };
                Command::Inspect { target }
            }
            "trace" => {
                let words = words(&[], &[])?;
                words.arity(&["[LIMIT]"], 1)?;
                Command::Trace {
                    limit: match words.positional.first() {
                        None => 0,
                        Some(text) => number("LIMIT", text)?,
                    },
                }
            }
            "cache" => {
                let words = words(&[], &[])?;
                match (words.at(0), words.positional.len()) {
                    ("inspect", 1) => Command::CacheInspect,
                    ("clear", 1) => Command::CacheClear,
                    _ => return Err("usage: cache inspect | clear".to_owned()),
                }
            }
            other => return Err(format!("unknown command `{other}`")),
        })
    }
}

fn read(file: &str) -> Result<String, Failure> {
    std::fs::read_to_string(file).map_err(|error| Failure::Usage(format!("{file}: {error}")))
}

/// Sends a command that has one answer, and gives its result.
pub fn execute(command: &Command, client: &Client) -> Result<Value, Failure> {
    let sent = match command {
        Command::Ping => client.ping().map(|version| json!({ "version": version })),
        Command::LoadPlugin { path } => client.load_plugin(path).map(|info| output::plugin(&info)),
        Command::PutGraph { name, file } => client
            .put_graph(name, &read(file)?)
            .map(|found| json!({ "graph": name, "diagnostics": output::diagnostics(&found) })),
        Command::PatchGraph { graph, file } => client
            .patch_graph(graph, &read(file)?)
            .map(|found| json!({ "graph": graph, "diagnostics": output::diagnostics(&found) })),
        Command::Plan { graph, options } => client
            .plan(graph, options.clone())
            .map(|summary| output::plan(&summary)),
        Command::Start { graph, config } => client
            .start(graph, config.clone())
            .map(|run| json!({ "run": run })),
        Command::Feed { run, port, value } => client
            .feed(*run, port, value.clone())
            .map(|fed| json!({ "woke": fed.woke, "dropped": fed.dropped })),
        Command::Step { run, count } => client
            .step(*run, *count)
            .map(|summary| output::stepped(&summary)),
        Command::Take { run, port } => client.take(*run, port).map(|value| output::typed(&value)),
        Command::Reload { run, graph } => client
            .reload(*run, graph)
            .map(|report| output::carry(&report)),
        Command::Cancel { run } => client.cancel(*run).map(|()| json!({})),
        Command::Stop { run, mode } => client
            .stop(*run, *mode)
            .map(|stopped| json!({ "run": run, "tick": stopped.tick, "dropped": stopped.dropped })),
        Command::Inspect { target } => client
            .inspect(target.clone())
            .map(|found| output::inspection(&found)),
        Command::Trace { limit } => client.trace(*limit).map(|entries| output::trace(&entries)),
        Command::CacheInspect => client.cache_inspect().map(
            |info| json!({ "entries": info.entries, "hits": info.hits, "misses": info.misses }),
        ),
        Command::CacheClear => client.cache_clear().map(|()| json!({})),
        Command::Watch { .. } => {
            return Err(Failure::Usage("watch is a stream".to_owned()));
        }
    };
    sent.map_err(Failure::Client)
}

/// Watches until the stream ends or `count` events have arrived. Each event is handed to `show`
/// as it comes. Returns false when the connection was lost before the watch ended.
pub fn watch(
    client: &Client,
    target: WatchTarget,
    kinds: EventKinds,
    count: Option<u64>,
    out: &mut dyn Write,
    mut show: impl FnMut(&mut dyn Write, &Value),
) -> Result<bool, Failure> {
    let stream = client.watch(target, kinds).map_err(Failure::Client)?;
    show(out, &json!({ "watching": stream.id() }));
    let mut seen = 0u64;
    let mut ended = false;
    while count.is_none_or(|count| seen < count) {
        let Some(event) = stream.next_event() else {
            break;
        };
        seen += 1;
        // The stream of a watch on one run ends with that run. One on every run ends only with
        // the watch itself.
        ended = matches!(
            (&event.body, target),
            (EventBody::Ended { .. }, WatchTarget::Run(_))
                | (EventBody::Ended { run: 0, .. }, WatchTarget::All)
        );
        show(out, &output::event(&event));
        if ended {
            break;
        }
    }
    Ok(ended || count.is_some_and(|count| seen >= count))
}

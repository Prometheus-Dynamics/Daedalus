//! Reads one command line of a scenario (the contract is in the crate's top-level doc). Tokens are
//! separated by single spaces. The last token of `feed` is the rest of the line.

use daedalus_client::protocol::{
    EventKinds, InspectTarget, PlanForm, PlanOptions, StartConfig, StopMode, TelemetryLevel,
    TypedValue, WatchTarget,
};
use daedalus_types::Scalar;
use daedalus_wire::{WireType, WireValue};

/// One command of a scenario.
#[derive(Debug, PartialEq)]
pub enum Cmd {
    Ping,
    /// `session NAME`, or `session` alone for the default session.
    Session(String),
    LoadPlugin(String),
    PutGraph {
        name: String,
        file: String,
    },
    PatchGraph {
        name: String,
        file: String,
    },
    Plan {
        graph: String,
        options: PlanOptions,
    },
    Blob {
        name: String,
        hex: String,
    },
    MakePlan {
        graph: String,
        blob: String,
        config: StartConfig,
    },
    PutPlan {
        name: String,
        blob: String,
    },
    Start {
        graph: String,
        config: StartConfig,
    },
    Feed {
        run: u64,
        port: String,
        value: TypedValue,
    },
    Step {
        run: u64,
        count: u32,
    },
    Take {
        run: u64,
        port: String,
    },
    Watch {
        id: u64,
        target: WatchTarget,
        kinds: EventKinds,
    },
    Next {
        id: u64,
    },
    Unwatch {
        id: u64,
    },
    Reload {
        run: u64,
        graph: String,
    },
    Cancel {
        run: u64,
    },
    Stop {
        run: u64,
        mode: StopMode,
    },
    Inspect(InspectTarget),
    CacheClear,
    Trace(u32),
}

/// Parses a command line. The error is a usage message.
pub fn parse(line: &str) -> Result<Cmd, String> {
    let words: Vec<&str> = line.split(' ').collect();
    let usage = || format!("cannot read `{line}`");
    let num = |text: &str| text.parse::<u64>().map_err(|_| usage());
    Ok(match words.as_slice() {
        ["ping"] => Cmd::Ping,
        ["session"] => Cmd::Session(String::new()),
        ["session", name] => Cmd::Session((*name).to_owned()),
        ["load_plugin", path] => Cmd::LoadPlugin((*path).to_owned()),
        ["put_graph", name, file] => Cmd::PutGraph {
            name: (*name).to_owned(),
            file: (*file).to_owned(),
        },
        ["patch_graph", name, file] => Cmd::PatchGraph {
            name: (*name).to_owned(),
            file: (*file).to_owned(),
        },
        ["plan", graph, form, rest @ ..] => Cmd::Plan {
            graph: (*graph).to_owned(),
            options: plan_options(form, rest)?,
        },
        ["blob", name, hex] => Cmd::Blob {
            name: (*name).to_owned(),
            hex: (*hex).to_owned(),
        },
        ["make_plan", graph, blob, rest @ ..] => Cmd::MakePlan {
            graph: (*graph).to_owned(),
            blob: (*blob).to_owned(),
            config: start_config(rest)?,
        },
        ["put_plan", name, blob] => Cmd::PutPlan {
            name: (*name).to_owned(),
            blob: (*blob).to_owned(),
        },
        ["start", graph, rest @ ..] => Cmd::Start {
            graph: (*graph).to_owned(),
            config: start_config(rest)?,
        },
        ["feed", run, port, ..] => {
            let typed = line.splitn(4, ' ').nth(3).ok_or_else(usage)?;
            Cmd::Feed {
                run: num(run)?,
                port: (*port).to_owned(),
                value: parse_value(typed)?,
            }
        }
        ["step", run, count] => Cmd::Step {
            run: num(run)?,
            count: count.parse().map_err(|_| usage())?,
        },
        ["take", run, port] => Cmd::Take {
            run: num(run)?,
            port: (*port).to_owned(),
        },
        ["watch", id, target, rest @ ..] => Cmd::Watch {
            id: num(id)?,
            target: match *target {
                "all" => WatchTarget::All,
                run => WatchTarget::Run(num(run)?),
            },
            kinds: event_kinds(rest)?,
        },
        ["next", id] => Cmd::Next { id: num(id)? },
        ["unwatch", id] => Cmd::Unwatch { id: num(id)? },
        ["reload", run, graph] => Cmd::Reload {
            run: num(run)?,
            graph: (*graph).to_owned(),
        },
        ["cancel", run] => Cmd::Cancel { run: num(run)? },
        ["stop", run, mode] => Cmd::Stop {
            run: num(run)?,
            mode: match *mode {
                "drain" => StopMode::Drain,
                "immediate" => StopMode::Immediate,
                _ => return Err(usage()),
            },
        },
        ["inspect", "run", run] => Cmd::Inspect(InspectTarget::Run(num(run)?)),
        ["inspect", "telemetry", run] => Cmd::Inspect(InspectTarget::Telemetry(num(run)?)),
        ["inspect", "graph", name] => Cmd::Inspect(InspectTarget::Graph((*name).to_owned())),
        ["inspect", "cache"] => Cmd::Inspect(InspectTarget::Cache),
        ["inspect", "host"] => Cmd::Inspect(InspectTarget::Host),
        ["cache", "clear"] => Cmd::CacheClear,
        ["trace", limit] => Cmd::Trace(limit.parse().map_err(|_| usage())?),
        _ => return Err(usage()),
    })
}

fn plan_options(form: &str, rest: &[&str]) -> Result<PlanOptions, String> {
    let form = match form {
        "steps" => PlanForm::Steps,
        "blob" => PlanForm::Blob,
        "source" => PlanForm::Source,
        other => return Err(format!("no plan form `{other}`")),
    };
    let mut options = PlanOptions {
        form,
        ..PlanOptions::default()
    };
    for word in rest {
        match *word {
            "inspect" => options.inspect = true,
            other => options.priority = priority(other)?,
        }
    }
    Ok(options)
}

/// The start options: `fifo=N`, `faults=N`, `telemetry=LEVEL`, `unmerged`, `priority=A,B`.
fn start_config(words: &[&str]) -> Result<StartConfig, String> {
    let mut config = StartConfig::default();
    for word in words {
        if *word == "unmerged" {
            config.unmerged = true;
        } else if let Some(value) = word.strip_prefix("fifo=") {
            config.fifo_capacity = value.parse().map_err(|_| format!("bad `{word}`"))?;
        } else if let Some(value) = word.strip_prefix("faults=") {
            config.fault_reports = value.parse().map_err(|_| format!("bad `{word}`"))?;
        } else if let Some(value) = word.strip_prefix("telemetry=") {
            config.telemetry = match value {
                "off" => TelemetryLevel::Off,
                "basic" => TelemetryLevel::Basic,
                "detailed" => TelemetryLevel::Detailed,
                "trace" => TelemetryLevel::Trace,
                other => return Err(format!("no telemetry level `{other}`")),
            };
        } else if let Some(value) = word.strip_prefix("priority=") {
            config.priority = priority(&format!("priority={value}"))?;
        } else {
            return Err(format!("no start option `{word}`"));
        }
    }
    Ok(config)
}

fn priority(word: &str) -> Result<Vec<String>, String> {
    let list = word
        .strip_prefix("priority=")
        .ok_or_else(|| format!("no option `{word}`"))?;
    Ok(list.split(',').map(str::to_owned).collect())
}

fn event_kinds(words: &[&str]) -> Result<EventKinds, String> {
    match words {
        [] | ["all"] => Ok(EventKinds::ALL),
        ["fault"] => Ok(EventKinds::FAULT),
        ["telemetry"] => Ok(EventKinds::TELEMETRY),
        _ => Err("watch kinds are fault, telemetry or all".to_owned()),
    }
}

/// `TYPE:VALUE`, as `daedalus-ctl` reads it. Floats are not part of the suite.
pub fn parse_value(text: &str) -> Result<TypedValue, String> {
    let (name, rest) = text.split_once(':').unwrap_or((text, ""));
    let bad = || format!("`{rest}` is not a {name}");
    let (scalar, value) = match name {
        "unit" => (Scalar::Unit, WireValue::Unit),
        "bool" => (
            Scalar::Bool,
            WireValue::Bool(rest.parse().map_err(|_| bad())?),
        ),
        "i8" => (Scalar::I8, WireValue::I8(rest.parse().map_err(|_| bad())?)),
        "i16" => (
            Scalar::I16,
            WireValue::I16(rest.parse().map_err(|_| bad())?),
        ),
        "i32" => (
            Scalar::I32,
            WireValue::I32(rest.parse().map_err(|_| bad())?),
        ),
        "i64" => (
            Scalar::Int,
            WireValue::I64(rest.parse().map_err(|_| bad())?),
        ),
        "isize" => (
            Scalar::ISize,
            WireValue::ISize(rest.parse().map_err(|_| bad())?),
        ),
        "u8" => (Scalar::U8, WireValue::U8(rest.parse().map_err(|_| bad())?)),
        "u16" => (
            Scalar::U16,
            WireValue::U16(rest.parse().map_err(|_| bad())?),
        ),
        "u32" => (
            Scalar::U32,
            WireValue::U32(rest.parse().map_err(|_| bad())?),
        ),
        "u64" => (
            Scalar::U64,
            WireValue::U64(rest.parse().map_err(|_| bad())?),
        ),
        "usize" => (
            Scalar::USize,
            WireValue::USize(rest.parse().map_err(|_| bad())?),
        ),
        "string" => (Scalar::String, WireValue::String(rest.to_owned())),
        "bytes" => (
            Scalar::Bytes,
            WireValue::Bytes(unhex(rest).ok_or_else(bad)?),
        ),
        other => return Err(format!("`{other}` is not a suite value type")),
    };
    Ok(TypedValue::new(WireType::Scalar(scalar), value))
}

/// Lowercase or uppercase hex pairs. An empty text is no bytes.
pub fn unhex(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(2) || !text.is_ascii() {
        return None;
    }
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_feed_takes_the_rest_of_the_line_as_its_value() {
        let Cmd::Feed { run, port, value } = parse("feed 3 in string:hi there").unwrap() else {
            panic!("a feed");
        };
        assert_eq!((run, port.as_str()), (3, "in"));
        assert_eq!(value.value, WireValue::String("hi there".to_owned()));
    }

    #[test]
    fn a_watch_names_a_target_and_kinds() {
        assert_eq!(
            parse("watch 2 1 fault").unwrap(),
            Cmd::Watch {
                id: 2,
                target: WatchTarget::Run(1),
                kinds: EventKinds::FAULT,
            }
        );
        assert!(parse("watch 2 all nothing").is_err());
    }

    #[test]
    fn start_options_are_read_in_any_order() {
        let Cmd::Start { config, .. } = parse("start g unmerged telemetry=trace fifo=4").unwrap()
        else {
            panic!("a start");
        };
        assert!(config.unmerged);
        assert_eq!(config.fifo_capacity, 4);
        assert_eq!(config.telemetry, TelemetryLevel::Trace);
    }
}

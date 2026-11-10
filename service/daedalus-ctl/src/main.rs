//! `daedalus-ctl`: the command line client for `daedalusd`. One command, one request, through
//! the service client. Output is text, or JSON with `--json`.
//!
//! Exit codes: 0 success, 1 the daemon refused or the connection failed after it was made,
//! 2 usage error, 3 could not connect.

#![forbid(unsafe_code)]

mod commands;
mod output;

use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use daedalus_client::{Client, ClientError, Endpoint, Options};
use serde_json::{Value, json};

use commands::{COMMANDS, Command, Failure};

const USAGE: &str = "\
usage: daedalus-ctl [--socket PATH | --tcp ADDRESS] [--session NAME] [--json] COMMAND ...

  --socket PATH    the daemon's unix socket (default: the daemon's default)
  --tcp ADDRESS    a loopback address the daemon listens on, in place of a socket
  --session NAME   the session whose graphs the command names (default: default)
  --json           print JSON, for scripts

";

const EXIT_DAEMON: u8 = 1;
const EXIT_USAGE: u8 = 2;
const EXIT_CONNECT: u8 = 3;

/// The socket the daemon listens on when its config names none.
#[cfg(unix)]
fn default_socket() -> PathBuf {
    let system = PathBuf::from("/run/daedalusd/daedalusd.sock");
    match std::env::var("XDG_RUNTIME_DIR") {
        Ok(dir) if !dir.is_empty() => {
            let user = PathBuf::from(dir).join("daedalusd").join("daedalusd.sock");
            // A daemon run by this user listens there. Without one, the system's is the default.
            if user.exists() { user } else { system }
        }
        _ => system,
    }
}

struct Invocation {
    socket: Option<PathBuf>,
    tcp: Option<String>,
    session: Option<String>,
    json: bool,
    command: String,
    rest: Vec<String>,
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Invocation, String> {
    let mut invocation = Invocation {
        socket: None,
        tcp: None,
        session: None,
        json: false,
        command: String::new(),
        rest: Vec::new(),
    };
    while let Some(arg) = args.next() {
        let mut value = |name: &str| args.next().ok_or(format!("{name} needs a value"));
        match arg.as_str() {
            "--socket" => invocation.socket = Some(PathBuf::from(value("--socket")?)),
            "--tcp" => invocation.tcp = Some(value("--tcp")?),
            "--session" => invocation.session = Some(value("--session")?),
            "--json" => invocation.json = true,
            "--help" | "-h" | "help" => return Err(String::new()),
            flag if flag.starts_with("--") && invocation.command.is_empty() => {
                return Err(format!("unknown flag `{flag}`"));
            }
            // `--json` may follow the command too. Everything else after it is the command's.
            _ if invocation.command.is_empty() => invocation.command = arg,
            _ => invocation.rest.push(arg),
        }
    }
    if invocation.command.is_empty() {
        return Err("a command is needed".to_owned());
    }
    if invocation.socket.is_some() && invocation.tcp.is_some() {
        return Err("--socket and --tcp are one or the other".to_owned());
    }
    Ok(invocation)
}

fn endpoint(invocation: &Invocation) -> Result<Endpoint, String> {
    if let Some(address) = &invocation.tcp {
        let address: SocketAddr = address
            .parse()
            .map_err(|_| format!("--tcp: `{address}` is not an address"))?;
        return Endpoint::tcp(address).map_err(|error| format!("--tcp: {error}"));
    }
    #[cfg(unix)]
    {
        Ok(Endpoint::unix(
            invocation.socket.clone().unwrap_or_else(default_socket),
        ))
    }
    #[cfg(not(unix))]
    {
        Err("this platform has no unix sockets: name the daemon with --tcp".to_owned())
    }
}

/// Prints to stdout. A closed pipe is not this tool's failure to report.
fn print(text: &str) {
    let _ = std::io::stdout().lock().write_all(text.as_bytes());
}

fn complain(text: &str) {
    let _ = std::io::stderr().lock().write_all(text.as_bytes());
}

/// Reports a failure in the asked form, and gives its exit code.
fn fail(
    json: bool,
    command: &str,
    kind: &str,
    code: u8,
    message: &str,
    detail: Option<Value>,
) -> ExitCode {
    if json {
        let error = detail.unwrap_or_else(|| json!({ "kind": kind, "message": message }));
        print(&format!("{}\n", output::failure(command, error)));
    } else {
        complain(&format!("daedalus-ctl: {message}\n"));
    }
    ExitCode::from(code)
}

fn client_failure(json: bool, command: &str, error: &ClientError) -> ExitCode {
    match error {
        ClientError::Service(refused) => {
            let mut message = refused.to_string();
            for found in &refused.diagnostics {
                message.push_str(&format!("\n  {}: {}", found.code, found.message));
            }
            let detail = output::service_error(refused);
            fail(json, command, "daemon", EXIT_DAEMON, &message, Some(detail))
        }
        other => fail(
            json,
            command,
            "daemon",
            EXIT_DAEMON,
            &other.to_string(),
            None,
        ),
    }
}

fn main() -> ExitCode {
    let invocation = match parse(std::env::args().skip(1)) {
        Ok(invocation) => invocation,
        Err(message) if message.is_empty() => {
            print(&format!("{USAGE}{COMMANDS}"));
            return ExitCode::SUCCESS;
        }
        Err(message) => {
            complain(&format!("daedalus-ctl: {message}\n{USAGE}{COMMANDS}"));
            return ExitCode::from(EXIT_USAGE);
        }
    };
    let (json, name) = (invocation.json, invocation.command.as_str());
    let usage = |message: &str| fail(json, name, "usage", EXIT_USAGE, message, None);
    let command = match Command::parse(name, &invocation.rest) {
        Ok(command) => command,
        Err(message) => return usage(&message),
    };
    let endpoint = match endpoint(&invocation) {
        Ok(endpoint) => endpoint,
        Err(message) => return usage(&message),
    };
    let client = match Client::connect(endpoint.clone(), Options::default()) {
        Ok(client) => client,
        // The daemon answered, and said no: that is its refusal, not a failure to reach it.
        Err(error @ ClientError::Service(_)) => return client_failure(json, name, &error),
        Err(error) => {
            let message = format!("could not connect to {endpoint}: {error}");
            return fail(json, name, "connect", EXIT_CONNECT, &message, None);
        }
    };
    let client = match &invocation.session {
        None => client,
        Some(session) => match client.in_session(session) {
            Ok(client) => client,
            Err(error) => return usage(&format!("--session: {error}")),
        },
    };
    if let Command::Watch {
        target,
        kinds,
        count,
    } = command
    {
        let mut stdout = std::io::stdout();
        let show = |out: &mut dyn Write, value: &Value| {
            let line = match json {
                true => format!("{value}\n"),
                // One event a paragraph, so a reader can tell them apart.
                false => format!("{}\n", output::text(value)),
            };
            let _ = out.write_all(line.as_bytes());
            let _ = out.flush();
        };
        return match commands::watch(&client, target, kinds, count, &mut stdout, show) {
            Ok(true) => ExitCode::SUCCESS,
            Ok(false) => fail(
                json,
                name,
                "daemon",
                EXIT_DAEMON,
                "the connection was lost",
                None,
            ),
            Err(Failure::Client(error)) => client_failure(json, name, &error),
            Err(Failure::Usage(message)) => usage(&message),
        };
    }
    match commands::execute(&command, &client) {
        Ok(result) => {
            match json {
                true => print(&format!("{}\n", output::success(name, result))),
                false => print(&output::text(&result)),
            }
            ExitCode::SUCCESS
        }
        Err(Failure::Client(error)) => client_failure(json, name, &error),
        Err(Failure::Usage(message)) => usage(&message),
    }
}

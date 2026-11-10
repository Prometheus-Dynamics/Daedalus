//! `daedalusd`: one Daedalus host in its own process. Clients reach it over a unix socket, TCP
//! on the loopback interface, or its own stdin and stdout. It loads the plugins in its plugin
//! directory, and SIGTERM or SIGINT stops it: listeners close, runs are cancelled and dropped,
//! and each plugin's library is closed once nothing compiled from it is left.

#![forbid(unsafe_code)]

mod config;
mod logging;
mod plugins;
mod signals;

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use daedalus_host::listen::{ListenOptions, Listener, ServiceHost};
use daedalus_registry::Registry;

use config::{Config, DEFAULT_CONFIG, Listen};
use logging::{error, info, warning};
use plugins::{Plugins, Probes};
use signals::Stop;

const USAGE: &str = "\
usage: daedalusd [--config FILE] [--check-config] [--stdio]

  --config FILE    read FILE (default /etc/daedalusd/daedalusd.toml, if it exists)
  --check-config   check the config, print what it resolves to, and exit
  --stdio          serve one session on stdin and stdout, and exit when stdin closes
";

/// How long a stopping daemon waits for sessions to close and plugins to be released.
const GRACE: Duration = Duration::from_secs(10);
const POLL: Duration = Duration::from_millis(20);

struct Args {
    config: Option<PathBuf>,
    check: bool,
    stdio: bool,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut parsed = Args {
        config: None,
        check: false,
        stdio: false,
    };
    let mut args = args;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => {
                let path = args.next().ok_or("--config needs a file")?;
                parsed.config = Some(PathBuf::from(path));
            }
            "--check-config" => parsed.check = true,
            "--stdio" => parsed.stdio = true,
            "--help" | "-h" => return Err(String::new()),
            other => return Err(format!("unknown argument `{other}`")),
        }
    }
    Ok(parsed)
}

fn out(text: &str) {
    let _ = std::io::stdout().lock().write_all(text.as_bytes());
}

fn err(text: &str) {
    let _ = std::io::stderr().lock().write_all(text.as_bytes());
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// The config: the named file, or the default one if it is there, or every default.
fn load_config(path: Option<&Path>) -> Result<(Config, String), config::ConfigError> {
    match path {
        Some(path) => Ok((Config::load(path)?, path.display().to_string())),
        None if Path::new(DEFAULT_CONFIG).exists() => Ok((
            Config::load(Path::new(DEFAULT_CONFIG))?,
            DEFAULT_CONFIG.to_owned(),
        )),
        None => Ok((Config::default(), "(defaults)".to_owned())),
    }
}

/// The sockets a service manager passed in, if it passed any.
#[cfg(unix)]
fn adopted(host: &Arc<ServiceHost>) -> Vec<Listener> {
    let mut passed = listenfd::ListenFd::from_env();
    let mut listeners = Vec::new();
    for index in 0..passed.len() {
        match passed.take_unix_listener(index) {
            Ok(Some(listener)) => listeners.push(host.listen_unix_adopted(listener)),
            Ok(None) => {}
            Err(error) => warning!("passed socket {index} is not a unix listener: {error}"),
        }
    }
    listeners
}

#[cfg(not(unix))]
fn adopted(_: &Arc<ServiceHost>) -> Vec<Listener> {
    Vec::new()
}

fn open(host: &Arc<ServiceHost>, listen: &Listen) -> std::io::Result<Listener> {
    match listen {
        #[cfg(unix)]
        Listen::Unix(path) => {
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            host.listen_unix(path)
        }
        #[cfg(not(unix))]
        Listen::Unix(path) => Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("{}: this platform has no unix sockets", path.display()),
        )),
        Listen::Tcp(addr) => host.listen_tcp(*addr).map(|(listener, _)| listener),
    }
}

/// Waits for `done`, for at most the grace period.
fn within_grace(mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + GRACE;
    while !done() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(POLL);
    }
    true
}

fn run(args: &Args, config: &Config) -> Result<bool, String> {
    let stop = Stop::install().map_err(|error| format!("signal handlers: {error}"))?;
    let probes: Probes = Arc::new(Mutex::new(Vec::new()));
    let options = ListenOptions {
        max_runs: config.max_runs,
        connections: match config.connections {
            0 => ListenOptions::default().connections,
            limit => limit,
        },
        ..ListenOptions::default()
    };
    let host = ServiceHost::builder(Registry::new())
        .plugins(Box::new(Plugins::new(Arc::clone(&probes))))
        .options(options)
        .build();

    // Plugins first, so a client that connects finds them. One that fails is tried again in the
    // background and does not hold the daemon up.
    let failed = match &config.plugin_dir {
        Some(dir) => plugins::load_all(&host, plugins::scan(dir)),
        None => Vec::new(),
    };
    let retrying = plugins::retry_in_background(Arc::clone(&host), failed, stop.flag());

    let mut listeners = adopted(&host);
    if listeners.is_empty() && !args.stdio {
        for listen in config.listeners(env).map_err(|error| error.to_string())? {
            let listener = open(&host, &listen).map_err(|error| format!("{listen}: {error}"))?;
            listeners.push(listener);
        }
    }
    for listener in &listeners {
        info!("listening on {}", listener.address());
    }
    if args.stdio {
        // One session on stdin and stdout. When stdin closes the daemon stops, as on a signal.
        let (host, stop) = (Arc::clone(&host), stop.clone());
        std::thread::Builder::new()
            .name("daedalusd-stdio".to_owned())
            .spawn(move || {
                host.serve_stdio();
                stop.ask();
            })
            .map_err(|error| format!("stdio session: {error}"))?;
    }

    while !stop.asked() {
        std::thread::sleep(POLL);
    }
    info!("stopping");
    let runs = host.shutdown();
    let drained = within_grace(|| host.connections() == 0);
    if !drained {
        warning!("{} connections did not close in time", host.connections());
    }
    for listener in listeners {
        listener.join();
    }
    if let Some(retrying) = retrying {
        let _ = retrying.join();
    }
    // The host holds the plugins' backends. With it gone, a library stays open only while a
    // step compiled from it is still held, and none should be.
    drop(host);
    let retired = within_grace(|| plugins::still_open(&probes).is_empty());
    for name in plugins::still_open(&probes) {
        warning!("plugin {name} was not released");
    }
    info!("stopped: {runs} runs dropped");
    Ok(drained && retired)
}

fn main() -> ExitCode {
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(message) if message.is_empty() => {
            out(USAGE);
            return ExitCode::SUCCESS;
        }
        Err(message) => {
            err(&format!("daedalusd: {message}\n{USAGE}"));
            return ExitCode::from(2);
        }
    };
    let (config, source) = match load_config(args.config.as_deref()) {
        Ok(loaded) => loaded,
        Err(error) => {
            err(&format!("daedalusd: config: {error}\n"));
            return ExitCode::FAILURE;
        }
    };
    let level = config.level().unwrap_or(logging::Level::Info);
    if args.check {
        return match config.listeners(env) {
            Ok(listeners) => {
                let addresses: Vec<String> = listeners.iter().map(Listen::to_string).collect();
                let plugins = config
                    .plugin_dir
                    .as_ref()
                    .map_or("(none)".to_owned(), |dir| dir.display().to_string());
                out(&format!(
                    "config ok: {source}\nlisten: {}\nplugin_dir: {plugins}\nmax_runs: {}\nlog_level: {}\n",
                    addresses.join(", "),
                    config.max_runs,
                    logging::Level::NAMES[level as usize],
                ));
                ExitCode::SUCCESS
            }
            Err(error) => {
                err(&format!("daedalusd: config: {source}: {error}\n"));
                ExitCode::FAILURE
            }
        };
    }
    logging::set_level(level);
    match run(&args, &config) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(message) => {
            error!("{message}");
            ExitCode::FAILURE
        }
    }
}

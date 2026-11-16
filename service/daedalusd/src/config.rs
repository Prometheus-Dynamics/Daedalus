//! The daemon's config file: where it listens, where its plugins are, how many runs it holds
//! and how much it logs. Every key is optional.

use std::fmt;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use daedalus_integrity::Integrity;
use serde::Deserialize;

use crate::logging::Level;

/// The socket the daemon listens on when the config names none and the environment gives no
/// runtime directory. The packaged socket unit listens on the same path.
pub const DEFAULT_SOCKET: &str = "/run/daedalusd/daedalusd.sock";
/// The config file read when `--config` is not given, if it exists.
pub const DEFAULT_CONFIG: &str = "/etc/daedalusd/daedalusd.toml";
/// The socket's file name inside a runtime directory.
pub(crate) const SOCKET_NAME: &str = "daedalusd.sock";

/// The file as written.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Addresses, each `unix:<absolute path>` or `tcp:<loopback address>:<port>`.
    pub listen: Vec<String>,
    /// The directory whose libraries are loaded at start.
    pub plugin_dir: Option<PathBuf>,
    /// The directory of made plans, `*.plan`, stored at start under their file names without the
    /// extension, in the default session. A plan is not started on its own.
    pub plan_dir: Option<PathBuf>,
    /// Most runs held at once. Zero is no limit.
    pub max_runs: u32,
    /// Most connections served at once. Zero is the default of 64.
    pub connections: usize,
    /// `error`, `warn`, `info` or `debug`.
    pub log_level: Option<String>,
    /// `off` (the default), `locked`, or `record`: whether plugin libraries and worker programs
    /// are checked against the lock.
    pub integrity: Option<String>,
    /// The lock file, as an absolute path. Needed under `locked` and `record`.
    pub lock_file: Option<PathBuf>,
}

/// One place to listen.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Listen {
    Unix(PathBuf),
    Tcp(SocketAddr),
}

impl fmt::Display for Listen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Listen::Unix(path) => write!(f, "unix:{}", path.display()),
            Listen::Tcp(addr) => write!(f, "tcp:{addr}"),
        }
    }
}

/// Why a config was refused. The message names the key.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The unix socket used when the config names no address: in the runtime directory a service
/// manager gave (`RUNTIME_DIRECTORY`), else in the user's (`XDG_RUNTIME_DIR`), else
/// [`DEFAULT_SOCKET`].
pub fn default_socket(env: impl Fn(&str) -> Option<String>) -> PathBuf {
    if let Some(dirs) = env("RUNTIME_DIRECTORY")
        && let Some(first) = dirs.split(':').find(|dir| !dir.is_empty())
    {
        return Path::new(first).join(SOCKET_NAME);
    }
    if let Some(dir) = env("XDG_RUNTIME_DIR").filter(|dir| !dir.is_empty()) {
        return Path::new(&dir).join("daedalusd").join(SOCKET_NAME);
    }
    PathBuf::from(DEFAULT_SOCKET)
}

impl Listen {
    pub fn parse(text: &str) -> Result<Listen, ConfigError> {
        if let Some(path) = text.strip_prefix("unix:") {
            let path = Path::new(path);
            if !path.is_absolute() {
                return Err(ConfigError(format!(
                    "listen: `{text}` is not an absolute path"
                )));
            }
            return Ok(Listen::Unix(path.to_path_buf()));
        }
        if let Some(addr) = text.strip_prefix("tcp:") {
            let addr: SocketAddr = addr.parse().map_err(|error| {
                ConfigError(format!("listen: `{text}` is not an address: {error}"))
            })?;
            if !addr.ip().is_loopback() {
                return Err(ConfigError(format!(
                    "listen: `{text}` is not a loopback address, and the daemon does not listen on the network"
                )));
            }
            return Ok(Listen::Tcp(addr));
        }
        Err(ConfigError(format!(
            "listen: `{text}` must start with `unix:` or `tcp:`"
        )))
    }
}

impl Config {
    pub fn parse(text: &str) -> Result<Config, ConfigError> {
        let config: Config =
            toml::from_str(text).map_err(|error| ConfigError(error.to_string()))?;
        config.check()?;
        Ok(config)
    }

    pub fn load(path: &Path) -> Result<Config, ConfigError> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| ConfigError(format!("{}: {error}", path.display())))?;
        Config::parse(&text).map_err(|error| ConfigError(format!("{}: {error}", path.display())))
    }

    /// Checks every value. Nothing is opened or bound.
    pub fn check(&self) -> Result<(), ConfigError> {
        for address in &self.listen {
            Listen::parse(address)?;
        }
        self.level()?;
        if let Some(dir) = &self.plugin_dir
            && !dir.is_absolute()
        {
            return Err(ConfigError(format!(
                "plugin_dir: `{}` is not an absolute path",
                dir.display()
            )));
        }
        if let Some(dir) = &self.plan_dir
            && !dir.is_absolute()
        {
            return Err(ConfigError(format!(
                "plan_dir: `{}` is not an absolute path",
                dir.display()
            )));
        }
        if let Some(lock) = &self.lock_file
            && !lock.is_absolute()
        {
            return Err(ConfigError(format!(
                "lock_file: `{}` is not an absolute path",
                lock.display()
            )));
        }
        self.integrity()?;
        Ok(())
    }

    /// The integrity mode the config names. Refused when the mode is not one of its three names,
    /// when it needs a lock and none is named, or when this platform cannot check files.
    pub fn integrity(&self) -> Result<Integrity, ConfigError> {
        let name = self.integrity.as_deref().unwrap_or("off");
        match name {
            "off" => Ok(Integrity::off()),
            "locked" | "record" => {
                let lock = self.lock_file.as_ref().ok_or_else(|| {
                    ConfigError(format!("lock_file: needed when integrity is `{name}`"))
                })?;
                let made = if name == "locked" {
                    Integrity::locked(lock)
                } else {
                    Integrity::record(lock)
                };
                made.map_err(|error| ConfigError(format!("integrity: {error}")))
            }
            other => Err(ConfigError(format!(
                "integrity: `{other}` is not one of off, locked, record"
            ))),
        }
    }

    pub fn level(&self) -> Result<Level, ConfigError> {
        match &self.log_level {
            None => Ok(Level::Info),
            Some(name) => Level::parse(name).ok_or_else(|| {
                ConfigError(format!(
                    "log_level: `{name}` is not one of {}",
                    Level::NAMES.join(", ")
                ))
            }),
        }
    }

    /// Where to listen: the configured addresses, or the default unix socket where there are
    /// unix sockets.
    pub fn listeners(
        &self,
        env: impl Fn(&str) -> Option<String>,
    ) -> Result<Vec<Listen>, ConfigError> {
        if self.listen.is_empty() {
            return match cfg!(unix) {
                true => Ok(vec![Listen::Unix(default_socket(env))]),
                false => Err(ConfigError(
                    "listen: this platform has no unix sockets, so name a `tcp:` address"
                        .to_owned(),
                )),
            };
        }
        self.listen.iter().map(|text| Listen::parse(text)).collect()
    }
}

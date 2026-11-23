//! The in-process service the suite drives: a `ServiceHost` built as the daemon builds one, with
//! its listener on loopback TCP or on a unix socket.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(unix)]
use std::sync::atomic::{AtomicU64, Ordering};

use daedalus_backend::Backend;
use daedalus_host::listen::{Listener, PluginLoader, ServiceHost, WireCodecs};
use daedalus_loader::{Loader, PluginBackend};
use daedalus_registry::{RegError, Registry};
use daedalus_wire::service::PluginInfo;

/// Which transport a scenario runs over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport {
    /// Loopback TCP. Every platform has it.
    Tcp,
    /// A unix socket, where the platform has them.
    #[cfg(unix)]
    Unix,
}

impl Transport {
    /// Every transport a driver is run over.
    pub fn all() -> Vec<Transport> {
        #[cfg(unix)]
        {
            vec![Transport::Tcp, Transport::Unix]
        }
        #[cfg(not(unix))]
        {
            vec![Transport::Tcp]
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Transport::Tcp => "tcp",
            #[cfg(unix)]
            Transport::Unix => "unix",
        }
    }
}

/// Loads plugins into the host, as the daemon's loader does: each node joins the registry, and
/// each backend of the library is wrapped and added.
pub struct Plugins {
    loader: Loader,
}

impl PluginLoader for Plugins {
    fn load(
        &mut self,
        path: &str,
        registry: &mut Registry,
        backends: &mut Vec<Arc<dyn Backend>>,
    ) -> Result<PluginInfo, String> {
        let plugin = self.loader.open(path).map_err(|error| error.to_string())?;
        for decl in plugin.nodes().iter().chain(plugin.conversions()) {
            match registry.register_owned(decl.clone()) {
                Ok(()) | Err(RegError::DuplicateBody { .. }) => {}
                Err(error) => return Err(format!("{path}: node `{}`: {error}", decl.id)),
            }
        }
        let info = PluginInfo {
            name: plugin.name().to_owned(),
            generation: u32::try_from(plugin.generation().number()).unwrap_or(u32::MAX),
            nodes: plugin.nodes().iter().map(|node| node.id.clone()).collect(),
            backends: plugin
                .backends()
                .iter()
                .map(|backend| backend.id().to_owned())
                .collect(),
        };
        let plugin = Arc::new(plugin);
        for index in 0..plugin.backends().len() {
            let wrapped: Arc<dyn Backend> =
                Arc::new(PluginBackend::new(Arc::clone(&plugin), index));
            backends.retain(|known| known.id() != wrapped.id());
            backends.push(wrapped);
        }
        Ok(info)
    }
}

/// A host with its listener. `shutdown` ends the host, and the listener with it.
pub struct Serving {
    host: Arc<ServiceHost>,
    listener: Option<Listener>,
    /// Where the listener is, as the transport's address: `tcp:ADDR` or `unix:PATH`.
    pub address: String,
    /// The socket path to remove once the host is down, for a unix transport.
    socket: Option<PathBuf>,
}

impl Serving {
    /// A host with the plugin loader when `plugins` is set, listening on `transport`.
    pub fn start(transport: Transport, plugins: bool) -> Result<Serving, String> {
        let mut builder = ServiceHost::builder(Registry::new()).codecs(WireCodecs::byte_form());
        if plugins {
            builder = builder.plugins(Box::new(Plugins {
                loader: Loader::new(),
            }));
        }
        let host = builder.build();
        match transport {
            Transport::Tcp => {
                let (listener, addr) = host
                    .listen_tcp(SocketAddr::from(([127, 0, 0, 1], 0)))
                    .map_err(|error| format!("listen on loopback: {error}"))?;
                Ok(Serving {
                    host,
                    listener: Some(listener),
                    address: format!("tcp:{addr}"),
                    socket: None,
                })
            }
            #[cfg(unix)]
            Transport::Unix => {
                static NEXT: AtomicU64 = AtomicU64::new(0);
                let path = std::env::temp_dir().join(format!(
                    "dd-drive-{}-{}.sock",
                    std::process::id(),
                    NEXT.fetch_add(1, Ordering::Relaxed)
                ));
                let _ = std::fs::remove_file(&path);
                let listener = host
                    .listen_unix(&path)
                    .map_err(|error| format!("listen on {}: {error}", path.display()))?;
                Ok(Serving {
                    host,
                    listener: Some(listener),
                    address: format!("unix:{}", path.display()),
                    socket: Some(path),
                })
            }
        }
    }

    /// Ends the host and waits for its accept loop to end.
    pub fn finish(mut self) {
        self.host.shutdown();
        if let Some(listener) = self.listener.take() {
            listener.join();
        }
        if let Some(path) = &self.socket {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Where a host's listener is, for a proxy to reach.
pub fn tcp_addr(address: &str) -> Option<SocketAddr> {
    address.strip_prefix("tcp:")?.parse().ok()
}

/// The fixture library's path under `dir`, after `build_fixture` built it there.
pub fn library_under(dir: &Path) -> PathBuf {
    dir.join("debug").join(format!(
        "{}daedalus_dylib_fixture{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    ))
}

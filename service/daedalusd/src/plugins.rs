//! Plugins: libraries opened through the loader. Each backend a plugin brings is wrapped so the
//! planner can use it, and a library that fails to load is tried again, with a backoff capped at
//! 8 s, for as long as its file is there. A plugin never stops the daemon.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use daedalus_backend::Backend;
use daedalus_host::listen::{PluginLoader, ServiceHost};
use daedalus_loader::{Generation, Loader, PluginBackend, Retry};
use daedalus_registry::{RegError, Registry};
use daedalus_wire::service::PluginInfo;

use crate::logging::{debug, info, warning};

/// The generations opened so far, for checking at shutdown that each one was retired.
pub(crate) type Probes = Arc<Mutex<Vec<(String, Weak<Generation>)>>>;

/// Loads plugins for the host.
pub struct Plugins {
    loader: Loader,
    probes: Probes,
}

impl Plugins {
    pub fn new(probes: Probes) -> Plugins {
        Plugins {
            loader: Loader::new(),
            probes,
        }
    }
}

impl PluginLoader for Plugins {
    fn load(
        &mut self,
        path: &str,
        registry: &mut Registry,
        backends: &mut Vec<Arc<dyn Backend>>,
    ) -> Result<PluginInfo, String> {
        let plugin = self.loader.open(path).map_err(|error| error.to_string())?;
        // A node this plugin's earlier generation already registered is the same node: its body
        // now runs from the new library. Anything else that does not join is an error.
        for decl in plugin.nodes() {
            match registry.register_owned(decl.clone()) {
                Ok(()) | Err(RegError::DuplicateBody { .. }) => {}
                Err(error) => return Err(format!("{path}: node `{}`: {error}", decl.id)),
            }
        }
        let plugin = Arc::new(plugin);
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
        for index in 0..plugin.backends().len() {
            let wrapped: Arc<dyn Backend> =
                Arc::new(PluginBackend::new(Arc::clone(&plugin), index));
            // The new generation's backend replaces the old one of the same id. The old library
            // stays open until the last step compiled from it is released.
            backends.retain(|known| known.id() != wrapped.id());
            backends.push(wrapped);
        }
        self.probes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push((
                format!("{} generation {}", info.name, info.generation),
                plugin.generation_probe(),
            ));
        Ok(info)
    }
}

/// The libraries in `dir`, in name order. A directory that cannot be read has none.
pub fn scan(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        warning!("plugin directory {} cannot be read", dir.display());
        return Vec::new();
    };
    let mut found: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(std::env::consts::DLL_SUFFIX))
        })
        .collect();
    found.sort();
    found
}

/// Loads every library in `paths` once. The ones that failed are returned, to be tried again.
pub(crate) fn load_all(host: &ServiceHost, paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut failed = Vec::new();
    for path in paths {
        match host.load_plugin(&path.to_string_lossy()) {
            Ok(plugin) => info!(
                "loaded plugin {} generation {} from {}: {} nodes, {} backends",
                plugin.name,
                plugin.generation,
                path.display(),
                plugin.nodes.len(),
                plugin.backends.len()
            ),
            Err(error) => {
                warning!("plugin {} did not load: {}", path.display(), error.message);
                failed.push(path);
            }
        }
    }
    failed
}

/// How often the retry thread looks at the stop flag while it waits.
const RETRY_POLL: Duration = Duration::from_millis(50);

/// Keeps trying the libraries that failed to load. Each has its own backoff, capped at 8 s. A
/// library is given up on only when its file is removed, which is how an operator stops the
/// retries by hand, or when the daemon stops.
pub(crate) fn retry_in_background(
    host: Arc<ServiceHost>,
    failed: Vec<PathBuf>,
    stop: Arc<AtomicBool>,
) -> Option<JoinHandle<()>> {
    if failed.is_empty() {
        return None;
    }
    let body = move || {
        let now = Instant::now();
        let mut waiting: BTreeMap<PathBuf, (Retry, Instant)> = failed
            .into_iter()
            .map(|path| {
                let mut retry = Retry::new();
                let due = now + retry.failed();
                (path, (retry, due))
            })
            .collect();
        while !waiting.is_empty() && !stop.load(Ordering::SeqCst) {
            std::thread::sleep(RETRY_POLL);
            let now = Instant::now();
            let due: Vec<PathBuf> = waiting
                .iter()
                .filter(|(_, (_, due))| *due <= now)
                .map(|(path, _)| path.clone())
                .collect();
            for path in due {
                if !path.exists() {
                    info!("plugin {} was removed: no longer retried", path.display());
                    waiting.remove(&path);
                    continue;
                }
                match host.load_plugin(&path.to_string_lossy()) {
                    Ok(plugin) => {
                        info!(
                            "loaded plugin {} from {} after retrying",
                            plugin.name,
                            path.display()
                        );
                        waiting.remove(&path);
                    }
                    Err(error) => {
                        if let Some((retry, due)) = waiting.get_mut(&path) {
                            let delay = retry.failed();
                            *due = Instant::now() + delay;
                            debug!(
                                "plugin {} still does not load ({}), next try in {:?}",
                                path.display(),
                                error.message,
                                delay
                            );
                        }
                    }
                }
            }
        }
    };
    std::thread::Builder::new()
        .name("daedalusd-plugins".to_owned())
        .spawn(body)
        .ok()
}

/// The generations that are still open: a step compiled from each is still held somewhere.
pub fn still_open(probes: &Probes) -> Vec<String> {
    probes
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .filter(|(_, probe)| probe.upgrade().is_some())
        .map(|(name, _)| name.clone())
        .collect()
}

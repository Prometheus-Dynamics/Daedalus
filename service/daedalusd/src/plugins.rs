//! Plugins: libraries opened through the loader. Each backend a plugin brings is wrapped so the
//! planner can use it, and a library that fails to load is tried again, with a backoff capped at
//! 8 s, for as long as its file is there. A plugin never stops the daemon.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::Instant;

use daedalus_backend::Backend;
use daedalus_host::listen::{PluginLoader, ServiceHost};
use daedalus_integrity::Integrity;
use daedalus_loader::{Generation, LoadError, Loader, PluginBackend, Retry};
use daedalus_registry::{RegError, Registry};
use daedalus_wire::service::PluginInfo;

use crate::logging::{debug, info, warning};

/// The generations opened so far, for checking at shutdown that each one was retired.
pub(crate) type Probes = Arc<Mutex<Vec<(String, Weak<Generation>)>>>;

/// What the plugins tell the retry thread.
pub(crate) enum Note {
    /// A library the integrity check refused. It is tried again, as a library that failed at start
    /// is, until its file is removed.
    Refused { path: PathBuf, reason: String },
    /// A library loaded, by a request or a retry. Nothing is tried again for it.
    Loaded(PathBuf),
    /// The daemon is stopping, and the retry thread ends.
    Stop,
}

/// Loads plugins for the host.
pub struct Plugins {
    loader: Loader,
    probes: Probes,
    notes: Sender<Note>,
}

impl Plugins {
    pub fn new(probes: Probes, integrity: Integrity, notes: Sender<Note>) -> Plugins {
        Plugins {
            loader: Loader::with_integrity(integrity),
            probes,
            notes,
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
        let plugin = match self.loader.open(path) {
            Ok(plugin) => plugin,
            Err(error) => {
                if matches!(error, LoadError::Integrity(_)) {
                    let _ = self.notes.send(Note::Refused {
                        path: PathBuf::from(path),
                        reason: error.to_string(),
                    });
                }
                return Err(error.to_string());
            }
        };
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
        let _ = self.notes.send(Note::Loaded(PathBuf::from(path)));
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

/// A library waiting to be tried again.
struct Waiting {
    retry: Retry,
    due: Instant,
    /// The last reason it did not load. A repeat of it is logged at debug, a new one at warning.
    reason: String,
}

/// Keeps trying the libraries that failed to load. Each has its own backoff, capped at 8 s. A
/// library is given up on only when its file is removed, which is how an operator stops the
/// retries by hand, or when the daemon stops. The thread waits on `notes` between tries, so it
/// costs nothing while there is nothing to try.
pub(crate) fn retry_in_background(
    host: Arc<ServiceHost>,
    failed: Vec<PathBuf>,
    notes: Receiver<Note>,
) -> Option<JoinHandle<()>> {
    let body = move || {
        let mut waiting: BTreeMap<PathBuf, Waiting> = BTreeMap::new();
        for path in failed {
            let mut retry = Retry::new();
            let due = Instant::now() + retry.failed();
            waiting.insert(
                path,
                Waiting {
                    retry,
                    due,
                    reason: String::new(),
                },
            );
        }
        loop {
            // Wait for the next due try, or for a note, whichever is first.
            let note = match waiting.values().map(|waiting| waiting.due).min() {
                None => match notes.recv() {
                    Ok(note) => Some(note),
                    Err(_) => return,
                },
                Some(due) => {
                    match notes.recv_timeout(due.saturating_duration_since(Instant::now())) {
                        Ok(note) => Some(note),
                        Err(RecvTimeoutError::Timeout) => None,
                        Err(RecvTimeoutError::Disconnected) => return,
                    }
                }
            };
            match note {
                Some(Note::Stop) => return,
                Some(Note::Loaded(path)) => {
                    waiting.remove(&path);
                }
                // A refusal the thread already waits on is its own failed try coming back.
                Some(Note::Refused { path, reason }) if !waiting.contains_key(&path) => {
                    let mut retry = Retry::new();
                    let delay = retry.failed();
                    warning!(
                        "plugin {} is refused: {reason}; what is loaded keeps running; next try in {delay:?}",
                        path.display()
                    );
                    waiting.insert(
                        path,
                        Waiting {
                            retry,
                            due: Instant::now() + delay,
                            reason,
                        },
                    );
                }
                None | Some(Note::Refused { .. }) => {}
            }
            let now = Instant::now();
            let due: Vec<PathBuf> = waiting
                .iter()
                .filter(|(_, waiting)| waiting.due <= now)
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
                            "loaded plugin {} generation {} from {} after retrying",
                            plugin.name,
                            plugin.generation,
                            path.display()
                        );
                        waiting.remove(&path);
                    }
                    Err(error) => {
                        if let Some(waiting) = waiting.get_mut(&path) {
                            let delay = waiting.retry.failed();
                            waiting.due = Instant::now() + delay;
                            if waiting.reason == error.message {
                                debug!(
                                    "plugin {} still does not load ({}), next try in {:?}",
                                    path.display(),
                                    error.message,
                                    delay
                                );
                            } else {
                                warning!(
                                    "plugin {} is refused: {}; what is loaded keeps running; next try in {:?}",
                                    path.display(),
                                    error.message,
                                    delay
                                );
                            }
                            waiting.reason = error.message;
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

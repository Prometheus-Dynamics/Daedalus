//! Plugins: libraries opened through the loader. Each backend a plugin brings is wrapped so the
//! planner can use it. A library that fails to load is tried again by the retry thread, and a
//! plugin never stops the daemon.

use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex, Weak};

use daedalus_backend::Backend;
use daedalus_host::listen::{PluginLoader, ServiceHost};
use daedalus_integrity::Integrity;
use daedalus_loader::{Generation, LoadError, Loader, PluginBackend};
use daedalus_registry::{RegError, Registry};
use daedalus_wire::service::PluginInfo;

use crate::logging::{info, warning};
use crate::retry::{Attempt, Note};

/// The generations opened so far, for checking at shutdown that each one was retired.
pub(crate) type Probes = Arc<Mutex<Vec<(String, Weak<Generation>)>>>;

/// Loads plugins for the host.
pub struct Plugins {
    loader: Loader,
    probes: Probes,
    notes: Sender<Note>,
    /// The workers, when the daemon runs them: their state is listed beside the plugins.
    #[cfg(feature = "workers")]
    workers: Option<Arc<crate::workers::Workers>>,
}

impl Plugins {
    pub fn new(probes: Probes, integrity: Integrity, notes: Sender<Note>) -> Plugins {
        Plugins {
            loader: Loader::with_integrity(integrity),
            probes,
            notes,
            #[cfg(feature = "workers")]
            workers: None,
        }
    }

    /// Lists the workers in `workers` beside the plugins, and answers for them.
    #[cfg(feature = "workers")]
    pub fn with_workers(mut self, workers: Arc<crate::workers::Workers>) -> Plugins {
        self.workers = Some(workers);
        self
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
                        attempt: Box::new(PluginAttempt {
                            path: PathBuf::from(path),
                        }),
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
        let _ = self.notes.send(Note::Loaded(path.to_owned()));
        Ok(info)
    }

    fn listing(&self) -> Vec<String> {
        #[cfg(feature = "workers")]
        if let Some(workers) = &self.workers {
            return workers.listing();
        }
        Vec::new()
    }

    fn not_running(&self) -> Vec<String> {
        #[cfg(feature = "workers")]
        if let Some(workers) = &self.workers {
            return workers.not_running();
        }
        Vec::new()
    }

    fn missing_backend(&self, backend: &str) -> Option<String> {
        #[cfg(not(feature = "workers"))]
        if backend == "process" {
            return Some(
                "the daemon was built without `workers`, so it runs no worker programs".to_owned(),
            );
        }
        let _ = backend;
        None
    }
}

/// The scan of a plugin directory: the libraries in it, in name order. A directory that cannot be
/// read has none.
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

/// A library the retry thread tries again: a plugin that failed to load, or was refused.
pub(crate) struct PluginAttempt {
    pub(crate) path: PathBuf,
}

impl Attempt for PluginAttempt {
    fn kind(&self) -> &'static str {
        "plugin"
    }

    fn name(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }

    fn gone(&self) -> bool {
        // A removed file is how an operator stops the retries for a plugin by hand.
        !self.path.exists()
    }

    fn retry(&mut self, host: &ServiceHost) -> Result<String, String> {
        match host.load_plugin(&self.name()) {
            Ok(plugin) => Ok(format!(
                "loaded plugin {} generation {} from {} after retrying",
                plugin.name,
                plugin.generation,
                self.path.display()
            )),
            Err(error) => Err(error.message),
        }
    }
}

/// Loads every library in `paths` once. The ones that failed are returned, to be tried again.
pub(crate) fn load_all(host: &ServiceHost, paths: Vec<PathBuf>) -> Vec<Box<dyn Attempt>> {
    let mut failed: Vec<Box<dyn Attempt>> = Vec::new();
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
                failed.push(Box::new(PluginAttempt { path }));
            }
        }
    }
    failed
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

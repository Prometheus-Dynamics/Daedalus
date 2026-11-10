//! A host: plugins in, graphs out.

use std::sync::Arc;

use daedalus_capi_sys::{Api, Handle};

use crate::error::HostError;
use crate::graph::Graph;

/// The host's handle, released when the host and every graph compiled on it are gone. A
/// graph keeps this, since the library won't release a host that still has graphs.
pub(crate) struct Held {
    pub(crate) api: Arc<Api>,
    pub(crate) handle: Handle,
}

impl Drop for Held {
    fn drop(&mut self) {
        // The last holder: no graph is left, so this can't be busy. A failure has nobody to
        // go to, and the library keeps the host.
        let _ = self.api.host_release(self.handle);
    }
}

/// A host. It has no nodes until a plugin is installed.
pub struct Host {
    held: Arc<Held>,
}

impl std::fmt::Debug for Host {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Host")
            .field("handle", &self.held.handle)
            .field("graphs", &self.graphs())
            .finish()
    }
}

impl Host {
    pub(crate) fn create(api: Arc<Api>) -> Result<Host, HostError> {
        let handle = api
            .host_create()
            .map_err(|code| HostError::from_code(&api, code))?;
        Ok(Host {
            held: Arc::new(Held { api, handle }),
        })
    }

    fn fail(&self, code: i32) -> HostError {
        HostError::from_code(&self.held.api, code)
    }

    /// Loads the plugin at `path`: its nodes join this host's, and its backends run them.
    pub fn install(&self, path: &str) -> Result<(), HostError> {
        self.held
            .api
            .host_install(self.held.handle, path)
            .map_err(|code| self.fail(code))
    }

    /// The host's nodes, as JSON: each node's id, its ports with their types, and the
    /// backends that run it.
    pub fn schemas(&self) -> Result<String, HostError> {
        self.held
            .api
            .host_schemas(self.held.handle)
            .map_err(|code| self.fail(code))
    }

    /// Compiles a graph, given as JSON in the graph format. It is proved against the host's
    /// nodes before anything runs.
    pub fn compile(&self, json: &str) -> Result<Graph, HostError> {
        let handle = self
            .held
            .api
            .graph_compile(self.held.handle, json)
            .map_err(|code| self.fail(code))?;
        Ok(Graph::new(Arc::clone(&self.held), handle))
    }

    /// How many graphs compiled on this host are still alive.
    pub fn graphs(&self) -> usize {
        // One reference is this host's own.
        Arc::strong_count(&self.held) - 1
    }

    /// Releases the host now, and says so. With graphs alive it is [`HostError::Busy`] and
    /// the host comes back. Dropping a host instead lets it go when its last graph does.
    pub fn release(self) -> Result<(), (Host, HostError)> {
        match self.held.api.host_release(self.held.handle) {
            // Released: the drop that follows finds a stale handle, which is ignored.
            Ok(()) => Ok(()),
            Err(code) => {
                let error = self.fail(code);
                Err((self, error))
            }
        }
    }
}

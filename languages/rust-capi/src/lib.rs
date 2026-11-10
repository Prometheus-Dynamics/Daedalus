//! A safe Rust host over the C surface.
//!
//! The surface is a shared library with a C header. This crate loads it and gives it back as
//! ordinary Rust: a [`Surface`] is the loaded library, a [`Host`] holds plugins, a [`Graph`]
//! is compiled on a host and pushed to, ticked and taken from. Handles are owned: dropping a
//! graph or a host releases it. Every status the library returns is a [`HostError`].
//!
//! There is no `unsafe` here. The calls themselves are made by `daedalus-capi-sys`.

#![forbid(unsafe_code)]

mod error;
mod graph;
mod host;
mod ports;

use std::path::Path;
use std::sync::Arc;

use daedalus_capi_sys::Api;

pub use daedalus_capi_sys::{Port, Telemetry, Tick, Version};
pub use error::HostError;
pub use graph::Graph;
pub use host::Host;
pub use ports::{PortIndex, PortValue, Pushed, ToPort};

/// The loaded library, checked to be the surface this crate was built for.
#[derive(Clone)]
pub struct Surface {
    api: Arc<Api>,
    version: Version,
}

impl Surface {
    /// Loads the shared library at `path`. It is refused unless its major version and its
    /// layout hash are the ones this crate was built against: a library with another layout
    /// passes values this crate would misread.
    pub fn load(path: impl AsRef<Path>) -> Result<Surface, HostError> {
        let abi = daedalus_abi::DAEDALUS_ABI;
        Surface::load_expecting(path, u32::from(abi.major), abi.layout)
    }

    /// [`Surface::load`] against a version given by the caller.
    pub fn load_expecting(
        path: impl AsRef<Path>,
        major: u32,
        layout_hash: u64,
    ) -> Result<Surface, HostError> {
        let api = Api::load(path).map_err(|error| HostError::Library(error.to_string()))?;
        let version = api
            .version()
            .map_err(|code| HostError::from_code(&api, code))?;
        if version.major != major || version.layout_hash != layout_hash {
            return Err(HostError::Version {
                found: version,
                wanted_major: major,
                wanted_layout_hash: layout_hash,
            });
        }
        Ok(Surface {
            api: Arc::new(api),
            version,
        })
    }

    /// The library's version.
    pub fn version(&self) -> Version {
        self.version
    }

    /// A new host, with no nodes until a plugin is installed.
    pub fn host(&self) -> Result<Host, HostError> {
        Host::create(Arc::clone(&self.api))
    }

    /// Makes the library panic inside a call. It comes back as [`HostError::Panic`], which is
    /// what a panic in any call does.
    pub fn selftest_panic(&self) -> HostError {
        HostError::from_code(&self.api, self.api.selftest_panic())
    }
}

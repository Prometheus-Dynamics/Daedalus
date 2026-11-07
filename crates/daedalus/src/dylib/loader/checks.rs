//! The stable ABI's checks at load: the `u32` version symbol against the descriptor, and the
//! optional handshake (`daedalus_plugin_stable_abi`) against this host's when a plugin exports one.

use std::path::Path;

use super::PluginLibraryError;
use crate::dylib::stable::{STABLE_HANDSHAKE, StableHandshake};

/// A plugin that exports the stable handshake must have it accepted by the host (same major and
/// layout hash, see `lemnos_abi::AbiVersion::accepts`). A plugin without one (every layout the
/// loader read before the handshake existed) is not checked and installs as it always has.
pub(super) fn check_stable_handshake(
    path: &Path,
    handshake: Option<StableHandshake>,
    version: u32,
) -> Result<(), PluginLibraryError> {
    let host = STABLE_HANDSHAKE;
    let reason = match handshake {
        None => return Ok(()),
        Some(found) if host.accepts(&found) == lemnos_abi::Status::OK => return Ok(()),
        Some(found) => format!(
            "its handshake (major {}, minor {}, layout {:#018x}) is not the host's (major {}, \
             minor {}, layout {:#018x})",
            found.major, found.minor, found.layout, host.major, host.minor, host.layout
        ),
    };
    Err(PluginLibraryError::StableHandshake {
        path: path.to_path_buf(),
        version,
        reason,
    })
}

/// A plugin's `daedalus_plugin_stable_version` (when it exports one) must equal its
/// descriptor's stable version.
pub(super) fn check_stable_version_symbol(
    path: &Path,
    symbol: Option<u32>,
    descriptor: u32,
) -> Result<(), PluginLibraryError> {
    match symbol {
        Some(symbol) if symbol != descriptor => Err(PluginLibraryError::StableVersionSymbol {
            path: path.to_path_buf(),
            symbol,
            descriptor,
        }),
        _ => Ok(()),
    }
}

//! `examples/plugins/node_groups` through the stable handler path: the cdylib's descriptor claims
//! another rustc, so a host refuses its Rust ABI and installs it through the stable path (as
//! `examples/plugins/stable_abi` does). The group is carried in the stable schema's node metadata.

/// The rustc the exported descriptor claims.
pub const SIMULATED_RUSTC: &str = "rustc 1.85.0 (4d91de4e4 2025-02-17; simulated)";

mod export {
    use daedalus::dylib::{PLUGIN_ABI_VERSION, PluginDescriptor, StableExtension, StrView};

    /// Returns the Daedalus dynamic plugin ABI version.
    #[unsafe(no_mangle)]
    pub extern "C" fn daedalus_plugin_abi_version() -> u32 {
        PLUGIN_ABI_VERSION
    }

    /// The plugin's descriptor, claiming another toolchain.
    #[unsafe(no_mangle)]
    pub extern "C" fn daedalus_plugin_descriptor() -> PluginDescriptor {
        let mut descriptor =
            daedalus::plugin_descriptor!(daedalus_plugins_node_groups::NodeGroupsPlugin);
        descriptor.info.rustc_version = StrView::from_static(super::SIMULATED_RUSTC);
        descriptor
    }

    /// The stable extension (const inputs sent once, see `StableExtension`).
    #[unsafe(no_mangle)]
    pub extern "C" fn daedalus_plugin_stable_extension() -> StableExtension {
        daedalus::dylib::stable_extension()
    }

    /// The codec table (see `StableCodecTable`).
    #[unsafe(no_mangle)]
    pub extern "C" fn daedalus_plugin_stable_codecs() -> *const daedalus::dylib::StableCodecTable {
        daedalus::plugin_codec_table!(daedalus_plugins_node_groups::NodeGroupsPlugin)
    }

    /// The stable ABI version, as data (see `STABLE_VERSION_SYMBOL`).
    #[unsafe(no_mangle)]
    #[allow(non_upper_case_globals)]
    pub static daedalus_plugin_stable_version: u32 = daedalus::dylib::STABLE_ABI_VERSION;

    /// The stable handshake (`STABLE_HANDSHAKE`, optional), as data: the layout hash the host checks.
    #[unsafe(no_mangle)]
    #[allow(non_upper_case_globals)]
    pub static daedalus_plugin_stable_abi: daedalus::dylib::StableHandshake =
        daedalus::dylib::STABLE_HANDSHAKE;
}

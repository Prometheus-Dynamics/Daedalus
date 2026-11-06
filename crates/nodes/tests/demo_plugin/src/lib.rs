//! The `daedalus-nodes` demo bundle (`demo:frame_src`, `demo:decode`, `demo:sink`) as a dynamic
//! plugin whose descriptor claims another rustc, so a host refuses its Rust ABI and installs it
//! through the stable entry points: its `Frame` crosses as a `Value` and its `Vec<Detection>` as
//! a `StablePod` view (the facade's `dylib_stable_demo` test).

#[cfg(feature = "dylib")]
mod export {
    use daedalus::dylib::{
        PLUGIN_ABI_VERSION, PluginDescriptor, StableCodecTable, StableExtension, StrView,
    };
    use daedalus_nodes::bundle_demo::DemoPlugin;

    /// The rustc the exported descriptor claims.
    const SIMULATED_RUSTC: &str = "rustc 1.85.0 (4d91de4e4 2025-02-17; simulated)";

    /// Returns the Daedalus dynamic plugin ABI version.
    #[unsafe(no_mangle)]
    pub extern "C" fn daedalus_plugin_abi_version() -> u32 {
        PLUGIN_ABI_VERSION
    }

    /// The plugin's descriptor, claiming another toolchain.
    #[unsafe(no_mangle)]
    pub extern "C" fn daedalus_plugin_descriptor() -> PluginDescriptor {
        let mut descriptor = daedalus::plugin_descriptor!(DemoPlugin);
        descriptor.info.rustc_version = StrView::from_static(SIMULATED_RUSTC);
        descriptor
    }

    /// The stable extension.
    #[unsafe(no_mangle)]
    pub extern "C" fn daedalus_plugin_stable_extension() -> StableExtension {
        daedalus::dylib::stable_extension()
    }

    /// The codec table (empty: the demo types cross without codecs).
    #[unsafe(no_mangle)]
    pub extern "C" fn daedalus_plugin_stable_codecs() -> *const StableCodecTable {
        daedalus::plugin_codec_table!(DemoPlugin)
    }

    /// The stable ABI version, as data.
    #[unsafe(no_mangle)]
    #[allow(non_upper_case_globals)]
    pub static daedalus_plugin_stable_version: u32 = daedalus::dylib::STABLE_ABI_VERSION;
}

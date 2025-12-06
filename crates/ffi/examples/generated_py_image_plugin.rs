#![crate_type = "cdylib"]
use daedalus_ffi::export_plugin;
use daedalus_ffi::{PythonManifest, PythonManifestPlugin};
use daedalus_runtime::plugins::{Plugin, PluginRegistry};

static MANIFEST_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/lang/python/examples/generated_py_image_plugin/manifest.json"
));
static BASE_DIR: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/lang/python/examples/generated_py_image_plugin"
);

pub struct GeneratedPyPlugin {
    inner: PythonManifestPlugin,
}

impl Default for GeneratedPyPlugin {
    fn default() -> Self {
        let manifest: PythonManifest =
            serde_json::from_str(MANIFEST_JSON).expect("invalid embedded manifest");
        let base = std::path::PathBuf::from(BASE_DIR);
        Self {
            inner: PythonManifestPlugin::from_manifest_with_base(manifest, Some(base)),
        }
    }
}

impl Plugin for GeneratedPyPlugin {
    fn id(&self) -> &'static str {
        self.inner.id()
    }

    fn install(&self, registry: &mut PluginRegistry) -> Result<(), &'static str> {
        self.inner.install(registry)
    }
}

export_plugin!(GeneratedPyPlugin);

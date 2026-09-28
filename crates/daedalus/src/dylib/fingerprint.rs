//! Build fingerprint shared by dynamic plugin hosts and plugins.
//!
//! The fingerprint is a `;`-separated list of `key=value` segments:
//!
//! - `target`, `pointer_width`: the compilation target.
//! - `features.<crate>`: the enabled, boundary-relevant Cargo features of every Daedalus crate
//!   whose types cross the plugin boundary (comma-separated, declaration order).
//! - `features.hash`: a stable FNV-1a hash of all `features.*` segments.
//! - `layout.<type>`: `size/align` of the Rust types `PluginRegistry` installation touches.
//!
//! Features that only change host-side execution (the engine, the executor worker pool,
//! metrics collection, the dylib loader itself) are excluded, so a plugin built with just
//! `plugins` loads into a host built with `engine-full,plugins,dylib-plugins`.

use std::fmt::Write as _;
use std::mem::{align_of, size_of};
use std::sync::OnceLock;

/// Per-crate enabled feature lists that feed the fingerprint.
fn crate_features() -> [(&'static str, &'static [&'static str]); 6] {
    [
        ("daedalus", crate::ENABLED_FEATURES),
        ("core", daedalus_core::ENABLED_FEATURES),
        ("data", daedalus_data::ENABLED_FEATURES),
        ("registry", daedalus_registry::ENABLED_FEATURES),
        ("planner", daedalus_planner::ENABLED_FEATURES),
        ("runtime", daedalus_runtime::ENABLED_FEATURES),
    ]
}

/// Features that do not change any type crossing the plugin boundary.
///
/// They only affect how the host plans and executes graphs; plugins normally do not enable
/// them. `daedalus-transport` has no features and `daedalus-engine` never crosses the boundary.
pub const HOST_ONLY_FEATURES: &[(&str, &str)] = &[
    ("daedalus", "engine"),
    ("daedalus", "engine-full"),
    ("daedalus", "embedded"),
    ("daedalus", "executor-pool"),
    ("daedalus", "metrics"),
    ("daedalus", "gpu-engine"),
    ("daedalus", "gpu"),
    ("daedalus", "dylib-plugins"),
    ("daedalus", "gpu-dmabuf"),
    ("daedalus", "examples"),
    ("daedalus", "styx-camera-example"),
    ("runtime", "executor-pool"),
    ("runtime", "lockfree-queues"),
    ("runtime", "metrics"),
    ("runtime", "snapshots"),
];

fn is_host_only(krate: &str, feature: &str) -> bool {
    HOST_ONLY_FEATURES
        .iter()
        .any(|(c, f)| *c == krate && *f == feature)
}

/// Boundary-relevant enabled features per crate, in fingerprint order.
pub fn boundary_features() -> Vec<(&'static str, Vec<&'static str>)> {
    crate_features()
        .into_iter()
        .map(|(krate, features)| {
            let kept = features
                .iter()
                .copied()
                .filter(|feature| !is_host_only(krate, feature))
                .collect();
            (krate, kept)
        })
        .collect()
}

/// 64-bit FNV-1a: tiny, dependency-free, and stable across toolchains and platforms.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn layouts() -> [(&'static str, usize, usize); 10] {
    macro_rules! layout {
        ($name:literal, $ty:ty) => {
            ($name, size_of::<$ty>(), align_of::<$ty>())
        };
    }
    [
        layout!("plugin_registry", crate::runtime::plugins::PluginRegistry),
        layout!(
            "handler_registry",
            daedalus_runtime::handler_registry::HandlerRegistry
        ),
        layout!("payload", daedalus_transport::Payload),
        layout!("type_key", daedalus_transport::TypeKey),
        layout!(
            "boundary_type_contract",
            daedalus_transport::BoundaryTypeContract
        ),
        layout!("type_expr", daedalus_data::model::TypeExpr),
        layout!("node_decl", daedalus_registry::capability::NodeDecl),
        layout!("adapter_decl", daedalus_registry::capability::AdapterDecl),
        layout!(
            "plugin_manifest",
            daedalus_registry::capability::PluginManifest
        ),
        layout!("str_view", super::StrView),
    ]
}

fn compute() -> String {
    let mut features = String::new();
    for (krate, enabled) in boundary_features() {
        let _ = write!(features, "features.{krate}={};", enabled.join(","));
    }
    let mut out = format!(
        "target={};pointer_width={};{features}features.hash={:016x}",
        env!("DAEDALUS_BUILD_TARGET"),
        usize::BITS,
        fnv1a64(features.as_bytes()),
    );
    for (name, size, align) in layouts() {
        let _ = write!(out, ";layout.{name}={size}/{align}");
    }
    out
}

/// Fingerprint of layout-affecting build properties of this copy of Daedalus.
///
/// Hosts and plugins must produce identical fingerprints. See the module docs of
/// [`crate::dylib`] for what it covers.
pub fn build_fingerprint() -> &'static str {
    static FINGERPRINT: OnceLock<String> = OnceLock::new();
    FINGERPRINT.get_or_init(compute)
}

/// Describe which fingerprint segments differ between `host` and `plugin`, e.g.
/// ``features.runtime: host `gpu,plugins`, plugin `plugins` ``.
///
/// Segments missing on one side are shown as `<missing>`. Returns an empty string when the
/// fingerprints are identical.
pub fn describe_fingerprint_mismatch(host: &str, plugin: &str) -> String {
    fn segments(fingerprint: &str) -> Vec<(&str, &str)> {
        fingerprint
            .split(';')
            .filter(|segment| !segment.is_empty())
            .map(|segment| segment.split_once('=').unwrap_or((segment, "")))
            .collect()
    }
    let host_segments = segments(host);
    let plugin_segments = segments(plugin);
    let lookup = |list: &[(&str, &str)], key: &str| {
        list.iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| format!("`{v}`"))
            .unwrap_or_else(|| "<missing>".to_string())
    };
    let mut keys: Vec<&str> = host_segments.iter().map(|(k, _)| *k).collect();
    for (key, _) in &plugin_segments {
        if !keys.contains(key) {
            keys.push(key);
        }
    }
    keys.into_iter()
        .filter_map(|key| {
            let h = lookup(&host_segments, key);
            let p = lookup(&plugin_segments, key);
            (h != p).then(|| format!("{key}: host {h}, plugin {p}"))
        })
        .collect::<Vec<_>>()
        .join("; ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn fingerprint_is_deterministic_and_readable() {
        let fingerprint = build_fingerprint();
        assert_eq!(fingerprint, compute());
        assert!(fingerprint.starts_with(&format!(
            "target={};pointer_width={}",
            env!("DAEDALUS_BUILD_TARGET"),
            usize::BITS
        )));
        for krate in ["daedalus", "core", "data", "registry", "planner", "runtime"] {
            assert!(
                fingerprint.contains(&format!(";features.{krate}=")),
                "{krate} missing from {fingerprint}"
            );
        }
        assert!(fingerprint.contains(";features.hash="));
        assert!(fingerprint.contains(&format!(
            ";layout.plugin_registry={}/{}",
            size_of::<crate::PluginRegistry>(),
            align_of::<crate::PluginRegistry>()
        )));
        for name in ["payload", "type_key", "node_decl", "type_expr"] {
            assert!(fingerprint.contains(&format!(";layout.{name}=")));
        }
    }

    #[test]
    fn fingerprint_reflects_boundary_features() {
        let features = |name: &str| {
            boundary_features()
                .into_iter()
                .find(|(krate, _)| *krate == name)
                .unwrap()
                .1
        };
        // This module only exists with `plugins`, which forwards to these crate features.
        assert!(features("daedalus").contains(&"plugins"));
        assert!(features("registry").contains(&"plugin"));
        let runtime = features("runtime");
        assert!(runtime.contains(&"plugins"));
        assert!(build_fingerprint().contains(&format!("features.runtime={};", runtime.join(","))));
        assert_eq!(
            runtime.contains(&"gpu"),
            cfg!(feature = "gpu-runtime"),
            "runtime gpu feature must follow the facade gpu-runtime feature"
        );
    }

    #[test]
    fn host_only_features_are_excluded() {
        for (krate, features) in boundary_features() {
            for feature in features {
                assert!(!is_host_only(krate, feature), "{krate}/{feature} leaked");
            }
        }
        let fingerprint = build_fingerprint();
        assert!(!fingerprint.contains("executor-pool"));
        assert!(!fingerprint.contains("dylib-plugins"));
        assert!(!fingerprint.contains("engine"));
    }

    #[test]
    fn feature_hash_changes_with_features() {
        assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_ne!(
            fnv1a64(b"features.runtime=plugins;"),
            fnv1a64(b"features.runtime=gpu,plugins;")
        );
    }

    #[test]
    fn mismatch_description_names_differing_segments() {
        let host = "target=x;features.runtime=gpu,plugins;features.hash=1;layout.payload=8/8";
        let plugin = "target=x;features.runtime=plugins;features.hash=2;layout.payload=8/8;extra=1";
        assert_eq!(
            describe_fingerprint_mismatch(host, plugin),
            "features.runtime: host `gpu,plugins`, plugin `plugins`; \
             features.hash: host `1`, plugin `2`; extra: host <missing>, plugin `1`"
        );
        assert_eq!(describe_fingerprint_mismatch(host, host), "");
    }

    /// Keeps each crate's hand-written `ENABLED_FEATURES` list in sync with its manifest.
    #[test]
    fn enabled_feature_lists_cover_every_declared_feature() {
        let crates_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("facade lives under crates/");
        for krate in ["daedalus", "core", "data", "registry", "planner", "runtime"] {
            let dir = crates_dir.join(krate);
            let manifest = std::fs::read_to_string(dir.join("Cargo.toml")).unwrap();
            let lib = std::fs::read_to_string(dir.join("src/lib.rs")).unwrap();
            let declared = manifest_features(&manifest);
            assert!(!declared.is_empty(), "{krate} declares no features");
            for feature in declared {
                let entry = format!("#[cfg(feature = \"{feature}\")]\n    \"{feature}\",");
                assert!(
                    lib.contains(&entry),
                    "crates/{krate}/src/lib.rs ENABLED_FEATURES is missing `{feature}`"
                );
            }
        }
    }

    fn manifest_features(manifest: &str) -> Vec<String> {
        let mut in_features = false;
        let mut out = Vec::new();
        for line in manifest.lines() {
            if line.starts_with('[') {
                in_features = line.trim() == "[features]";
                continue;
            }
            if !in_features {
                continue;
            }
            let Some((key, _)) = line.split_once('=') else {
                continue;
            };
            let key = key.trim();
            let is_key = !key.is_empty()
                && !line.starts_with(char::is_whitespace)
                && key
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
            if is_key && key != "default" {
                out.push(key.to_string());
            }
        }
        out
    }
}

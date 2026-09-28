//! Build fingerprint shared by dynamic plugin hosts and plugins.
//!
//! The fingerprint is a `;`-separated list of `key=value` segments:
//!
//! - `target`, `pointer_width`: the compilation target.
//! - `features.<crate>`: the enabled, boundary-relevant Cargo features of every Daedalus crate
//!   whose types cross the plugin boundary (comma-separated).
//! - `features.hash`: a stable FNV-1a hash of all `features.*` segments.
//! - `layout.<type>`: `size/align` of the Rust types `PluginRegistry` installation touches.
//!
//! Each crate classifies its features in the `[package.metadata.daedalus]` table of its
//! `Cargo.toml` (`boundary-features` / `host-only-features`), read here through the crate's
//! `CARGO_MANIFEST`. Only enabled boundary features enter the fingerprint, so a plugin built
//! with just `dylib-plugins` loads into a host built with `engine-full,dylib-plugins`.
//! `daedalus-transport` has no features and `daedalus-engine` never crosses the boundary.

use std::fmt::Write as _;
use std::mem::{align_of, size_of};
use std::sync::OnceLock;

/// `(name, ENABLED_FEATURES, CARGO_MANIFEST)` of every crate whose types cross the boundary.
fn fingerprinted_crates() -> [(&'static str, &'static str, &'static str); 6] {
    [
        ("daedalus", crate::ENABLED_FEATURES, crate::CARGO_MANIFEST),
        (
            "core",
            daedalus_core::ENABLED_FEATURES,
            daedalus_core::CARGO_MANIFEST,
        ),
        (
            "data",
            daedalus_data::ENABLED_FEATURES,
            daedalus_data::CARGO_MANIFEST,
        ),
        (
            "registry",
            daedalus_registry::ENABLED_FEATURES,
            daedalus_registry::CARGO_MANIFEST,
        ),
        (
            "planner",
            daedalus_planner::ENABLED_FEATURES,
            daedalus_planner::CARGO_MANIFEST,
        ),
        (
            "runtime",
            daedalus_runtime::ENABLED_FEATURES,
            daedalus_runtime::CARGO_MANIFEST,
        ),
    ]
}

/// The string array `key` of the `[package.metadata.daedalus]` table in a `Cargo.toml`
/// (single- or multi-line). Missing tables or keys yield an empty list.
fn metadata_list<'a>(manifest: &'a str, key: &str) -> Vec<&'a str> {
    let mut in_table = false;
    let mut lines = manifest.lines();
    while let Some(line) = lines.next() {
        let line = line.trim();
        if line.starts_with('[') {
            in_table = line == "[package.metadata.daedalus]";
            continue;
        }
        let Some(mut rest) = line
            .strip_prefix(key)
            .filter(|_| in_table)
            .and_then(|rest| rest.trim_start().strip_prefix('='))
        else {
            continue;
        };
        let mut values = Vec::new();
        loop {
            let (chunk, closed) = rest
                .split_once(']')
                .map_or((rest, false), |(c, _)| (c, true));
            values.extend(chunk.split('"').skip(1).step_by(2));
            match lines.next() {
                Some(next) if !closed => rest = next,
                _ => return values,
            }
        }
    }
    Vec::new()
}

/// Boundary-relevant enabled features per crate, in fingerprint order.
pub fn boundary_features() -> Vec<(&'static str, Vec<&'static str>)> {
    fingerprinted_crates()
        .into_iter()
        .map(|(krate, enabled, manifest)| {
            let boundary = metadata_list(manifest, "boundary-features");
            let kept = enabled
                .split(',')
                .filter(|feature| boundary.contains(feature))
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
    use std::collections::BTreeSet;

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
        // This module only exists with `dylib-plugins`, which implies `plugins` and therefore
        // these crate features.
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
        for ((krate, features), (_, _, manifest)) in
            boundary_features().into_iter().zip(fingerprinted_crates())
        {
            let host_only = metadata_list(manifest, "host-only-features");
            for feature in features {
                assert!(!host_only.contains(&feature), "{krate}/{feature} leaked");
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

    #[test]
    fn metadata_lists_parse_single_and_multi_line_arrays() {
        let manifest = "[package]\nname = \"x\"\n\n[package.metadata.daedalus]\n\
            boundary-features = [\"a\", \"b-c\"]\n\
            host-only-features = [\n    \"d\",\n    \"e\",\n]\n\n\
            [features]\nboundary-features = [\"ignored\"]\n";
        assert_eq!(metadata_list(manifest, "boundary-features"), ["a", "b-c"]);
        assert_eq!(metadata_list(manifest, "host-only-features"), ["d", "e"]);
        assert!(metadata_list(manifest, "missing").is_empty());
        assert!(metadata_list("[features]\na = []\n", "boundary-features").is_empty());
    }

    /// A new Cargo feature must be classified in its crate's `[package.metadata.daedalus]`,
    /// otherwise it would silently stay out of the fingerprint.
    #[test]
    fn every_feature_of_every_fingerprinted_crate_is_classified() {
        for (krate, _, manifest) in fingerprinted_crates() {
            let declared: BTreeSet<&str> = manifest_features(manifest).collect();
            let boundary = metadata_list(manifest, "boundary-features");
            let host_only = metadata_list(manifest, "host-only-features");
            assert!(!declared.is_empty(), "{krate} declares no features");
            for feature in &boundary {
                assert!(
                    !host_only.contains(feature),
                    "{krate}/{feature} is classified as both boundary and host-only"
                );
            }
            let classified: BTreeSet<&str> = boundary.into_iter().chain(host_only).collect();
            assert_eq!(
                classified, declared,
                "crates/{krate}/Cargo.toml: `[package.metadata.daedalus]` must list every \
                 feature in exactly one of `boundary-features` / `host-only-features`"
            );
        }
    }

    /// Keys of the `[features]` table, except `default`.
    fn manifest_features(manifest: &str) -> impl Iterator<Item = &str> {
        let mut in_features = false;
        manifest.lines().filter_map(move |line| {
            if line.starts_with('[') {
                in_features = line.trim() == "[features]";
                return None;
            }
            let key = line.split_once('=').filter(|_| in_features)?.0.trim();
            let is_key = !line.starts_with(char::is_whitespace)
                && !key.is_empty()
                && key != "default"
                && key
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
            is_key.then_some(key)
        })
    }
}

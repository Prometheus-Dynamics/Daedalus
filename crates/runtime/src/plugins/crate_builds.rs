//! Build information of third-party crates whose types cross the plugin boundary, so a host can
//! say exactly how a dynamic plugin's build of such a crate differs from its own (see
//! [`PluginRegistry::register_crate_build`]).

use super::*;
use core::fmt;

/// How one crate was built: its name as it appears in type paths, its version and its enabled
/// Cargo features. Captured in the crate itself with [`crate_build_info!`](crate::crate_build_info).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CrateBuildInfo {
    /// `CARGO_CRATE_NAME`, e.g. `styx_core` (the first segment of the crate's type names).
    pub name: &'static str,
    /// `CARGO_PKG_VERSION`.
    pub version: &'static str,
    /// The enabled Cargo features, comma-separated (`CARGO_CFG_FEATURE`).
    pub features: &'static str,
}

impl CrateBuildInfo {
    /// The enabled features, sorted.
    pub fn feature_list(&self) -> Vec<&'static str> {
        let mut features: Vec<_> = self
            .features
            .split(',')
            .map(str::trim)
            .filter(|feature| !feature.is_empty())
            .collect();
        features.sort_unstable();
        features.dedup();
        features
    }
}

/// The [`CrateBuildInfo`] of the invoking crate. Its build script must export the enabled
/// features as `DAEDALUS_CRATE_FEATURES`:
///
/// ```ignore
/// // build.rs
/// fn main() {
///     let features = std::env::var("CARGO_CFG_FEATURE").unwrap_or_default();
///     println!("cargo:rustc-env=DAEDALUS_CRATE_FEATURES={features}");
/// }
/// ```
///
/// Register it from the crate's Daedalus plugin (`#[plugin(.., crate_build)]`, or
/// `registry.register_crate_build(daedalus::crate_build_info!())` in an install hook).
#[macro_export]
macro_rules! crate_build_info {
    () => {
        $crate::plugins::CrateBuildInfo {
            name: env!("CARGO_CRATE_NAME"),
            version: env!("CARGO_PKG_VERSION"),
            features: env!(
                "DAEDALUS_CRATE_FEATURES",
                "`crate_build_info!` needs the crate's build.rs to run `println!(\"cargo:rustc-env=\
                 DAEDALUS_CRATE_FEATURES={}\", std::env::var(\"CARGO_CFG_FEATURE\")\
                 .unwrap_or_default())`"
            ),
        }
    };
}

/// One crate built differently by the host and a dynamic plugin (version or features).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CrateBuildDiff {
    pub host: CrateBuildInfo,
    pub plugin: CrateBuildInfo,
}

impl CrateBuildDiff {
    /// `Some` when `host` and `plugin` describe the same crate built differently.
    pub fn new(host: CrateBuildInfo, plugin: CrateBuildInfo) -> Option<Self> {
        let differs =
            host.version != plugin.version || host.feature_list() != plugin.feature_list();
        (host.name == plugin.name && differs).then_some(Self { host, plugin })
    }

    /// Features the host enables and the plugin's build does not.
    pub fn missing_in_plugin(&self) -> Vec<&'static str> {
        let plugin = self.plugin.feature_list();
        let mut host = self.host.feature_list();
        host.retain(|feature| !plugin.contains(feature));
        host
    }

    /// Features the plugin's build enables and the host does not.
    pub fn extra_in_plugin(&self) -> Vec<&'static str> {
        let host = self.host.feature_list();
        let mut plugin = self.plugin.feature_list();
        plugin.retain(|feature| !host.contains(feature));
        plugin
    }
}

/// E.g. ``crate `styx_core` 0.4.0: host features `framelease,v4l2`, plugin features `framelease`
/// (missing in plugin: v4l2)``.
impl fmt::Display for CrateBuildDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (host, plugin) = (&self.host, &self.plugin);
        let features = |info: &CrateBuildInfo| info.feature_list().join(",");
        if host.version == plugin.version {
            write!(f, "crate `{}` {}: host", host.name, host.version)?;
        } else {
            write!(f, "crate `{}`: host {}", host.name, host.version)?;
        }
        write!(f, " features `{}`, plugin", features(host))?;
        if host.version != plugin.version {
            write!(f, " {}", plugin.version)?;
        }
        write!(f, " features `{}`", features(plugin))?;
        let lists = [
            ("missing in plugin", self.missing_in_plugin()),
            ("only in plugin", self.extra_in_plugin()),
        ];
        let lists: Vec<String> = lists
            .into_iter()
            .filter(|(_, list)| !list.is_empty())
            .map(|(label, list)| format!("{label}: {}", list.join(", ")))
            .collect();
        if !lists.is_empty() {
            write!(f, " ({})", lists.join("; "))?;
        }
        Ok(())
    }
}

impl PluginRegistry {
    /// Record how a crate whose types cross the plugin boundary was built in this build, so
    /// [`Self::crate_build_diffs`] (and a dynamic plugin's boundary type conflicts) can name the
    /// exact version and feature differences of a separately built plugin. The first
    /// registration of a crate name wins; later ones are ignored.
    pub fn register_crate_build(&mut self, info: CrateBuildInfo) -> PluginResult<()> {
        self.ensure_open()?;
        self.crate_builds.entry(info.name).or_insert(info);
        Ok(())
    }

    /// The crate builds recorded by [`Self::register_crate_build`], by crate name.
    pub fn crate_builds(&self) -> &BTreeMap<&'static str, CrateBuildInfo> {
        &self.crate_builds
    }

    /// Every crate of `other` (another build's crate builds, e.g. a dynamic plugin's) this
    /// registry recorded with another version or feature set.
    pub fn crate_build_diffs<'a>(
        &self,
        other: impl IntoIterator<Item = &'a CrateBuildInfo>,
    ) -> Vec<CrateBuildDiff> {
        other
            .into_iter()
            .filter_map(|other| CrateBuildDiff::new(*self.crate_builds.get(other.name)?, *other))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOST: CrateBuildInfo = CrateBuildInfo {
        name: "styx_core",
        version: "0.4.0",
        features: "v4l2,framelease",
    };

    #[test]
    fn diffs_name_version_and_feature_differences() {
        let plugin = CrateBuildInfo {
            features: "framelease",
            ..HOST
        };
        let diff = CrateBuildDiff::new(HOST, plugin).expect("differs");
        assert_eq!(diff.missing_in_plugin(), ["v4l2"]);
        assert_eq!(
            diff.to_string(),
            "crate `styx_core` 0.4.0: host features `framelease,v4l2`, plugin features \
             `framelease` (missing in plugin: v4l2)"
        );
        let plugin = CrateBuildInfo {
            version: "0.5.0",
            features: "framelease,v4l2,mock",
            ..HOST
        };
        assert_eq!(
            CrateBuildDiff::new(HOST, plugin).unwrap().to_string(),
            "crate `styx_core`: host 0.4.0 features `framelease,v4l2`, plugin 0.5.0 features \
             `framelease,mock,v4l2` (only in plugin: mock)"
        );
        let reordered = CrateBuildInfo {
            features: "framelease,v4l2",
            ..HOST
        };
        assert_eq!(CrateBuildDiff::new(HOST, reordered), None);
    }

    #[test]
    fn registry_compares_crates_it_knows() {
        let mut registry = PluginRegistry::new();
        registry.register_crate_build(HOST).unwrap();
        // First registration wins.
        let other = CrateBuildInfo {
            features: "",
            ..HOST
        };
        registry.register_crate_build(other).unwrap();
        assert_eq!(registry.crate_builds()["styx_core"], HOST);
        let unknown = CrateBuildInfo {
            name: "other",
            ..other
        };
        let diffs = registry.crate_build_diffs(&[other, unknown, HOST]);
        assert_eq!(diffs, [CrateBuildDiff::new(HOST, other).unwrap()]);
        assert_eq!(
            diffs[0].to_string(),
            "crate `styx_core` 0.4.0: host features `framelease,v4l2`, plugin features `` \
             (missing in plugin: framelease, v4l2)"
        );
    }
}

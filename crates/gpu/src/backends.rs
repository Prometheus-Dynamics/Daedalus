//! Which graphics APIs the `wgpu` backend enumerates.
//!
//! [`GpuBackends`] is crate-owned, so the public API does not expose `wgpu::Backends`. The
//! `WGPU_BACKEND` environment variable uses the names `wgpu` accepts; [`resolve_backends`] applies
//! the precedence: the environment variable, then [`GpuOptions::backends`](crate::GpuOptions),
//! then every backend.

use bitflags::bitflags;
use serde::{Deserialize, Serialize};

bitflags! {
    /// Graphics APIs the `wgpu` backend may enumerate. Combine them with `|`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct GpuBackends: u8 {
        const VULKAN = 1 << 0;
        const METAL = 1 << 1;
        const DX12 = 1 << 2;
        const GL = 1 << 3;
        const BROWSER_WEBGPU = 1 << 4;
    }
}

impl GpuBackends {
    /// Parse a comma-separated list of backend names, case-insensitively, with the spellings
    /// `WGPU_BACKEND` accepts: `vulkan` or `vk`, `dx12` or `d3d12`, `metal` or `mtl`,
    /// `gl`, `gles` or `opengl`, and `webgpu`. Unknown names are skipped.
    pub fn from_names(list: &str) -> Self {
        list.split(',').fold(Self::empty(), |acc, name| {
            acc | match name.trim().to_ascii_lowercase().as_str() {
                "vulkan" | "vk" => Self::VULKAN,
                "dx12" | "d3d12" => Self::DX12,
                "metal" | "mtl" => Self::METAL,
                "gl" | "gles" | "opengl" => Self::GL,
                "webgpu" => Self::BROWSER_WEBGPU,
                _ => Self::empty(),
            }
        })
    }
}

/// The backends to enumerate. The first of `env` (the parsed `WGPU_BACKEND`) and `requested` that
/// names at least one backend wins; with neither, every backend is enumerated.
#[cfg_attr(not(feature = "gpu-wgpu"), allow(dead_code))]
pub(crate) fn resolve_backends(
    env: Option<GpuBackends>,
    requested: Option<GpuBackends>,
) -> GpuBackends {
    [env, requested]
        .into_iter()
        .flatten()
        .find(|backends| !backends.is_empty())
        .unwrap_or_else(GpuBackends::all)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environment_variable_beats_the_requested_backends() {
        let env = Some(GpuBackends::from_names("vulkan"));
        let requested = Some(GpuBackends::GL);
        assert_eq!(resolve_backends(env, requested), GpuBackends::VULKAN);
    }

    #[test]
    fn requested_backends_apply_when_the_variable_is_unset() {
        assert_eq!(
            resolve_backends(None, Some(GpuBackends::GL)),
            GpuBackends::GL
        );
    }

    #[test]
    fn a_variable_naming_no_backend_falls_through_to_the_request() {
        let env = Some(GpuBackends::from_names("no-such-api"));
        assert_eq!(
            resolve_backends(env, Some(GpuBackends::DX12)),
            GpuBackends::DX12
        );
        assert_eq!(
            resolve_backends(Some(GpuBackends::empty()), Some(GpuBackends::METAL)),
            GpuBackends::METAL
        );
    }

    #[test]
    fn every_backend_is_enumerated_when_neither_names_one() {
        assert_eq!(resolve_backends(None, None), GpuBackends::all());
        assert_eq!(
            resolve_backends(None, Some(GpuBackends::empty())),
            GpuBackends::all()
        );
    }

    #[test]
    fn names_parse_with_aliases_case_and_whitespace() {
        assert_eq!(
            GpuBackends::from_names(" VK , gles,bogus,d3d12"),
            GpuBackends::VULKAN | GpuBackends::GL | GpuBackends::DX12
        );
        assert_eq!(
            GpuBackends::from_names("mtl,webgpu"),
            GpuBackends::METAL | GpuBackends::BROWSER_WEBGPU
        );
        assert!(GpuBackends::from_names("").is_empty());
    }
}

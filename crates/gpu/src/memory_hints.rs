//! How the wgpu device sizes its memory blocks ([`GpuOptions::memory_hints`]).
//!
//! [`GpuOptions::memory_hints`]: crate::GpuOptions::memory_hints

use serde::{Deserialize, Serialize};

/// The wgpu device's memory allocation strategy (`wgpu::MemoryHints`).
///
/// `Performance` makes large memory blocks up front: the first allocation of each memory type
/// pays for a whole block (a 42 ms tick for the first readback buffer on the CM5's V3D, 3.6 ms
/// on desktop RADV). `MemoryUsage` allocates closer to what is asked (0.23 ms on RADV).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum GpuMemoryHints {
    /// `MemoryUsage` on integrated, virtual and CPU adapters (V3D, Mali: memory shared with
    /// the CPU), `Performance` on discrete GPUs. `DAEDALUS_GPU_MEMORY_HINTS=performance|memory`
    /// overrides it.
    #[default]
    Auto,
    Performance,
    MemoryUsage,
}

impl GpuMemoryHints {
    /// Parse `performance` or `memory` / `memory_usage` (case-insensitive).
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "performance" => Some(Self::Performance),
            "memory" | "memory_usage" | "memoryusage" => Some(Self::MemoryUsage),
            _ => None,
        }
    }

    /// The hints for a device on an adapter of `device_type`: `Auto` resolved, never `Auto`.
    #[cfg(feature = "gpu-wgpu")]
    pub(crate) fn resolve(self, device_type: wgpu::DeviceType) -> Self {
        let requested = match self {
            Self::Auto => std::env::var("DAEDALUS_GPU_MEMORY_HINTS")
                .ok()
                .and_then(|raw| Self::parse(&raw))
                .unwrap_or(Self::Auto),
            hints => hints,
        };
        match requested {
            Self::Auto => match device_type {
                wgpu::DeviceType::DiscreteGpu | wgpu::DeviceType::Other => Self::Performance,
                wgpu::DeviceType::IntegratedGpu
                | wgpu::DeviceType::VirtualGpu
                | wgpu::DeviceType::Cpu => Self::MemoryUsage,
            },
            hints => hints,
        }
    }

    #[cfg(feature = "gpu-wgpu")]
    pub(crate) fn to_wgpu(self) -> wgpu::MemoryHints {
        match self {
            Self::MemoryUsage => wgpu::MemoryHints::MemoryUsage,
            Self::Auto | Self::Performance => wgpu::MemoryHints::Performance,
        }
    }
}

#[cfg(all(test, feature = "gpu-wgpu"))]
mod tests {
    use super::GpuMemoryHints;

    #[test]
    fn explicit_hints_are_kept_and_auto_follows_the_adapter() {
        for device in [
            wgpu::DeviceType::DiscreteGpu,
            wgpu::DeviceType::IntegratedGpu,
        ] {
            assert_eq!(
                GpuMemoryHints::Performance.resolve(device),
                GpuMemoryHints::Performance
            );
            assert_eq!(
                GpuMemoryHints::MemoryUsage.resolve(device),
                GpuMemoryHints::MemoryUsage
            );
        }
        if std::env::var_os("DAEDALUS_GPU_MEMORY_HINTS").is_none() {
            assert_eq!(
                GpuMemoryHints::Auto.resolve(wgpu::DeviceType::IntegratedGpu),
                GpuMemoryHints::MemoryUsage
            );
            assert_eq!(
                GpuMemoryHints::Auto.resolve(wgpu::DeviceType::DiscreteGpu),
                GpuMemoryHints::Performance
            );
        }
        assert_eq!(
            GpuMemoryHints::parse(" Memory "),
            Some(GpuMemoryHints::MemoryUsage)
        );
        assert_eq!(GpuMemoryHints::parse("fast"), None);
    }
}

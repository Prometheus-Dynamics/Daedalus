//! The memory spaces of this backend, and how its device sizes memory blocks.

use std::borrow::Cow;

use daedalus_backend::{MemorySpace, SpaceKind};

use crate::adapter::DeviceKind;

/// A buffer in device memory. Where this backend's outputs live.
pub const SPACE_DEVICE: &str = "wgpu.buffer";
/// A buffer the host can also read and write in place. On a GPU that shares memory with the CPU
/// (V3D, Mali, integrated GPUs, lavapipe) an upload or a readback here is a plain copy.
pub const SPACE_HOST_MAPPED: &str = "wgpu.mapped";
/// Host-mapped, in cached memory, for buffers the host reads a lot.
pub const SPACE_HOST_CACHED: &str = "wgpu.cached";

pub(crate) fn spaces() -> Vec<MemorySpace> {
    let space = |id, kind| MemorySpace {
        id: Cow::Borrowed(id),
        kind,
        readable: true,
    };
    vec![
        space(SPACE_DEVICE, SpaceKind::Device),
        space(SPACE_HOST_MAPPED, SpaceKind::Shared),
        space(SPACE_HOST_CACHED, SpaceKind::Shared),
    ]
}

/// How the device sizes its memory blocks.
///
/// `Performance` makes large blocks up front, so the first allocation of each kind pays for a
/// whole block. `MemoryUsage` allocates close to what is asked.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum MemoryHints {
    /// `MemoryUsage` on integrated, virtual and CPU adapters, whose memory is shared with the
    /// host, and `Performance` on the rest.
    #[default]
    Auto,
    Performance,
    MemoryUsage,
}

impl MemoryHints {
    /// Parses `auto`, `performance`, or `memory`, ignoring case, as a host's setting gives it.
    pub fn parse(text: &str) -> Option<MemoryHints> {
        match text.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(MemoryHints::Auto),
            "performance" => Some(MemoryHints::Performance),
            "memory" | "memory_usage" | "memoryusage" => Some(MemoryHints::MemoryUsage),
            _ => None,
        }
    }

    /// What `Auto` means on an adapter of `kind`. Never `Auto`.
    pub fn resolve(self, kind: DeviceKind) -> MemoryHints {
        match (self, kind) {
            (MemoryHints::Auto, DeviceKind::Discrete | DeviceKind::Other) => {
                MemoryHints::Performance
            }
            (MemoryHints::Auto, _) => MemoryHints::MemoryUsage,
            (explicit, _) => explicit,
        }
    }

    pub(crate) fn to_wgpu(self) -> wgpu::MemoryHints {
        match self {
            MemoryHints::MemoryUsage => wgpu::MemoryHints::MemoryUsage,
            MemoryHints::Auto | MemoryHints::Performance => wgpu::MemoryHints::Performance,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryHints;
    use crate::adapter::DeviceKind;

    #[test]
    fn explicit_hints_are_kept_and_auto_follows_the_adapter() {
        let kinds = [
            DeviceKind::Discrete,
            DeviceKind::Integrated,
            DeviceKind::Virtual,
            DeviceKind::Cpu,
            DeviceKind::Other,
        ];
        for kind in kinds {
            for explicit in [MemoryHints::Performance, MemoryHints::MemoryUsage] {
                assert_eq!(explicit.resolve(kind), explicit);
            }
            let shared = matches!(
                kind,
                DeviceKind::Integrated | DeviceKind::Virtual | DeviceKind::Cpu
            );
            let expected = if shared {
                MemoryHints::MemoryUsage
            } else {
                MemoryHints::Performance
            };
            assert_eq!(MemoryHints::Auto.resolve(kind), expected, "{kind:?}");
        }
        assert_eq!(
            MemoryHints::parse(" Memory "),
            Some(MemoryHints::MemoryUsage)
        );
        assert_eq!(
            MemoryHints::parse("performance"),
            Some(MemoryHints::Performance)
        );
        assert_eq!(MemoryHints::parse("fast"), None);
    }
}

//! What kind of GPU an adapter is, for choices the limits do not show: tile-based or
//! immediate-mode, the execution width, a default workgroup size.
//!
//! Classified from the adapter's PCI vendor id and name. The Mali entries (RK3588S G610, RK3576
//! and RK3566 G52) follow ARM's published architecture (Bifrost: 4- or 8-wide warps, Valhall:
//! 16-wide), and the PowerVR entry (Allwinner A733, BXM-4-64) is an assumed width. All are written
//! before board runs: `docs/testing.md` lists what each board run must confirm.

use crate::{GpuAdapterInfo, GpuContextHandle};

impl GpuContextHandle {
    /// How to shape work for this adapter ([`GpuDeviceProfile::classify`]).
    pub fn device_profile(&self) -> GpuDeviceProfile {
        GpuDeviceProfile::classify(self.adapter_info())
    }
}

/// PCI vendor ids (Khronos registry ids for vendors without one).
pub mod vendor_ids {
    pub const ARM: u32 = 0x13B5;
    pub const BROADCOM: u32 = 0x14E4;
    pub const QUALCOMM: u32 = 0x5143;
    pub const IMAGINATION: u32 = 0x1010;
    pub const APPLE: u32 = 0x106B;
    pub const NVIDIA: u32 = 0x10DE;
    pub const AMD: u32 = 0x1002;
    pub const INTEL: u32 = 0x8086;
    /// `VK_VENDOR_ID_MESA`: llvmpipe / lavapipe.
    pub const MESA: u32 = 0x10005;
}

/// The GPU family.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GpuFamily {
    /// Mali Bifrost (G31, G51, G52, G71, G72, G76).
    MaliBifrost,
    /// Mali Valhall (G57, G68, G77, G78, G310, G510, G610, G710, G615, G715, ...).
    MaliValhall,
    /// A Mali not recognized by name (Midgard or newer than this table).
    MaliOther,
    /// Broadcom VideoCore (V3D on the Raspberry Pi 4 / 5 and CM5).
    VideoCore,
    /// Qualcomm Adreno.
    Adreno,
    /// Imagination PowerVR.
    PowerVr,
    Apple,
    /// NVIDIA, AMD or Intel.
    Desktop,
    /// A CPU implementation (llvmpipe, lavapipe, SwiftShader, WARP).
    Software,
    Unknown,
}

/// How to shape work for an adapter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GpuDeviceProfile {
    pub family: GpuFamily,
    /// The PCI vendor id, when the adapter reports one.
    pub vendor: Option<u32>,
    /// Tile-based rendering: fragment passes over a target beat compute for per-pixel work
    /// (measured on V3D; ARM documents the same for Mali).
    pub tile_based: bool,
    /// Threads executing together (warp, wave, quad), when known.
    pub warp_width: Option<u32>,
    /// A default 1-D workgroup size: a multiple of the warp width, at most 64 on mobile parts
    /// (ARM's guidance for Mali, register pressure on Bifrost).
    pub workgroup_size: u32,
}

impl GpuDeviceProfile {
    /// Run per-pixel kernels as fragment passes (`FragmentPass`) rather than compute.
    pub fn prefer_fragment_passes(&self) -> bool {
        self.tile_based
    }

    /// Classify an adapter from its info.
    pub fn classify(info: &GpuAdapterInfo) -> Self {
        let vendor = info.vendor_id.as_deref().and_then(parse_id);
        Self::from_parts(vendor, &info.name)
    }

    /// Classify from a vendor id and adapter name.
    pub fn from_parts(vendor: Option<u32>, name: &str) -> Self {
        let lower = name.to_ascii_lowercase();
        let family = family(vendor, &lower);
        let (tile_based, warp_width, workgroup_size) = match family {
            GpuFamily::MaliBifrost => (true, Some(bifrost_warp(&lower)), 64),
            GpuFamily::MaliValhall => (true, Some(16), 64),
            GpuFamily::MaliOther => (true, None, 64),
            // V3D executes 16-wide (4-way SIMD over 4 cycles).
            GpuFamily::VideoCore => (true, Some(16), 64),
            GpuFamily::Adreno => (true, None, 64),
            GpuFamily::PowerVr => (true, Some(32), 64),
            GpuFamily::Apple => (true, Some(32), 64),
            GpuFamily::Desktop => {
                let warp = match vendor {
                    Some(vendor_ids::NVIDIA) => Some(32),
                    Some(vendor_ids::AMD) => Some(64),
                    _ => None,
                };
                (false, warp, 64)
            }
            GpuFamily::Software | GpuFamily::Unknown => (false, None, 64),
        };
        Self {
            family,
            vendor,
            tile_based,
            warp_width,
            workgroup_size,
        }
    }
}

fn parse_id(id: &str) -> Option<u32> {
    let id = id.trim();
    match id.strip_prefix("0x").or_else(|| id.strip_prefix("0X")) {
        Some(hex) => u32::from_str_radix(hex, 16).ok(),
        None => id.parse().ok(),
    }
}

const BIFROST: [&str; 6] = ["g31", "g51", "g52", "g71", "g72", "g76"];
const VALHALL: [&str; 12] = [
    "g57", "g68", "g77", "g78", "g310", "g510", "g610", "g710", "g615", "g715", "g620", "g720",
];

/// The Mali model ("g610") in a lowercase adapter name ("mali-g610 (panfrost)", "mali-g52 r1").
fn mali_model(lower: &str) -> Option<&str> {
    let at = lower.find("mali-")? + "mali-".len();
    let rest = &lower[at..];
    let end = rest
        .find(|c: char| !c.is_ascii_alphanumeric())
        .unwrap_or(rest.len());
    Some(&rest[..end])
}

fn family(vendor: Option<u32>, lower: &str) -> GpuFamily {
    if lower.contains("llvmpipe")
        || lower.contains("lavapipe")
        || lower.contains("swiftshader")
        || lower.contains("microsoft basic render")
        || vendor == Some(vendor_ids::MESA)
    {
        return GpuFamily::Software;
    }
    if vendor == Some(vendor_ids::ARM) || lower.contains("mali") || lower.contains("immortalis") {
        return match mali_model(lower) {
            Some(m) if BIFROST.contains(&m) => GpuFamily::MaliBifrost,
            Some(m) if VALHALL.contains(&m) => GpuFamily::MaliValhall,
            _ if lower.contains("immortalis") => GpuFamily::MaliValhall,
            _ => GpuFamily::MaliOther,
        };
    }
    if vendor == Some(vendor_ids::BROADCOM) || lower.contains("v3d") || lower.contains("videocore")
    {
        return GpuFamily::VideoCore;
    }
    if vendor == Some(vendor_ids::QUALCOMM) || lower.contains("adreno") {
        return GpuFamily::Adreno;
    }
    if vendor == Some(vendor_ids::IMAGINATION) || lower.contains("powervr") {
        return GpuFamily::PowerVr;
    }
    if vendor == Some(vendor_ids::APPLE) || lower.starts_with("apple") {
        return GpuFamily::Apple;
    }
    if matches!(
        vendor,
        Some(vendor_ids::NVIDIA | vendor_ids::AMD | vendor_ids::INTEL)
    ) {
        return GpuFamily::Desktop;
    }
    GpuFamily::Unknown
}

/// Bifrost warp width: 4 on the first generation (G71, G72, G31, G51), 8 on G76 and G52.
fn bifrost_warp(lower: &str) -> u32 {
    match mali_model(lower) {
        Some("g76" | "g52") => 8,
        _ => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adapters_classify_by_vendor_and_name() {
        let cases = [
            (
                Some(0x13B5),
                "Mali-G610 MC4",
                GpuFamily::MaliValhall,
                true,
                Some(16),
            ),
            (
                Some(0x13B5),
                "Mali-G610 (Panfrost)",
                GpuFamily::MaliValhall,
                true,
                Some(16),
            ),
            (
                Some(0x13B5),
                "Mali-G52 r1",
                GpuFamily::MaliBifrost,
                true,
                Some(8),
            ),
            (
                None,
                "Mali-G52 (Panfrost)",
                GpuFamily::MaliBifrost,
                true,
                Some(8),
            ),
            (
                Some(0x13B5),
                "Mali-G72",
                GpuFamily::MaliBifrost,
                true,
                Some(4),
            ),
            (Some(0x13B5), "Mali-T860", GpuFamily::MaliOther, true, None),
            (
                Some(0x13B5),
                "Mali-G52 2EE",
                GpuFamily::MaliBifrost,
                true,
                Some(8),
            ),
            (
                Some(0x1010),
                "PowerVR B-Series BXM-4-64 MC1",
                GpuFamily::PowerVr,
                true,
                Some(32),
            ),
            (
                None,
                "Imagination PowerVR BXM-4-64",
                GpuFamily::PowerVr,
                true,
                Some(32),
            ),
            (
                Some(0x14E4),
                "V3D 7.1.10.2",
                GpuFamily::VideoCore,
                true,
                Some(16),
            ),
            (
                Some(0x5143),
                "Adreno (TM) 740",
                GpuFamily::Adreno,
                true,
                None,
            ),
            (
                Some(0x10DE),
                "NVIDIA GeForce RTX 4060",
                GpuFamily::Desktop,
                false,
                Some(32),
            ),
            (
                Some(0x1002),
                "AMD Radeon RX 7600 (RADV NAVI33)",
                GpuFamily::Desktop,
                false,
                Some(64),
            ),
            (
                Some(0x10005),
                "llvmpipe (LLVM 19.1.7, 256 bits)",
                GpuFamily::Software,
                false,
                None,
            ),
            (None, "something new", GpuFamily::Unknown, false, None),
        ];
        for (vendor, name, family, tile, warp) in cases {
            let p = GpuDeviceProfile::from_parts(vendor, name);
            assert_eq!(
                (p.family, p.tile_based, p.warp_width),
                (family, tile, warp),
                "{name}"
            );
            assert_eq!(p.prefer_fragment_passes(), tile);
            assert!(
                p.workgroup_size.is_multiple_of(p.warp_width.unwrap_or(1)),
                "{name}"
            );
        }
        let info = GpuAdapterInfo {
            name: "Mali-G610".into(),
            backend: crate::GpuBackendKind::Wgpu,
            device_id: None,
            vendor_id: Some("5045".into()), // 0x13B5 in decimal, as wgpu reports it
        };
        assert_eq!(
            GpuDeviceProfile::classify(&info).family,
            GpuFamily::MaliValhall
        );
        assert_eq!(parse_id("0x13b5"), Some(0x13B5));
        // AFBC 16x16 | YTR | SPARSE, as Rockchip's VOP2 scans out.
        let afbc = crate::drm_format_mod_arm_afbc(1 | (1 << 4) | (1 << 6));
        assert_eq!(afbc, 0x0800_0000_0000_0051);
        assert!(crate::is_arm_afbc(afbc));
        assert!(!crate::is_arm_afbc(crate::DRM_FORMAT_MOD_LINEAR));
        assert!(!crate::is_arm_afbc(0x0700_0000_0000_0006), "Broadcom UIF");
    }

    /// The machine's adapter classifies, and the GPU kernels run at the profile's workgroup
    /// size (64 everywhere today).
    #[test]
    #[cfg(feature = "gpu-wgpu")]
    fn this_adapter_classifies() {
        let ctx = crate::select_backend(&crate::GpuOptions {
            preferred_backend: Some(crate::GpuBackendKind::Wgpu),
            ..Default::default()
        })
        .unwrap();
        if ctx.wgpu().is_none() {
            return;
        }
        let profile = ctx.device_profile();
        assert_ne!(
            profile.family,
            GpuFamily::Unknown,
            "{:?}",
            ctx.adapter_info()
        );
        assert_eq!(profile.workgroup_size, 64);
    }
}

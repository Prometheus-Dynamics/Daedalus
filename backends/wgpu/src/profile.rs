//! What kind of GPU an adapter is, for choices its limits don't show: tile-based or not, how many
//! threads run together, a default workgroup size.
//!
//! Classified from the PCI vendor id and the name. The V3D and desktop rows are measured. The
//! Mali, PowerVR, Adreno and Apple rows follow published architecture and carry
//! [`DeviceProfile::measured`] `false` until a board run confirms them.

use crate::adapter::AdapterInfo;

/// PCI vendor ids, or Khronos ids for vendors without one.
pub mod vendor_ids {
    pub(crate) const ARM: u32 = 0x13B5;
    pub(crate) const BROADCOM: u32 = 0x14E4;
    pub(crate) const QUALCOMM: u32 = 0x5143;
    pub(crate) const IMAGINATION: u32 = 0x1010;
    pub(crate) const APPLE: u32 = 0x106B;
    pub const NVIDIA: u32 = 0x10DE;
    pub const AMD: u32 = 0x1002;
    pub(crate) const INTEL: u32 = 0x8086;
    /// llvmpipe and lavapipe.
    pub(crate) const MESA: u32 = 0x10005;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Family {
    /// Mali G31, G51, G52, G71, G72, G76.
    MaliBifrost,
    /// Mali G57 and later, and Immortalis.
    MaliValhall,
    /// A Mali this table doesn't know by name.
    MaliOther,
    /// Broadcom V3D, as on the Raspberry Pi 4 and 5.
    VideoCore,
    Adreno,
    PowerVr,
    Apple,
    /// NVIDIA, AMD or Intel.
    Desktop,
    /// A CPU implementation.
    Software,
    Unknown,
}

/// How to shape work for an adapter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DeviceProfile {
    pub family: Family,
    /// The PCI vendor id, when the adapter reports one.
    pub vendor: Option<u32>,
    /// Tile-based rendering: a fragment pass over a target beats compute for per-pixel work.
    pub tile_based: bool,
    /// Threads that execute together, when known.
    pub warp_width: Option<u32>,
    /// A default one-dimensional workgroup size: a multiple of the warp width.
    pub workgroup_size: u32,
    /// False while this row is from documentation and no board run has confirmed it.
    pub measured: bool,
}

impl DeviceProfile {
    /// Whether per-pixel work should run as fragment passes.
    pub fn prefer_fragment_passes(&self) -> bool {
        self.tile_based
    }

    pub fn classify(info: &AdapterInfo) -> DeviceProfile {
        DeviceProfile::from_parts((info.vendor != 0).then_some(info.vendor), &info.name)
    }

    pub fn from_parts(vendor: Option<u32>, name: &str) -> DeviceProfile {
        let lower = name.to_ascii_lowercase();
        let family = family(vendor, &lower);
        let (tile_based, warp_width, measured) = match family {
            Family::MaliBifrost => (true, Some(bifrost_warp(&lower)), false),
            Family::MaliValhall => (true, Some(16), false),
            Family::MaliOther | Family::Adreno => (true, None, false),
            Family::VideoCore => (true, Some(16), true),
            Family::PowerVr | Family::Apple => (true, Some(32), false),
            Family::Desktop => {
                let warp = match vendor {
                    Some(vendor_ids::NVIDIA) => Some(32),
                    Some(vendor_ids::AMD) => Some(64),
                    _ => None,
                };
                (false, warp, true)
            }
            Family::Software | Family::Unknown => (false, None, true),
        };
        DeviceProfile {
            family,
            vendor,
            tile_based,
            warp_width,
            workgroup_size: 64,
            measured,
        }
    }
}

const BIFROST: [&str; 6] = ["g31", "g51", "g52", "g71", "g72", "g76"];
const VALHALL: [&str; 12] = [
    "g57", "g68", "g77", "g78", "g310", "g510", "g610", "g710", "g615", "g715", "g620", "g720",
];

/// The model in a lowercase name: `g610` in `mali-g610 (panfrost)`.
fn mali_model(lower: &str) -> Option<&str> {
    let rest = lower.split_once("mali-")?.1;
    let end = rest
        .find(|c: char| !c.is_ascii_alphanumeric())
        .unwrap_or(rest.len());
    rest.get(..end)
}

fn family(vendor: Option<u32>, lower: &str) -> Family {
    if crate::is_software(lower)
        || lower.contains("microsoft basic render")
        || vendor == Some(vendor_ids::MESA)
    {
        return Family::Software;
    }
    if vendor == Some(vendor_ids::ARM) || lower.contains("mali") || lower.contains("immortalis") {
        return match mali_model(lower) {
            Some(model) if BIFROST.contains(&model) => Family::MaliBifrost,
            Some(model) if VALHALL.contains(&model) => Family::MaliValhall,
            _ if lower.contains("immortalis") => Family::MaliValhall,
            _ => Family::MaliOther,
        };
    }
    if vendor == Some(vendor_ids::BROADCOM) || lower.contains("v3d") || lower.contains("videocore")
    {
        return Family::VideoCore;
    }
    if vendor == Some(vendor_ids::QUALCOMM) || lower.contains("adreno") {
        return Family::Adreno;
    }
    if vendor == Some(vendor_ids::IMAGINATION) || lower.contains("powervr") {
        return Family::PowerVr;
    }
    if vendor == Some(vendor_ids::APPLE) || lower.starts_with("apple") {
        return Family::Apple;
    }
    if matches!(
        vendor,
        Some(vendor_ids::NVIDIA | vendor_ids::AMD | vendor_ids::INTEL)
    ) {
        return Family::Desktop;
    }
    Family::Unknown
}

/// 4 on the first Bifrost generation, 8 on G76 and G52.
fn bifrost_warp(lower: &str) -> u32 {
    match mali_model(lower) {
        Some("g76" | "g52") => 8,
        _ => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::{DeviceProfile, Family};

    #[test]
    fn adapters_classify_by_vendor_and_name() {
        use Family::{
            Adreno, Desktop, MaliBifrost, MaliOther, MaliValhall, PowerVr, Software, Unknown,
            VideoCore,
        };
        let cases = [
            (
                Some(0x13B5),
                "Mali-G610 MC4",
                MaliValhall,
                true,
                Some(16),
                false,
            ),
            (
                Some(0x13B5),
                "Mali-G610 (Panfrost)",
                MaliValhall,
                true,
                Some(16),
                false,
            ),
            (
                Some(0x13B5),
                "Mali-G52 r1",
                MaliBifrost,
                true,
                Some(8),
                false,
            ),
            (
                None,
                "Mali-G52 (Panfrost)",
                MaliBifrost,
                true,
                Some(8),
                false,
            ),
            (Some(0x13B5), "Mali-G72", MaliBifrost, true, Some(4), false),
            (Some(0x13B5), "Mali-T860", MaliOther, true, None, false),
            (
                Some(0x1010),
                "PowerVR B-Series BXM-4-64 MC1",
                PowerVr,
                true,
                Some(32),
                false,
            ),
            (
                None,
                "Imagination PowerVR BXM-4-64",
                PowerVr,
                true,
                Some(32),
                false,
            ),
            (
                Some(0x14E4),
                "V3D 7.1.10.2",
                VideoCore,
                true,
                Some(16),
                true,
            ),
            (Some(0x5143), "Adreno (TM) 740", Adreno, true, None, false),
            (
                Some(0x10DE),
                "NVIDIA GeForce RTX 4060",
                Desktop,
                false,
                Some(32),
                true,
            ),
            (
                Some(0x1002),
                "AMD Radeon RX 7600 (RADV NAVI33)",
                Desktop,
                false,
                Some(64),
                true,
            ),
            (
                Some(0x10005),
                "llvmpipe (LLVM 19.1.7, 256 bits)",
                Software,
                false,
                None,
                true,
            ),
            (None, "something new", Unknown, false, None, true),
        ];
        for (vendor, name, family, tile, warp, measured) in cases {
            let profile = DeviceProfile::from_parts(vendor, name);
            assert_eq!(
                (
                    profile.family,
                    profile.tile_based,
                    profile.warp_width,
                    profile.measured
                ),
                (family, tile, warp, measured),
                "{name}"
            );
            assert_eq!(profile.prefer_fragment_passes(), tile);
            assert!(
                profile
                    .workgroup_size
                    .is_multiple_of(profile.warp_width.unwrap_or(1)),
                "{name}"
            );
        }
    }
}

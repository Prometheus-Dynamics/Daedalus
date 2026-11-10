//! DRM format modifiers, and what individual drivers ask of an image's memory beyond its layout.

use super::layout::{Described, Fourcc, ImportError, Layout};
use crate::profile::{Family, vendor_ids};

/// Rows one after the other, with nothing else in the memory.
pub const MOD_LINEAR: u64 = 0;
/// Not a layout: the producer didn't say.
pub const MOD_INVALID: u64 = 0x00ff_ffff_ffff_ffff;
/// The most memory planes a modifier can have.
pub const MAX_MEMORY_PLANES: usize = 4;

/// The vendor a modifier belongs to: its top byte.
pub const fn vendor_of(modifier: u64) -> u8 {
    (modifier >> 56) as u8
}

/// Bytes V3D reads past the end of the last plane.
const V3D_SLACK: u64 = 256;
/// NVIDIA lays each NV12 plane out in 64 KiB.
const NVIDIA_NV12_PLANE: u64 = 64 << 10;

/// The device an image is imported on, as far as its memory rules go.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Importer {
    pub family: Family,
    pub vendor: Option<u32>,
}

impl Importer {
    /// Whether the device can't import this layout at all, and why. V3D doesn't sample a linear
    /// image, so a linear dma-buf has to be copied there.
    pub fn refuses(&self, described: &Described<'_>) -> Option<&'static str> {
        (self.family == Family::VideoCore && described.is_linear())
            .then_some("V3D doesn't sample a linear image: copy it into a texture instead")
    }

    /// The least each plane's memory has to hold on this device, which can be more than the
    /// layout's own end: the driver's padding is added. The import refuses memory shorter than
    /// this and says the size. It never reads less than the driver asks for.
    pub fn required_len(&self, fourcc: Fourcc, layout: &Layout) -> Vec<u64> {
        let mut needed = layout.min_len.clone();
        if self.family == Family::VideoCore
            && let Some(last) = needed.last_mut()
        {
            *last = last.saturating_add(V3D_SLACK);
        }
        if self.vendor == Some(vendor_ids::NVIDIA) && fourcc == Fourcc::NV12 {
            for plane in &mut needed {
                *plane = (*plane).max(NVIDIA_NV12_PLANE);
            }
        }
        needed
    }

    /// Checks each plane's memory against what the device needs.
    pub fn check_len(
        &self,
        fourcc: Fourcc,
        layout: &Layout,
        have: &[u64],
    ) -> Result<(), ImportError> {
        let needed = self.required_len(fourcc, layout);
        for (index, (&need, &has)) in needed.iter().zip(have).enumerate() {
            if has < need {
                return Err(ImportError::invalid(format!(
                    "plane {index}: the memory is {has} bytes, and this device needs {need}"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Importer, MOD_INVALID, MOD_LINEAR, vendor_of};
    use crate::dmabuf::layout::{Described, Fourcc};
    use crate::profile::{Family, vendor_ids};

    const V3D: Importer = Importer {
        family: Family::VideoCore,
        vendor: Some(vendor_ids::BROADCOM),
    };
    const NVIDIA: Importer = Importer {
        family: Family::Desktop,
        vendor: Some(vendor_ids::NVIDIA),
    };
    const AMD: Importer = Importer {
        family: Family::Desktop,
        vendor: Some(vendor_ids::AMD),
    };

    fn described(fourcc: Fourcc, modifier: Option<u64>, planes: &[(u64, u64)]) -> Described<'_> {
        Described {
            width: 64,
            height: 4,
            fourcc,
            modifier,
            planes,
        }
    }

    #[test]
    fn modifiers_name_their_vendor() {
        assert_eq!(vendor_of(0x0700_0000_0000_0006), 7, "Broadcom");
        assert_eq!(vendor_of(0x0200_0000_2096_bb03), 2, "AMD");
        assert_eq!((vendor_of(MOD_LINEAR), vendor_of(MOD_INVALID)), (0, 0));
    }

    #[test]
    fn each_driver_asks_for_its_own_padding_and_none_is_clamped() {
        let xrgb = described(Fourcc::XRGB8888, Some(0x0700_0000_0000_0006), &[(0, 256)]);
        let layout = xrgb.validate().expect("valid");
        // A tiled plane is its offset here: the device checks the rest. V3D adds its slack.
        assert_eq!(AMD.required_len(Fourcc::XRGB8888, &layout), [0]);
        assert_eq!(V3D.required_len(Fourcc::XRGB8888, &layout), [256]);

        let linear = described(Fourcc::XRGB8888, None, &[(0, 256)]);
        let layout = linear.validate().expect("valid");
        assert_eq!(AMD.required_len(Fourcc::XRGB8888, &layout), [1024]);
        AMD.check_len(Fourcc::XRGB8888, &layout, &[1024])
            .expect("exactly enough");
        let short = AMD
            .check_len(Fourcc::XRGB8888, &layout, &[1023])
            .unwrap_err();
        assert_eq!(
            short.to_string(),
            "plane 0: the memory is 1023 bytes, and this device needs 1024"
        );

        // NVIDIA wants 64 KiB a plane for NV12, which is 128 KiB for the image.
        let nv12 = described(Fourcc::NV12, None, &[(0, 64), (256, 64)]);
        let layout = nv12.validate().expect("valid");
        assert_eq!(AMD.required_len(Fourcc::NV12, &layout), [256, 384]);
        assert_eq!(NVIDIA.required_len(Fourcc::NV12, &layout), [65536, 65536]);
        let short = NVIDIA
            .check_len(Fourcc::NV12, &layout, &[65536, 4096])
            .unwrap_err();
        assert!(
            short
                .to_string()
                .contains("plane 1: the memory is 4096 bytes, and this device needs 65536")
        );
    }

    #[test]
    fn v3d_refuses_a_linear_image_with_the_reason() {
        let linear = described(Fourcc::XRGB8888, None, &[(0, 256)]);
        assert!(
            V3D.refuses(&linear)
                .is_some_and(|why| why.contains("doesn't sample a linear image"))
        );
        let explicit = described(Fourcc::XRGB8888, Some(MOD_LINEAR), &[(0, 256)]);
        assert!(V3D.refuses(&explicit).is_some());
        let tiled = described(Fourcc::XRGB8888, Some(0x0700_0000_0000_0006), &[(0, 256)]);
        assert_eq!(V3D.refuses(&tiled), None);
        assert_eq!(AMD.refuses(&linear), None);
    }
}

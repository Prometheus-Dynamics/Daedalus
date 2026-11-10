//! What an external image is made of, and the checks on its layout that are the same on every
//! device: the fourcc, the planes, the strides.

use std::fmt;

use super::modifiers::{MAX_MEMORY_PLANES, MOD_INVALID, MOD_LINEAR};

/// A DRM fourcc, as in `drm_fourcc.h`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Fourcc(pub u32);

impl Fourcc {
    pub const fn from_bytes(code: [u8; 4]) -> Fourcc {
        Fourcc(u32::from_le_bytes(code))
    }

    /// One channel, 8 bits.
    pub const R8: Fourcc = Fourcc::from_bytes(*b"R8  ");
    /// Two channels, 8 bits each: R then G in memory.
    pub const GR88: Fourcc = Fourcc::from_bytes(*b"GR88");
    /// B, G, R, X in memory.
    pub const XRGB8888: Fourcc = Fourcc::from_bytes(*b"XR24");
    /// B, G, R, A in memory.
    pub const ARGB8888: Fourcc = Fourcc::from_bytes(*b"AR24");
    /// R, G, B, X in memory.
    pub const XBGR8888: Fourcc = Fourcc::from_bytes(*b"XB24");
    /// R, G, B, A in memory.
    pub const ABGR8888: Fourcc = Fourcc::from_bytes(*b"AB24");
    /// A full-size Y plane, then a half-size plane of interleaved U and V.
    pub const NV12: Fourcc = Fourcc::from_bytes(*b"NV12");
    /// Three planes: Y, then quarter-size U and V.
    pub const YUV420: Fourcc = Fourcc::from_bytes(*b"YU12");

    pub const fn to_bytes(self) -> [u8; 4] {
        self.0.to_le_bytes()
    }

    /// What this fourcc imports as. `None` for one this backend doesn't import as one image.
    pub const fn format(self) -> Option<ImageFormat> {
        Some(match self {
            Fourcc::R8 => ImageFormat::R8,
            Fourcc::GR88 => ImageFormat::Rg8,
            Fourcc::XRGB8888 | Fourcc::ARGB8888 => ImageFormat::Bgra8,
            Fourcc::XBGR8888 | Fourcc::ABGR8888 => ImageFormat::Rgba8,
            Fourcc::NV12 => ImageFormat::Nv12,
            _ => return None,
        })
    }
}

impl fmt::Debug for Fourcc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for Fourcc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.to_bytes();
        if bytes
            .iter()
            .all(|byte| byte.is_ascii_graphic() || *byte == b' ')
        {
            let text: String = bytes.iter().map(|&byte| byte as char).collect();
            write!(f, "{}", text.trim_end())
        } else {
            write!(f, "0x{:08x}", self.0)
        }
    }
}

/// What an imported image is on the device.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ImageFormat {
    R8,
    Rg8,
    Rgba8,
    Bgra8,
    /// Two planes in one image, which can only be sampled: Y as one channel, and U and V as two
    /// at half the size each way. Turning it into RGB is the shader's work.
    Nv12,
}

/// One plane of a format.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlaneFormat {
    pub bytes_per_texel: u32,
    /// How much smaller than the image it is, each way.
    pub subsampling: u32,
}

impl ImageFormat {
    /// The planes of the format, in memory order.
    pub const fn planes(self) -> &'static [PlaneFormat] {
        const fn plane(bytes_per_texel: u32, subsampling: u32) -> PlaneFormat {
            PlaneFormat {
                bytes_per_texel,
                subsampling,
            }
        }
        const ONE: &[PlaneFormat] = &[plane(1, 1)];
        const TWO: &[PlaneFormat] = &[plane(2, 1)];
        const FOUR: &[PlaneFormat] = &[plane(4, 1)];
        const NV12: &[PlaneFormat] = &[plane(1, 1), plane(2, 2)];
        match self {
            ImageFormat::R8 => ONE,
            ImageFormat::Rg8 => TWO,
            ImageFormat::Rgba8 | ImageFormat::Bgra8 => FOUR,
            ImageFormat::Nv12 => NV12,
        }
    }

    pub const fn is_planar(self) -> bool {
        self.planes().len() > 1
    }
}

/// Why an image wasn't imported.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ImportError {
    /// The description doesn't hold together. The text says where.
    Invalid(String),
    /// The format or its modifier isn't one this device imports. The text is the reason, the
    /// driver's own where it gave one.
    UnsupportedFormat {
        fourcc: Fourcc,
        modifier: Option<u64>,
        reason: String,
    },
    /// This device, or this machine, can't import at all.
    Unsupported(String),
    /// The device refused the memory. The text is the driver's reason.
    Refused(String),
    /// The producer's fence didn't signal in time.
    FenceTimeout(std::time::Duration),
}

impl ImportError {
    pub(crate) fn invalid(text: impl Into<String>) -> ImportError {
        ImportError::Invalid(text.into())
    }
}

impl fmt::Display for ImportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ImportError::Invalid(text)
            | ImportError::Unsupported(text)
            | ImportError::Refused(text) => f.write_str(text),
            ImportError::UnsupportedFormat {
                fourcc,
                modifier,
                reason,
            } => match modifier {
                Some(modifier) => write!(f, "{fourcc} with modifier 0x{modifier:x}: {reason}"),
                None => write!(f, "{fourcc}: {reason}"),
            },
            ImportError::FenceTimeout(after) => {
                write!(f, "the producer's fence didn't signal within {after:?}")
            }
        }
    }
}

impl std::error::Error for ImportError {}

/// The layout of an image, with no memory attached: what the pure checks look at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Described<'a> {
    pub width: u32,
    pub height: u32,
    pub fourcc: Fourcc,
    /// `None` is the implicit layout, which is linear.
    pub modifier: Option<u64>,
    /// Each memory plane's offset and stride, in order.
    pub planes: &'a [(u64, u64)],
}

/// What [`Described::validate`] found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Layout {
    pub format: ImageFormat,
    /// The least each plane's memory has to hold: the end of its last row for a linear layout,
    /// and just its offset for a tiled or auxiliary plane, which the device checks itself.
    pub min_len: Vec<u64>,
}

impl Described<'_> {
    pub fn is_linear(&self) -> bool {
        self.modifier.is_none_or(|modifier| modifier == MOD_LINEAR)
    }

    /// The checks every device shares: a real extent, a fourcc that imports as one image, a
    /// usable modifier, the right number of planes, and for a linear layout rows that fit their
    /// stride. A linear layout has one memory plane per plane of the format. Any other modifier
    /// sets its own count, from one to four: a tiled layout can keep both NV12 planes in one, or
    /// add planes of compression data. Those are left to the device, which knows the modifier.
    pub fn validate(&self) -> Result<Layout, ImportError> {
        if self.width == 0 || self.height == 0 {
            return Err(ImportError::invalid(format!(
                "an image of {}x{} has no pixels",
                self.width, self.height
            )));
        }
        if self.modifier == Some(MOD_INVALID) {
            return Err(ImportError::invalid(
                "DRM_FORMAT_MOD_INVALID names no layout to import",
            ));
        }
        let Some(format) = self.fourcc.format() else {
            let reason = if self.fourcc == Fourcc::YUV420 {
                "three-plane YUV isn't imported as one image: import each plane as R8, with its own offset and stride"
            } else {
                "no image format for this fourcc"
            };
            return Err(ImportError::UnsupportedFormat {
                fourcc: self.fourcc,
                modifier: self.modifier,
                reason: reason.into(),
            });
        };
        let formats = format.planes();
        let linear = self.is_linear();
        let allowed = if linear {
            formats.len()..=formats.len()
        } else {
            1..=MAX_MEMORY_PLANES
        };
        if !allowed.contains(&self.planes.len()) {
            let expected = if linear {
                format!("exactly {}", formats.len())
            } else {
                format!("1 to {MAX_MEMORY_PLANES}")
            };
            return Err(ImportError::invalid(format!(
                "{} takes {expected} plane(s), and {} were given",
                self.fourcc,
                self.planes.len()
            )));
        }
        for plane in formats {
            let by = plane.subsampling;
            if !self.width.is_multiple_of(by) || !self.height.is_multiple_of(by) {
                return Err(ImportError::invalid(format!(
                    "{} needs a width and height divisible by {by}, not {}x{}",
                    self.fourcc, self.width, self.height
                )));
            }
        }
        let mut min_len: Vec<u64> = self.planes.iter().map(|&(offset, _)| offset).collect();
        if linear {
            for (index, (plane, &(offset, stride))) in formats.iter().zip(self.planes).enumerate() {
                let (width, height) = (
                    self.width / plane.subsampling,
                    self.height / plane.subsampling,
                );
                let row = u64::from(width) * u64::from(plane.bytes_per_texel);
                if stride < row {
                    return Err(ImportError::invalid(format!(
                        "plane {index}: a stride of {stride} is shorter than a row of {width} texels of {} ({row} bytes)",
                        self.fourcc
                    )));
                }
                let end = stride
                    .checked_mul(u64::from(height - 1))
                    .and_then(|rows| rows.checked_add(row))
                    .and_then(|bytes| bytes.checked_add(offset));
                min_len[index] = end.ok_or_else(|| {
                    ImportError::invalid(format!(
                        "plane {index}: the layout doesn't fit in 64 bits"
                    ))
                })?;
            }
        }
        Ok(Layout { format, min_len })
    }
}

#[cfg(test)]
mod tests {
    use super::{Described, Fourcc, ImageFormat, ImportError};
    use crate::dmabuf::modifiers::{MOD_INVALID, MOD_LINEAR};

    fn xrgb(width: u32, height: u32, planes: &[(u64, u64)]) -> Described<'_> {
        Described {
            width,
            height,
            fourcc: Fourcc::XRGB8888,
            modifier: None,
            planes,
        }
    }

    fn nv12(width: u32, height: u32, planes: &[(u64, u64)]) -> Described<'_> {
        Described {
            fourcc: Fourcc::NV12,
            ..xrgb(width, height, planes)
        }
    }

    #[test]
    fn fourcc_codes_match_drm_fourcc_h() {
        // fourcc_code(a, b, c, d) is a | b << 8 | c << 16 | d << 24.
        let code = |a: u8, b: u8, c: u8, d: u8| {
            u32::from(a) | u32::from(b) << 8 | u32::from(c) << 16 | u32::from(d) << 24
        };
        assert_eq!(Fourcc::R8.0, code(b'R', b'8', b' ', b' '));
        assert_eq!(Fourcc::GR88.0, code(b'G', b'R', b'8', b'8'));
        assert_eq!(Fourcc::XRGB8888.0, code(b'X', b'R', b'2', b'4'));
        assert_eq!(Fourcc::ARGB8888.0, code(b'A', b'R', b'2', b'4'));
        assert_eq!(Fourcc::XBGR8888.0, code(b'X', b'B', b'2', b'4'));
        assert_eq!(Fourcc::ABGR8888.0, code(b'A', b'B', b'2', b'4'));
        assert_eq!(Fourcc::NV12.0, code(b'N', b'V', b'1', b'2'));
        assert_eq!(Fourcc::YUV420.0, code(b'Y', b'U', b'1', b'2'));
        assert_eq!(Fourcc::NV12.0, 0x3231_564e);
        assert_eq!(
            (Fourcc::R8.to_string(), Fourcc(1).to_string()),
            ("R8".into(), "0x00000001".into())
        );
        assert_eq!(Fourcc::XRGB8888.format(), Some(ImageFormat::Bgra8));
        assert_eq!(Fourcc::ABGR8888.format(), Some(ImageFormat::Rgba8));
    }

    #[test]
    fn validate_accepts_padded_stride_and_reports_min_len() {
        let layout = xrgb(64, 4, &[(0, 512)]).validate().expect("valid");
        assert_eq!(layout.format, ImageFormat::Bgra8);
        assert_eq!(
            layout.min_len,
            [512 * 3 + 256],
            "three padded rows and the last row's pixels"
        );
        let offset = xrgb(64, 4, &[(4096, 256)]).validate().expect("valid");
        assert_eq!(offset.min_len, [4096 + 1024]);
    }

    #[test]
    fn validate_nv12_plane_layout() {
        // Y at 0, and UV after it at half the height. Both planes are 64 bytes a row.
        let layout = nv12(64, 4, &[(0, 64), (256, 64)])
            .validate()
            .expect("valid");
        assert_eq!(layout.format, ImageFormat::Nv12);
        assert!(layout.format.is_planar());
        assert_eq!(layout.min_len, [64 * 4, 256 + 64 * 2]);
        let short = nv12(64, 4, &[(0, 64), (256, 32)]).validate().unwrap_err();
        assert!(
            short.to_string().contains("plane 1: a stride of 32"),
            "{short}"
        );
        let odd = nv12(64, 3, &[(0, 64), (192, 64)]).validate().unwrap_err();
        assert!(odd.to_string().contains("divisible by 2"), "{odd}");
        let one = nv12(64, 4, &[(0, 64)]).validate().unwrap_err();
        assert!(one.to_string().contains("exactly 2 plane(s)"), "{one}");
    }

    #[test]
    fn validate_tiled_modifiers_take_aux_planes() {
        const AMD_DCC: u64 = 0x0200_0000_2096_bb03;
        let with = |planes: &'static [(u64, u64)]| Described {
            modifier: Some(AMD_DCC),
            ..xrgb(64, 4, planes)
        };
        // The surface and its planes of compression data, passed through as given.
        let layout = with(&[(0, 256), (4096, 64), (8192, 64)])
            .validate()
            .expect("valid");
        assert_eq!(layout.min_len, [0, 4096, 8192]);
        assert!(!layout.format.is_planar());
        // A tiled pitch isn't a row length, so it isn't checked as one here.
        with(&[(0, 16)]).validate().expect("valid");
        let many = with(&[(0, 256), (1, 1), (2, 1), (3, 1), (4, 1)])
            .validate()
            .unwrap_err();
        assert!(many.to_string().contains("1 to 4 plane(s)"), "{many}");
        // A linear layout has no extra planes.
        let linear = Described {
            modifier: Some(MOD_LINEAR),
            ..with(&[(0, 256), (4096, 64)])
        };
        let error = linear.validate().unwrap_err();
        assert!(error.to_string().contains("exactly 1 plane(s)"), "{error}");
    }

    #[test]
    fn validate_tiled_nv12_takes_one_memory_plane() {
        // V3D's tiled NV12 keeps both planes of the format in one memory plane.
        const BROADCOM_UIF: u64 = 0x0700_0000_0000_0006;
        let tiled = |height| Described {
            modifier: Some(BROADCOM_UIF),
            ..nv12(64, height, &[(0, 64)])
        };
        assert!(tiled(4).validate().expect("valid").format.is_planar());
        // The format's subsampling still applies.
        let odd = tiled(3).validate().unwrap_err();
        assert!(odd.to_string().contains("divisible by 2"), "{odd}");
    }

    #[test]
    fn validate_rejects_bad_descriptors() {
        let invalid = |described: Described<'_>, text: &str| {
            let error = described.validate().unwrap_err();
            assert!(matches!(error, ImportError::Invalid(_)), "{error:?}");
            assert!(error.to_string().contains(text), "{error}");
        };
        invalid(xrgb(0, 4, &[(0, 256)]), "has no pixels");
        invalid(xrgb(64, 0, &[(0, 256)]), "has no pixels");
        invalid(
            xrgb(64, 4, &[(0, 255)]),
            "a stride of 255 is shorter than a row of 64 texels",
        );
        invalid(xrgb(64, 4, &[]), "exactly 1 plane(s)");
        invalid(
            xrgb(64, 4, &[(u64::MAX - 8, 256)]),
            "doesn't fit in 64 bits",
        );
        invalid(
            Described {
                modifier: Some(MOD_INVALID),
                ..xrgb(64, 4, &[(0, 256)])
            },
            "DRM_FORMAT_MOD_INVALID",
        );
        // Three-plane YUV says what to do instead, and an unknown fourcc is named.
        let yuv = Described {
            fourcc: Fourcc::YUV420,
            ..xrgb(64, 4, &[(0, 64)])
        };
        let error = yuv.validate().unwrap_err();
        assert!(matches!(
            &error,
            ImportError::UnsupportedFormat {
                fourcc: Fourcc::YUV420,
                ..
            }
        ));
        assert!(
            error.to_string().contains("import each plane as R8"),
            "{error}"
        );
        let unknown = Described {
            fourcc: Fourcc::from_bytes(*b"BA81"),
            ..xrgb(64, 4, &[(0, 64)])
        };
        assert_eq!(
            unknown.validate().unwrap_err().to_string(),
            "BA81: no image format for this fourcc"
        );
    }
}

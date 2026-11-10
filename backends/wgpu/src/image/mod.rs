//! Images that arrive as bytes in a buffer, made into textures a shader can sample.
//!
//! A camera's image is rows of pixels in a dma-buf. Imported as a storage buffer it costs
//! nothing, and a copy on the device then turns it into a texture. That copy takes rows that
//! start 256 bytes apart, so an image whose rows don't is folded: several rows are laid side by
//! side in a wider texture by one copy, and a small pass puts them back.

mod fold;
#[cfg(target_os = "linux")]
mod storage;
mod texture_copy;

pub use fold::{Plan, plan, source_of};
#[cfg(target_os = "linux")]
pub use storage::{ExternalBuffer, ImportedStorage};
pub use texture_copy::{ImageCopy, ImageError};

#[cfg(target_os = "linux")]
pub(crate) use storage::StorageImports;

use crate::dmabuf::Fourcc;

/// What a fourcc's bytes are, as far as the device is concerned.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ImageKind {
    /// Pixels a shader can read as they are.
    Pixels,
    /// A sensor's raw mosaic: one colour sample per pixel, to be demosaiced first.
    Bayer,
    /// A compressed stream, to be decoded first.
    Compressed,
}

impl ImageKind {
    /// Which kind a fourcc is. One this table doesn't know is taken as pixels: the import that
    /// follows checks the format itself.
    pub fn of(fourcc: Fourcc) -> ImageKind {
        const BAYER: [&[u8; 4]; 20] = [
            b"BA81", b"GBRG", b"GRBG", b"RGGB", b"BG10", b"GB10", b"BA10", b"RG10", b"pBAA",
            b"pGAA", b"pgAA", b"pRAA", b"BG12", b"GB12", b"BA12", b"RG12", b"pBCC", b"pGCC",
            b"pgCC", b"pRCC",
        ];
        const COMPRESSED: [&[u8; 4]; 9] = [
            b"MJPG", b"JPEG", b"H264", b"HEVC", b"VP80", b"VP90", b"AV01", b"MPG2", b"MPG4",
        ];
        let code = fourcc.to_bytes();
        if BAYER.iter().any(|known| **known == code) {
            ImageKind::Bayer
        } else if COMPRESSED.iter().any(|known| **known == code) {
            ImageKind::Compressed
        } else {
            ImageKind::Pixels
        }
    }

    /// Why bytes of this kind aren't imported as an image, or `None` when they are.
    pub fn refusal(self) -> Option<&'static str> {
        match self {
            ImageKind::Pixels => None,
            ImageKind::Bayer => {
                Some("a Bayer mosaic isn't an image on the device: demosaic it first")
            }
            ImageKind::Compressed => {
                Some("a compressed stream isn't an image on the device: decode it first")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ImageKind;
    use crate::dmabuf::Fourcc;

    #[test]
    fn bayer_and_compressed_images_are_refused_and_pixels_are_not() {
        let kind = |code: &[u8; 4]| ImageKind::of(Fourcc::from_bytes(*code));
        for raw in [b"BA81", b"RGGB", b"pBAA", b"RG12", b"pgCC"] {
            assert_eq!(kind(raw), ImageKind::Bayer, "{}", Fourcc::from_bytes(*raw));
        }
        for stream in [b"MJPG", b"H264", b"HEVC", b"AV01"] {
            assert_eq!(
                kind(stream),
                ImageKind::Compressed,
                "{}",
                Fourcc::from_bytes(*stream)
            );
        }
        for pixels in [
            Fourcc::R8,
            Fourcc::XRGB8888,
            Fourcc::NV12,
            Fourcc::from_bytes(*b"GREY"),
        ] {
            assert_eq!(ImageKind::of(pixels), ImageKind::Pixels, "{pixels}");
            assert_eq!(ImageKind::of(pixels).refusal(), None);
        }
        assert!(
            ImageKind::Bayer
                .refusal()
                .is_some_and(|why| why.contains("demosaic"))
        );
        assert!(
            ImageKind::Compressed
                .refusal()
                .is_some_and(|why| why.contains("decode"))
        );
    }
}

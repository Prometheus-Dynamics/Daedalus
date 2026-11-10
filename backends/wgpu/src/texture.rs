//! Textures of the formats this backend supports, made at setup.

use std::fmt;
use std::sync::Arc;

use crate::WgpuBackend;
use crate::context::{Context, OpenError};

/// A texture format this backend supports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Format {
    R8,
    Rgba8,
    R32F,
    Rg32F,
    R8Uint,
    Rgba16Uint,
    Rg32Uint,
}

impl Format {
    pub const ALL: [Format; 7] = [
        Format::R8,
        Format::Rgba8,
        Format::R32F,
        Format::Rg32F,
        Format::R8Uint,
        Format::Rgba16Uint,
        Format::Rg32Uint,
    ];

    pub const fn bytes_per_pixel(self) -> u32 {
        match self {
            Format::R8 | Format::R8Uint => 1,
            Format::Rgba8 | Format::R32F => 4,
            Format::Rg32F | Format::Rgba16Uint | Format::Rg32Uint => 8,
        }
    }

    /// Whether WebGPU lets a compute pass write this format as a storage texture.
    pub const fn storage(self) -> bool {
        !matches!(self, Format::R8 | Format::R8Uint)
    }

    pub(crate) const fn to_wgpu(self) -> wgpu::TextureFormat {
        match self {
            Format::R8 => wgpu::TextureFormat::R8Unorm,
            Format::Rgba8 => wgpu::TextureFormat::Rgba8Unorm,
            Format::R32F => wgpu::TextureFormat::R32Float,
            Format::Rg32F => wgpu::TextureFormat::Rg32Float,
            Format::R8Uint => wgpu::TextureFormat::R8Uint,
            Format::Rgba16Uint => wgpu::TextureFormat::Rgba16Uint,
            Format::Rg32Uint => wgpu::TextureFormat::Rg32Uint,
        }
    }
}

/// The size and format of a texture.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TextureDesc {
    pub format: Format,
    pub width: u32,
    pub height: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TextureError {
    Open(OpenError),
    /// A side is zero or over the device's limit.
    Size {
        width: u32,
        height: u32,
        max: u32,
    },
    /// A row is shorter than the pixels in it, or isn't a whole number of pixels.
    RowPitch {
        bytes_per_row: u32,
        least: u32,
    },
    /// The bytes given aren't `needed` long.
    Length {
        len: usize,
        needed: u64,
    },
}

impl fmt::Display for TextureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TextureError::Open(error) => error.fmt(f),
            TextureError::Size { width, height, max } => {
                write!(
                    f,
                    "a {width}x{height} texture, where each side is 1 to {max}"
                )
            }
            TextureError::RowPitch {
                bytes_per_row,
                least,
            } => write!(
                f,
                "a row pitch of {bytes_per_row} bytes, where a row is at least {least} and a whole number of pixels"
            ),
            TextureError::Length { len, needed } => {
                write!(f, "{len} bytes, where the texture needs {needed}")
            }
        }
    }
}

impl std::error::Error for TextureError {}

impl TextureDesc {
    /// Checks a texture of this description against the device's largest side.
    pub fn validate(&self, max_dimension: u32) -> Result<(), TextureError> {
        let sides = [self.width, self.height];
        if sides.iter().any(|&side| side == 0 || side > max_dimension) {
            return Err(TextureError::Size {
                width: self.width,
                height: self.height,
                max: max_dimension,
            });
        }
        Ok(())
    }

    /// Checks `len` bytes laid out in rows of `bytes_per_row` against this description. The last
    /// row may stop at its last pixel.
    pub fn validate_bytes(&self, bytes_per_row: u32, len: usize) -> Result<(), TextureError> {
        let pixel = self.format.bytes_per_pixel();
        let least = self.width.saturating_mul(pixel);
        if bytes_per_row < least || !bytes_per_row.is_multiple_of(pixel) {
            return Err(TextureError::RowPitch {
                bytes_per_row,
                least,
            });
        }
        let rows = u64::from(self.height.saturating_sub(1));
        let needed = rows * u64::from(bytes_per_row) + u64::from(least);
        if len as u64 != needed {
            return Err(TextureError::Length { len, needed });
        }
        Ok(())
    }
}

/// A texture on the backend's device. Made at setup, never while a region runs.
pub struct Texture {
    #[expect(dead_code, reason = "bound by shader nodes, from E4")]
    pub(crate) texture: wgpu::Texture,
    desc: TextureDesc,
    context: Arc<Context>,
}

impl Texture {
    /// A texture that can be sampled and copied, and written by compute where the format allows.
    pub fn new(backend: &WgpuBackend, desc: TextureDesc) -> Result<Texture, TextureError> {
        let context = backend.context().map_err(TextureError::Open)?;
        desc.validate(context.limits.max_texture_dimension_2d)?;
        let mut usage = wgpu::TextureUsages::TEXTURE_BINDING
            | wgpu::TextureUsages::COPY_SRC
            | wgpu::TextureUsages::COPY_DST;
        if desc.format.storage() {
            usage |= wgpu::TextureUsages::STORAGE_BINDING;
        }
        let texture = context.device.create_texture(&wgpu::TextureDescriptor {
            label: None,
            size: wgpu::Extent3d {
                width: desc.width,
                height: desc.height,
                depth_or_array_layers: 1,
            },
            mip_level_count: 1,
            sample_count: 1,
            dimension: wgpu::TextureDimension::D2,
            format: desc.format.to_wgpu(),
            usage,
            view_formats: &[],
        });
        Ok(Texture {
            texture,
            desc,
            context,
        })
    }

    pub fn desc(&self) -> TextureDesc {
        self.desc
    }

    /// Which device this is on. Two values with one key share a device.
    pub fn device_key(&self) -> u64 {
        self.context.key
    }
}

#[cfg(test)]
mod tests {
    use super::{Format, TextureDesc, TextureError};

    #[test]
    fn every_format_has_a_size_and_a_mapping() {
        for format in Format::ALL {
            let block = format.to_wgpu().block_copy_size(None);
            assert_eq!(block, Some(format.bytes_per_pixel()), "{format:?}");
        }
    }

    #[test]
    fn rejects_oversize_buffer() {
        let desc = TextureDesc {
            format: Format::Rgba8,
            width: 4,
            height: 3,
        };
        assert_eq!(desc.validate_bytes(16, 48), Ok(()));
        assert_eq!(
            desc.validate_bytes(32, 80),
            Ok(()),
            "padded rows, short last row"
        );
        assert_eq!(
            desc.validate_bytes(16, 49),
            Err(TextureError::Length {
                len: 49,
                needed: 48
            })
        );
        assert_eq!(
            desc.validate_bytes(16, 32),
            Err(TextureError::Length {
                len: 32,
                needed: 48
            })
        );
        let big = TextureDesc {
            width: 9000,
            ..desc
        };
        assert!(matches!(
            big.validate(8192),
            Err(TextureError::Size { max: 8192, .. })
        ));
        let empty = TextureDesc { height: 0, ..desc };
        assert!(empty.validate(8192).is_err());
        assert_eq!(desc.validate(8192), Ok(()));
    }

    #[test]
    fn rejects_unaligned_row_pitch() {
        let desc = TextureDesc {
            format: Format::Rg32F,
            width: 4,
            height: 2,
        };
        assert_eq!(desc.validate_bytes(32, 64), Ok(()));
        let short = desc.validate_bytes(24, 48);
        assert_eq!(
            short,
            Err(TextureError::RowPitch {
                bytes_per_row: 24,
                least: 32
            })
        );
        let ragged = desc.validate_bytes(36, 68);
        assert_eq!(
            ragged,
            Err(TextureError::RowPitch {
                bytes_per_row: 36,
                least: 32
            })
        );
    }
}

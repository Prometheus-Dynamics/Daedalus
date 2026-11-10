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
    /// The device lost the readback, or didn't finish it in time.
    Device(String),
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
            TextureError::Device(reason) => f.write_str(reason),
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
    pub(crate) texture: wgpu::Texture,
    pub(crate) view: wgpu::TextureView,
    /// Unique to this texture, for the bind group cache.
    pub(crate) id: u64,
    desc: TextureDesc,
    context: Arc<Context>,
}

impl fmt::Debug for Texture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Texture")
            .field("desc", &self.desc)
            .finish_non_exhaustive()
    }
}

impl Texture {
    /// A texture that can be sampled, copied and drawn to, and written by compute where the
    /// format allows.
    pub fn new(backend: &WgpuBackend, desc: TextureDesc) -> Result<Texture, TextureError> {
        let context = backend.context().map_err(TextureError::Open)?;
        desc.validate(context.limits.max_texture_dimension_2d)?;
        let mut usage = wgpu::TextureUsages::TEXTURE_BINDING
            | wgpu::TextureUsages::RENDER_ATTACHMENT
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
            view: texture.create_view(&wgpu::TextureViewDescriptor::default()),
            texture,
            id: Context::next_id(),
            desc,
            context,
        })
    }

    pub fn desc(&self) -> TextureDesc {
        self.desc
    }

    fn extent(&self) -> wgpu::Extent3d {
        wgpu::Extent3d {
            width: self.desc.width,
            height: self.desc.height,
            depth_or_array_layers: 1,
        }
    }

    /// Uploads the whole texture from `bytes`, laid out in rows of `bytes_per_row`. The last row
    /// may stop at its last pixel. The bytes travel with the next submit.
    pub fn write(&self, bytes: &[u8], bytes_per_row: u32) -> Result<(), TextureError> {
        self.desc.validate_bytes(bytes_per_row, bytes.len())?;
        self.context.queue.write_texture(
            self.texture.as_image_copy(),
            bytes,
            wgpu::TexelCopyBufferLayout {
                offset: 0,
                bytes_per_row: Some(bytes_per_row),
                rows_per_image: Some(self.desc.height),
            },
            self.extent(),
        );
        self.context.moved.upload(bytes.len() as u64);
        Ok(())
    }

    /// Reads the whole texture into `out`, as rows with no padding. This waits for the copy, and
    /// for the device's earlier work: it is for setup and tests, and nothing in a region calls it.
    pub fn read_into(&self, out: &mut [u8]) -> Result<(), TextureError> {
        let context = &self.context;
        let row = self.desc.width * self.desc.format.bytes_per_pixel();
        let needed = u64::from(row) * u64::from(self.desc.height);
        if out.len() as u64 != needed {
            return Err(TextureError::Length {
                len: out.len(),
                needed,
            });
        }
        let lost = |what: &str| TextureError::Device(format!("the readback {what}"));
        // A copy into a buffer wants each row to start on a 256-byte boundary.
        let padded = row.next_multiple_of(wgpu::COPY_BYTES_PER_ROW_ALIGNMENT);
        let size = u64::from(padded) * u64::from(self.desc.height);
        let _place = context.copies.acquire();
        let staging = context.staging().take(size).unwrap_or_else(|| {
            context.device.create_buffer(&wgpu::BufferDescriptor {
                label: None,
                size,
                usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
                mapped_at_creation: false,
            })
        });
        let mut encoder = context
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor::default());
        encoder.copy_texture_to_buffer(
            self.texture.as_image_copy(),
            wgpu::TexelCopyBufferInfo {
                buffer: &staging,
                layout: wgpu::TexelCopyBufferLayout {
                    offset: 0,
                    bytes_per_row: Some(padded),
                    rows_per_image: Some(self.desc.height),
                },
            },
            self.extent(),
        );
        let mapped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&mapped);
        {
            let _held = context.submit.lock();
            context.queue.submit([encoder.finish()]);
        }
        {
            let _held = context.submit.lock();
            staging
                .slice(..size)
                .map_async(wgpu::MapMode::Read, move |result| {
                    flag.store(result.is_ok(), std::sync::atomic::Ordering::Release);
                });
        }
        context
            .wait("a readback")
            .map_err(|error| TextureError::Device(error.to_string()))?;
        if !mapped.load(std::sync::atomic::Ordering::Acquire) {
            return Err(lost("could not be mapped"));
        }
        {
            let view = staging
                .slice(..size)
                .get_mapped_range()
                .map_err(|_| lost("could not be mapped"))?;
            let rows = view
                .chunks(padded as usize)
                .zip(out.chunks_mut(row as usize));
            for (from, to) in rows {
                let pixels = from
                    .get(..to.len())
                    .ok_or_else(|| lost("came back short"))?;
                to.copy_from_slice(pixels);
            }
        }
        staging.unmap();
        let kept = staging.size();
        context.staging().put(kept, staging);
        context.moved.download(needed);
        Ok(())
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

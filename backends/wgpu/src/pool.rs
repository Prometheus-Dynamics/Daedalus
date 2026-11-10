//! Scratch buffers and textures a device keeps between runs. The pool belongs to its device, so
//! it goes when the device does.

/// How many of each a device keeps. Small on purpose: a large pool of full-size intermediates
/// can exhaust an embedded GPU.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TempPoolLimits {
    pub buffers: usize,
    pub textures: usize,
}

impl Default for TempPoolLimits {
    fn default() -> Self {
        TempPoolLimits {
            buffers: 16,
            textures: 6,
        }
    }
}

/// What a pooled texture was made as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TextureKey {
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) format: wgpu::TextureFormat,
    pub(crate) usage: wgpu::TextureUsages,
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "shader nodes take their scratch here, from E4")
)]
pub(crate) struct TempPool {
    limits: TempPoolLimits,
    buffers: Vec<wgpu::Buffer>,
    textures: Vec<(TextureKey, wgpu::Texture)>,
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "shader nodes take their scratch here, from E4")
)]
impl TempPool {
    pub(crate) fn new(limits: TempPoolLimits) -> Self {
        let limits = TempPoolLimits {
            buffers: limits.buffers.max(1),
            textures: limits.textures.max(1),
        };
        TempPool {
            buffers: Vec::with_capacity(limits.buffers),
            textures: Vec::with_capacity(limits.textures),
            limits,
        }
    }

    /// A pooled buffer of `size..=2 * size` bytes with at least `usage`.
    pub(crate) fn take_buffer(
        &mut self,
        size: u64,
        usage: wgpu::BufferUsages,
    ) -> Option<wgpu::Buffer> {
        let at = self.buffers.iter().position(|buffer| {
            (size..=size.saturating_mul(2)).contains(&buffer.size())
                && buffer.usage().contains(usage)
        })?;
        Some(self.buffers.swap_remove(at))
    }

    /// Keeps a buffer. At the limit, the oldest goes.
    pub(crate) fn put_buffer(&mut self, buffer: wgpu::Buffer) {
        if self.buffers.len() >= self.limits.buffers {
            self.buffers.swap_remove(0);
        }
        self.buffers.push(buffer);
    }

    /// A pooled texture of this size and format. One made with more usages serves a request for
    /// fewer.
    pub(crate) fn take_texture(&mut self, key: TextureKey) -> Option<wgpu::Texture> {
        let at = self.textures.iter().position(|(have, _)| {
            (have.width, have.height, have.format) == (key.width, key.height, key.format)
                && have.usage.contains(key.usage)
        })?;
        Some(self.textures.swap_remove(at).1)
    }

    pub(crate) fn put_texture(&mut self, key: TextureKey, texture: wgpu::Texture) {
        if self.textures.len() >= self.limits.textures {
            self.textures.swap_remove(0);
        }
        self.textures.push((key, texture));
    }

    pub(crate) fn counts(&self) -> (usize, usize) {
        (self.buffers.len(), self.textures.len())
    }
}

#[cfg(test)]
mod tests {
    use super::{TempPoolLimits, TextureKey};
    use crate::adapter::Options;
    use crate::context::Context;

    #[test]
    fn scratch_is_reused_by_size_and_usage_and_the_oldest_goes_at_the_limit() {
        let limits = TempPoolLimits {
            buffers: 2,
            textures: 0,
        };
        let Ok(context) = Context::open(&Options::default()) else {
            assert!(std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none());
            return;
        };
        let buffer = |size| {
            context.device.create_buffer(&wgpu::BufferDescriptor {
                label: None,
                size,
                usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
                mapped_at_creation: false,
            })
        };
        // A pool of this test's own: the device's is shared with every other test here.
        let mut pool = super::TempPool::new(limits);
        assert_eq!(
            context.temp.lock().expect("the device's pool").counts(),
            (0, 0)
        );
        pool.put_buffer(buffer(1024));
        assert!(
            pool.take_buffer(256, wgpu::BufferUsages::STORAGE).is_none(),
            "too large"
        );
        assert!(
            pool.take_buffer(1024, wgpu::BufferUsages::UNIFORM)
                .is_none(),
            "wrong usage"
        );
        assert!(pool.take_buffer(600, wgpu::BufferUsages::STORAGE).is_some());
        for size in [64, 128, 256] {
            pool.put_buffer(buffer(size));
        }
        assert_eq!(pool.counts(), (2, 0));
        assert!(
            pool.take_buffer(64, wgpu::BufferUsages::STORAGE)
                .is_some_and(|b| b.size() != 64)
        );

        let key = TextureKey {
            width: 8,
            height: 8,
            format: wgpu::TextureFormat::Rgba8Unorm,
            usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
        };
        let texture = |key: TextureKey| {
            context.device.create_texture(&wgpu::TextureDescriptor {
                label: None,
                size: wgpu::Extent3d {
                    width: key.width,
                    height: key.height,
                    depth_or_array_layers: 1,
                },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format: key.format,
                usage: key.usage,
                view_formats: &[],
            })
        };
        pool.put_texture(key, texture(key));
        let fewer = TextureKey {
            usage: wgpu::TextureUsages::TEXTURE_BINDING,
            ..key
        };
        let wider = TextureKey { width: 16, ..key };
        assert!(pool.take_texture(wider).is_none());
        assert!(
            pool.take_texture(fewer).is_some(),
            "more usages serve fewer"
        );
        pool.put_texture(key, texture(key));
        pool.put_texture(wider, texture(wider));
        assert_eq!(pool.counts().1, 1, "a limit of zero acts as one");
        assert!(pool.take_texture(wider).is_some());
    }
}

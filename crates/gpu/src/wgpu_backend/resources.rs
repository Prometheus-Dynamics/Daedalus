use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

use crate::handles::{GpuBufferId, GpuImageId};

#[derive(Debug, Default)]
pub(super) struct WgpuResources {
    pub(super) buffers: Mutex<HashMap<GpuBufferId, wgpu::Buffer>>,
    pub(super) textures: Mutex<HashMap<GpuImageId, Arc<wgpu::Texture>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResourceKind {
    Buffer(GpuBufferId),
    Texture {
        id: GpuImageId,
        recycle: Option<TextureRecycleMeta>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TextureRecycleMeta {
    pub(super) device_key: usize,
    pub(super) width: u32,
    pub(super) height: u32,
    pub(super) format: wgpu::TextureFormat,
    pub(super) usage: wgpu::TextureUsages,
}

#[derive(Debug)]
pub(super) struct ResourceDropToken {
    pub(super) kind: ResourceKind,
    pub(super) resources: Weak<WgpuResources>,
}

impl Drop for ResourceDropToken {
    fn drop(&mut self) {
        let Some(resources) = self.resources.upgrade() else {
            return;
        };
        match self.kind {
            ResourceKind::Buffer(id) => {
                let mut buffers = resources.buffers.lock();
                buffers.remove(&id);
            }
            ResourceKind::Texture { id, recycle } => {
                let mut textures = resources.textures.lock();
                let texture = textures.remove(&id);
                if let (Some(texture), Some(meta)) = (texture, recycle) {
                    let mut pool = crate::shader::temp_pool().lock();
                    pool.put_texture(
                        meta.device_key,
                        meta.width,
                        meta.height,
                        meta.format,
                        meta.usage,
                        texture,
                    );
                }
            }
        }
    }
}

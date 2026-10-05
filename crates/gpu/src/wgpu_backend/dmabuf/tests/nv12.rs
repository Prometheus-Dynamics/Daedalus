use std::os::fd::AsFd;

use super::support::{DmaBuf, device_poll, exclusive, import_backend, skip};
use crate::{
    DrmFourcc, ExternalFrameDescriptor, ExternalImportError, ExternalPlane, GpuBackend, GpuFormat,
    GpuImageHandle, WgpuBackend,
};

fn nv12_luma(x: u32, y: u32) -> u8 {
    (x * 3 + y * 5) as u8
}

fn nv12_chroma(x: u32, y: u32) -> [u8; 2] {
    [(x * 16 + 7) as u8, (255 - y * 16) as u8]
}

/// Write an NV12 frame: Y plane at `y_offset`, interleaved UV plane at `uv_offset`.
fn write_nv12(bytes: &mut [u8], w: u32, h: u32, stride: u64, y_offset: u64, uv_offset: u64) {
    for y in 0..h {
        for x in 0..w {
            bytes[(y_offset + u64::from(y) * stride + u64::from(x)) as usize] = nv12_luma(x, y);
        }
    }
    for y in 0..h / 2 {
        for x in 0..w / 2 {
            let at = (uv_offset + u64::from(y) * stride + u64::from(x) * 2) as usize;
            bytes[at..at + 2].copy_from_slice(&nv12_chroma(x, y));
        }
    }
}

pub(super) fn assert_nv12(backend: &WgpuBackend, handle: &GpuImageHandle) {
    assert_eq!(handle.format, GpuFormat::Nv12);
    let words = sample_nv12(backend, handle);
    for y in 0..handle.height {
        for x in 0..handle.width {
            let [u, v] = nv12_chroma(x / 2, y / 2);
            let expected = u32::from(nv12_luma(x, y)) | u32::from(u) << 8 | u32::from(v) << 16;
            let got = words[(y * handle.width + x) as usize];
            assert_eq!(
                got, expected,
                "pixel ({x},{y}): {got:06x} != {expected:06x}"
            );
        }
    }
}

#[test]
#[ignore = "needs a Vulkan GPU with dmabuf import and access to /dev/dma_heap"]
fn dmabuf_import_nv12_as_one_texture() {
    let _gpu = exclusive();
    let Some(backend) = import_backend() else {
        return;
    };
    let (width, height, stride) = (64u32, 32u32, 256u64);
    let uv_offset = stride * u64::from(height);
    let buf = DmaBuf::alloc((uv_offset * 3 / 2) as usize).expect("dma-heap alloc");
    buf.write(|bytes| write_nv12(bytes, width, height, stride, 0, uv_offset));
    let plane = |offset| ExternalPlane::from_borrowed(buf.fd.as_fd(), offset, stride).unwrap();
    let result = backend.import_dmabuf(
        ExternalFrameDescriptor::new(
            width,
            height,
            DrmFourcc::NV12,
            vec![plane(0), plane(uv_offset)],
        )
        .with_implicit_fence()
        .expect("implicit fence"),
    );
    let handle = match result {
        Ok(handle) => handle,
        Err(ExternalImportError::UnsupportedFormat { reason, .. }) => {
            skip(format!(
                "NV12 not importable on {:?}: {reason}",
                backend.adapter_info().name
            ));
            return;
        }
        Err(err) => panic!("NV12 import failed: {err}"),
    };
    assert_nv12(&backend, &handle);
    // wgpu 30 cannot copy out of NV12 textures.
    assert_eq!(
        backend.read_texture(&handle),
        Err(crate::GpuError::Unsupported)
    );

    // Planes in two separate dmabufs: a disjoint image, when the modifier allows it.
    let y_buf = DmaBuf::alloc((stride * u64::from(height)) as usize).expect("y alloc");
    let uv_buf = DmaBuf::alloc((stride * u64::from(height) / 2) as usize).expect("uv alloc");
    let mut whole = vec![0u8; (uv_offset * 3 / 2) as usize];
    write_nv12(&mut whole, width, height, stride, 0, uv_offset);
    y_buf.write(|bytes| bytes.copy_from_slice(&whole[..uv_offset as usize]));
    uv_buf.write(|bytes| bytes.copy_from_slice(&whole[uv_offset as usize..]));
    let planes = vec![
        ExternalPlane::from_borrowed(y_buf.fd.as_fd(), 0, stride).unwrap(),
        ExternalPlane::from_borrowed(uv_buf.fd.as_fd(), 0, stride).unwrap(),
    ];
    match backend.import_dmabuf(ExternalFrameDescriptor::new(
        width,
        height,
        DrmFourcc::NV12,
        planes,
    )) {
        Ok(handle) => assert_nv12(&backend, &handle),
        Err(ExternalImportError::UnsupportedFormat { reason, .. }) => {
            skip(format!("disjoint NV12: {reason}"));
        }
        Err(err) => panic!("disjoint NV12 import failed: {err}"),
    }
}

/// Samples an NV12 texture through its plane views and returns `y | u << 8 | v << 16` per pixel.
pub(super) fn sample_nv12(backend: &WgpuBackend, handle: &GpuImageHandle) -> Vec<u32> {
    let (device, queue) = backend.device_queue();
    let texture = backend.get_texture(handle).expect("registered texture");
    let [y_view, uv_view]: [wgpu::TextureView; 2] = crate::texture_plane_views(&texture)
        .try_into()
        .expect("two plane views");
    let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
        label: Some("nv12-sample"),
        source: wgpu::ShaderSource::Wgsl(
            r#"
@group(0) @binding(0) var y_tex: texture_2d<f32>;
@group(0) @binding(1) var uv_tex: texture_2d<f32>;
@group(0) @binding(2) var<storage, read_write> out: array<u32>;
@compute @workgroup_size(8, 8)
fn main(@builtin(global_invocation_id) id: vec3<u32>) {
let dims = textureDimensions(y_tex);
if (id.x >= dims.x || id.y >= dims.y) { return; }
let y = textureLoad(y_tex, vec2<i32>(id.xy), 0).r;
let uv = textureLoad(uv_tex, vec2<i32>(id.xy / 2u), 0).rg;
out[id.y * dims.x + id.x] = u32(round(y * 255.0))
    | (u32(round(uv.x * 255.0)) << 8u)
    | (u32(round(uv.y * 255.0)) << 16u);
}
"#
            .into(),
        ),
    });
    let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
        label: Some("nv12-sample"),
        layout: None,
        module: &shader,
        entry_point: Some("main"),
        compilation_options: Default::default(),
        cache: None,
    });
    let size = u64::from(handle.width * handle.height) * 4;
    let out = device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("nv12-out"),
        size,
        usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
        mapped_at_creation: false,
    });
    let readback = device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("nv12-readback"),
        size,
        usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
        mapped_at_creation: false,
    });
    let bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: Some("nv12-sample"),
        layout: &pipeline.get_bind_group_layout(0),
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: wgpu::BindingResource::TextureView(&y_view),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: wgpu::BindingResource::TextureView(&uv_view),
            },
            wgpu::BindGroupEntry {
                binding: 2,
                resource: out.as_entire_binding(),
            },
        ],
    });
    let mut encoder = device.create_command_encoder(&Default::default());
    {
        let mut pass = encoder.begin_compute_pass(&Default::default());
        pass.set_pipeline(&pipeline);
        pass.set_bind_group(0, &bind_group, &[]);
        pass.dispatch_workgroups(handle.width.div_ceil(8), handle.height.div_ceil(8), 1);
    }
    encoder.copy_buffer_to_buffer(&out, 0, &readback, 0, size);
    queue.submit(Some(encoder.finish()));
    readback
        .slice(..)
        .map_async(wgpu::MapMode::Read, |res| res.expect("map"));
    device_poll(backend);
    let view = readback.slice(..).get_mapped_range().expect("mapped range");
    let words = bytemuck::cast_slice::<u8, u32>(&view).to_vec();
    drop(view);
    readback.unmap();
    words
}

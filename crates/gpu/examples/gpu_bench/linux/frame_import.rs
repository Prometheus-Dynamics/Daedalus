//! Per-frame cost of a `LINEAR` dmabuf camera frame reaching a compute kernel.

use std::fs::File;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::time::Instant;

use ash::{khr, vk};
use daedalus_gpu::wgpu::{self, hal::api::Vulkan, hal::vulkan as hal_vk, util::DeviceExt};
use daedalus_gpu::{
    DRM_FORMAT_MOD_LINEAR, DrmFourcc, ExternalFrameDescriptor, ExternalPlane, GpuBackend,
    WgpuBackend,
};

use super::{bind, iters, kv, pipeline, run_once, stats, storage, wait};

/// `_IOWR('H', 0, struct dma_heap_allocation_data)`.
const DMA_HEAP_IOCTL_ALLOC: u64 = 0xC018_4800;
/// `_IOW('b', 0, struct dma_buf_sync)`.
const DMA_BUF_IOCTL_SYNC: u64 = 0x4008_6200;
const DMA_BUF_SYNC_RW: u64 = 3;
const DMA_BUF_SYNC_START: u64 = 0;
const DMA_BUF_SYNC_END: u64 = 4;
/// Frame sizes for the import benchmark (XRGB8888).
const FRAMES: [(u32, u32); 2] = [(640, 480), (1280, 720)];

#[repr(C)]
struct DmaHeapAllocationData {
    len: u64,
    fd: u32,
    fd_flags: u32,
    heap_flags: u64,
}

/// A CPU-mapped dma-heap buffer.
struct DmaBuf {
    fd: OwnedFd,
    len: usize,
}

impl DmaBuf {
    fn alloc(len: usize) -> std::io::Result<Self> {
        let heap =
            std::env::var("DAEDALUS_DMA_HEAP").unwrap_or_else(|_| "/dev/dma_heap/system".into());
        let heap = File::options().read(true).write(true).open(&heap)?;
        let mut data = DmaHeapAllocationData {
            len: len as u64,
            fd: 0,
            fd_flags: (libc::O_RDWR | libc::O_CLOEXEC) as u32,
            heap_flags: 0,
        };
        // SAFETY: valid heap fd and a correctly laid out argument struct.
        let rc = unsafe { libc::ioctl(heap.as_raw_fd(), DMA_HEAP_IOCTL_ALLOC as _, &mut data) };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: the kernel returned a fresh fd we now own.
        let fd = unsafe { OwnedFd::from_raw_fd(data.fd as i32) };
        Ok(Self { fd, len })
    }

    /// Fill through a CPU mapping, bracketed by `DMA_BUF_IOCTL_SYNC`.
    fn write(&self, fill: impl FnOnce(&mut [u8])) -> std::io::Result<()> {
        // SAFETY: mapping `len` bytes of a dmabuf we own.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                self.len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                self.fd.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        let sync = |flags: u64| {
            // SAFETY: valid dmabuf fd and a u64 `dma_buf_sync` argument.
            unsafe { libc::ioctl(self.fd.as_raw_fd(), DMA_BUF_IOCTL_SYNC as _, &flags) };
        };
        sync(DMA_BUF_SYNC_START | DMA_BUF_SYNC_RW);
        // SAFETY: the mapping is `len` bytes and exclusively ours here.
        fill(unsafe { std::slice::from_raw_parts_mut(ptr.cast::<u8>(), self.len) });
        sync(DMA_BUF_SYNC_END | DMA_BUF_SYNC_RW);
        // SAFETY: unmapping the mapping created above.
        unsafe { libc::munmap(ptr, self.len) };
        Ok(())
    }
}

/// Whether the driver imports dmabuf memory into a storage buffer, per
/// `vkGetPhysicalDeviceExternalBufferProperties`.
fn buffer_import_supported(hal: &hal_vk::Device) -> Result<(), String> {
    let instance = hal.shared_instance().raw_instance();
    let info = vk::PhysicalDeviceExternalBufferInfo::default()
        .usage(vk::BufferUsageFlags::STORAGE_BUFFER | vk::BufferUsageFlags::TRANSFER_SRC)
        .handle_type(vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT);
    let mut props = vk::ExternalBufferProperties::default();
    // SAFETY: valid physical device; query only.
    unsafe {
        instance.get_physical_device_external_buffer_properties(
            hal.raw_physical_device(),
            &info,
            &mut props,
        )
    };
    let features = props.external_memory_properties.external_memory_features;
    if features.contains(vk::ExternalMemoryFeatureFlags::IMPORTABLE) {
        Ok(())
    } else {
        Err(format!("not importable (features {features:?})"))
    }
}

/// Import `fd` (a dmabuf of `len` bytes) as a wgpu storage buffer through Vulkan.
fn import_buffer(device: &wgpu::Device, fd: OwnedFd, len: u64) -> Result<wgpu::Buffer, String> {
    // SAFETY: the hal device outlives this function; the buffer and memory created here go to
    // wgpu (`from_raw_managed`), which destroys and frees them when the wgpu buffer drops.
    let hal = unsafe { device.as_hal::<Vulkan>() }.ok_or("not a Vulkan device")?;
    let instance = hal.shared_instance().raw_instance();
    let raw = hal.raw_device();
    let handle_type = vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT;
    let mut external = vk::ExternalMemoryBufferCreateInfo::default().handle_types(handle_type);
    let info = vk::BufferCreateInfo::default()
        .size(len)
        .usage(
            vk::BufferUsageFlags::STORAGE_BUFFER
                | vk::BufferUsageFlags::TRANSFER_SRC
                | vk::BufferUsageFlags::TRANSFER_DST,
        )
        .sharing_mode(vk::SharingMode::EXCLUSIVE)
        .push_next(&mut external);
    // SAFETY: valid device and create info.
    let buffer = unsafe { raw.create_buffer(&info, None) }.map_err(|e| e.to_string())?;
    let destroy = |memory: Option<vk::DeviceMemory>| {
        // SAFETY: both objects were created above on `raw` and are not in use.
        unsafe {
            if let Some(memory) = memory {
                raw.free_memory(memory, None);
            }
            raw.destroy_buffer(buffer, None);
        }
    };
    // SAFETY: `buffer` is a valid buffer of `raw`.
    let req = unsafe { raw.get_buffer_memory_requirements(buffer) };
    let fd_api = khr::external_memory_fd::Device::new(instance, raw);
    let mut fd_props = vk::MemoryFdPropertiesKHR::default();
    // SAFETY: `fd` is a valid dmabuf; query only.
    if let Err(err) =
        unsafe { fd_api.get_memory_fd_properties(handle_type, fd.as_raw_fd(), &mut fd_props) }
    {
        destroy(None);
        return Err(format!("vkGetMemoryFdPropertiesKHR: {err}"));
    }
    let bits = req.memory_type_bits & fd_props.memory_type_bits;
    if bits == 0 || req.size > len {
        destroy(None);
        return Err(format!(
            "no memory type (buffer 0x{:x}, dmabuf 0x{:x}) or size {} > {len}",
            req.memory_type_bits, fd_props.memory_type_bits, req.size
        ));
    }
    let mut dedicated = vk::MemoryDedicatedAllocateInfo::default().buffer(buffer);
    let mut import = vk::ImportMemoryFdInfoKHR::default()
        .handle_type(handle_type)
        .fd(fd.as_raw_fd());
    let alloc = vk::MemoryAllocateInfo::default()
        .allocation_size(req.size)
        .memory_type_index(bits.trailing_zeros())
        .push_next(&mut import)
        .push_next(&mut dedicated);
    // SAFETY: valid allocate info; on success Vulkan owns the fd.
    let memory = match unsafe { raw.allocate_memory(&alloc, None) } {
        Ok(memory) => {
            let _ = fd.into_raw_fd();
            memory
        }
        Err(err) => {
            destroy(None);
            return Err(format!("vkAllocateMemory (import): {err}"));
        }
    };
    // SAFETY: fresh buffer and memory of matching requirements.
    if let Err(err) = unsafe { raw.bind_buffer_memory(buffer, memory, 0) } {
        destroy(Some(memory));
        return Err(format!("vkBindBufferMemory: {err}"));
    }
    // SAFETY: see above; wgpu takes ownership of `buffer` and `memory`.
    let hal_buffer = unsafe { hal_vk::Buffer::from_raw_managed(buffer, memory, 0, req.size) };
    drop(hal);
    // SAFETY: `hal_buffer` was created on this device with the usages described below.
    Ok(unsafe {
        device.create_buffer_from_hal::<Vulkan>(
            hal_buffer,
            &wgpu::BufferDescriptor {
                label: Some("dmabuf-storage"),
                size: req.size,
                usage: wgpu::BufferUsages::STORAGE
                    | wgpu::BufferUsages::COPY_SRC
                    | wgpu::BufferUsages::COPY_DST,
                mapped_at_creation: false,
            },
        )
    })
}

/// Packs the green channel of a strided XRGB8888 frame into `out` (one u32 per pixel): reads
/// every pixel once, like a first plane-sweep pass would.
const UNPACK: &str = "
    struct Frame { width: u32, height: u32, stride_words: u32, _pad: u32 }
    @group(0) @binding(0) var<storage, read> src: array<u32>;
    @group(0) @binding(1) var<storage, read_write> out: array<u32>;
    @group(0) @binding(2) var<storage, read> frame: Frame;
    @compute @workgroup_size(8, 8) fn main(@builtin(global_invocation_id) id: vec3<u32>) {
        if id.x >= frame.width || id.y >= frame.height { return; }
        out[id.y * frame.width + id.x] = (src[id.y * frame.stride_words + id.x] >> 8u) & 0xffu;
    }";

/// Same, from a sampled-image copy of the frame.
const UNPACK_TEXTURE: &str = "
    @group(0) @binding(0) var src: texture_2d<f32>;
    @group(0) @binding(1) var<storage, read_write> out: array<u32>;
    @compute @workgroup_size(8, 8) fn main(@builtin(global_invocation_id) id: vec3<u32>) {
        let size = textureDimensions(src);
        if id.x >= size.x || id.y >= size.y { return; }
        let px = textureLoad(src, vec2<i32>(id.xy), 0);
        out[id.y * size.x + id.x] = u32(px.g * 255.0 + 0.5);
    }";

fn pixel(x: u32, y: u32) -> [u8; 4] {
    // XRGB8888 little-endian: B, G, R, X.
    [
        (x % 251) as u8,
        ((x + y) % 253) as u8,
        (y % 241) as u8,
        0xff,
    ]
}

/// Per-frame cost of handing a LINEAR XRGB8888 dmabuf to a compute shader: as a storage buffer
/// (import per frame, and cached), and as an image (import + copy to a sampleable texture).
pub(super) fn frame_import(backend: &WgpuBackend, device: &wgpu::Device, queue: &wgpu::Queue) {
    // SAFETY: query only; the guard is dropped before any import.
    let probe = match unsafe { device.as_hal::<Vulkan>() } {
        Some(hal) => buffer_import_supported(&hal),
        None => Err("not a Vulkan device".into()),
    };
    kv(
        "dmabuf_storage_buffer_import",
        match &probe {
            Ok(()) => "importable".to_string(),
            Err(err) => err.clone(),
        },
    );
    kv(
        "dmabuf_image_import",
        if backend.dmabuf_import_support().is_supported() {
            "supported".to_string()
        } else {
            format!("{:?}", backend.dmabuf_import_support())
        },
    );
    let unpack = pipeline(device, UNPACK);
    let unpack_texture = pipeline(device, UNPACK_TEXTURE);
    for (width, height) in FRAMES {
        let stride = u64::from(width * 4).next_multiple_of(256);
        let len = (stride * u64::from(height)) as usize;
        let tag = format!("{width}x{height}");
        let buf = match DmaBuf::alloc(len) {
            Ok(buf) => buf,
            Err(err) => {
                kv(&tag, format!("dma-heap alloc failed: {err}"));
                continue;
            }
        };
        let filled = buf.write(|bytes| {
            for y in 0..height {
                for x in 0..width {
                    let at = y as usize * stride as usize + x as usize * 4;
                    bytes[at..at + 4].copy_from_slice(&pixel(x, y));
                }
            }
        });
        if let Err(err) = filled {
            kv(&tag, format!("mmap failed: {err}"));
            continue;
        }
        let out = storage(device, u64::from(width * height) * 4);
        if probe.is_ok() {
            buffer_path(
                device,
                queue,
                &unpack,
                &buf,
                &out,
                (width, height, stride),
                &tag,
            );
            verify(device, queue, &out, width, height, &format!("{tag}_buffer"));
        }
        image_path(
            backend,
            device,
            queue,
            &unpack_texture,
            &buf,
            &out,
            (width, height, stride),
            &tag,
        );
        verify(device, queue, &out, width, height, &format!("{tag}_image"));
    }
}

#[allow(clippy::too_many_arguments)]
fn buffer_path(
    device: &wgpu::Device,
    queue: &wgpu::Queue,
    unpack: &wgpu::ComputePipeline,
    buf: &DmaBuf,
    out: &wgpu::Buffer,
    (width, height, stride): (u32, u32, u64),
    tag: &str,
) {
    let params: [u32; 4] = [width, height, (stride / 4) as u32, 0];
    let frame = device.create_buffer_init(&wgpu::util::BufferInitDescriptor {
        label: Some("frame"),
        contents: bytemuck::cast_slice(&params),
        usage: wgpu::BufferUsages::STORAGE,
    });
    let groups = (width.div_ceil(8), height.div_ceil(8));
    let (mut import, mut total) = (Vec::new(), Vec::new());
    let mut cached = None;
    for _ in 0..iters() {
        let start = Instant::now();
        let fd = match buf.fd.try_clone() {
            Ok(fd) => fd,
            Err(err) => return kv(&format!("{tag}_buffer"), format!("dup failed: {err}")),
        };
        let src = match import_buffer(device, fd, buf.len as u64) {
            Ok(src) => src,
            Err(err) => return kv(&format!("{tag}_buffer"), format!("import failed: {err}")),
        };
        let bind_group = bind(device, unpack, &[&src, out, &frame]);
        import.push(start.elapsed());
        run_once(device, queue, unpack, &bind_group, groups);
        total.push(start.elapsed());
        cached = Some((src, bind_group));
    }
    kv(&format!("{tag}_buffer_import"), stats(&mut import));
    kv(
        &format!("{tag}_buffer_import_and_kernel"),
        stats(&mut total),
    );
    if let Some((_src, bind_group)) = cached {
        let mut kernel: Vec<_> = (0..iters())
            .map(|_| run_once(device, queue, unpack, &bind_group, groups))
            .collect();
        kv(&format!("{tag}_buffer_cached_kernel"), stats(&mut kernel));
    }
}

#[allow(clippy::too_many_arguments)]
fn image_path(
    backend: &WgpuBackend,
    device: &wgpu::Device,
    queue: &wgpu::Queue,
    unpack: &wgpu::ComputePipeline,
    buf: &DmaBuf,
    out: &wgpu::Buffer,
    (width, height, stride): (u32, u32, u64),
    tag: &str,
) {
    let size = wgpu::Extent3d {
        width,
        height,
        depth_or_array_layers: 1,
    };
    let sampled = device.create_texture(&wgpu::TextureDescriptor {
        label: Some("frame-copy"),
        size,
        mip_level_count: 1,
        sample_count: 1,
        dimension: wgpu::TextureDimension::D2,
        format: wgpu::TextureFormat::Bgra8Unorm,
        usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
        view_formats: &[],
    });
    let view = sampled.create_view(&Default::default());
    let bind_group = device.create_bind_group(&wgpu::BindGroupDescriptor {
        label: None,
        layout: &unpack.get_bind_group_layout(0),
        entries: &[
            wgpu::BindGroupEntry {
                binding: 0,
                resource: wgpu::BindingResource::TextureView(&view),
            },
            wgpu::BindGroupEntry {
                binding: 1,
                resource: out.as_entire_binding(),
            },
        ],
    });
    let groups = (width.div_ceil(8), height.div_ceil(8));
    let (mut import, mut copy, mut total) = (Vec::new(), Vec::new(), Vec::new());
    for _ in 0..iters() {
        let start = Instant::now();
        let plane = match ExternalPlane::from_borrowed(buf.fd.as_fd(), 0, stride) {
            Ok(plane) => plane,
            Err(err) => return kv(&format!("{tag}_image"), format!("plane failed: {err}")),
        };
        let handle = match backend.import_dmabuf(
            ExternalFrameDescriptor::single_plane(width, height, DrmFourcc::XRGB8888, plane)
                .with_modifier(DRM_FORMAT_MOD_LINEAR),
        ) {
            Ok(handle) => handle,
            Err(err) => return kv(&format!("{tag}_image"), format!("import failed: {err}")),
        };
        let Some(texture) = backend.wgpu_get_texture(&handle) else {
            return kv(&format!("{tag}_image"), "imported handle has no texture");
        };
        import.push(start.elapsed());
        let copy_start = Instant::now();
        let mut encoder = device.create_command_encoder(&Default::default());
        encoder.copy_texture_to_texture(texture.as_image_copy(), sampled.as_image_copy(), size);
        queue.submit([encoder.finish()]);
        wait(device);
        copy.push(copy_start.elapsed());
        run_once(device, queue, unpack, &bind_group, groups);
        total.push(start.elapsed());
    }
    kv(&format!("{tag}_image_import"), stats(&mut import));
    kv(&format!("{tag}_image_copy"), stats(&mut copy));
    kv(
        &format!("{tag}_image_import_copy_kernel"),
        stats(&mut total),
    );
}

/// Read `out` back and check it against the frame's green channel.
fn verify(
    device: &wgpu::Device,
    queue: &wgpu::Queue,
    out: &wgpu::Buffer,
    width: u32,
    height: u32,
    tag: &str,
) {
    let size = out.size();
    let staging = device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("verify"),
        size,
        usage: wgpu::BufferUsages::MAP_READ | wgpu::BufferUsages::COPY_DST,
        mapped_at_creation: false,
    });
    let mut encoder = device.create_command_encoder(&Default::default());
    encoder.copy_buffer_to_buffer(out, 0, &staging, 0, size);
    queue.submit([encoder.finish()]);
    let slice = staging.slice(..);
    slice.map_async(wgpu::MapMode::Read, |_| {});
    wait(device);
    let words: Vec<u32> = match slice.get_mapped_range() {
        Ok(view) => bytemuck::cast_slice(&view).to_vec(),
        Err(err) => return kv(&format!("{tag}_verify"), format!("map failed: {err:?}")),
    };
    let bad = (0..height)
        .flat_map(|y| (0..width).map(move |x| (x, y)))
        .filter(|&(x, y)| words[(y * width + x) as usize] != u32::from(pixel(x, y)[1]))
        .count();
    kv(
        &format!("{tag}_verify"),
        if bad == 0 {
            "ok".to_string()
        } else {
            format!("{bad} wrong pixels")
        },
    );
    // Clear for the next path.
    queue.write_buffer(out, 0, &vec![0u8; size as usize]);
}

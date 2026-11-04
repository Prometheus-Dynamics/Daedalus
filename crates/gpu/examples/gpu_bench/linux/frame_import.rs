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

/// Import `fd` (a dmabuf of `len` bytes) as a wgpu storage buffer of `size` bytes through Vulkan.
fn import_buffer(
    device: &wgpu::Device,
    fd: OwnedFd,
    size: u64,
    len: u64,
) -> Result<wgpu::Buffer, String> {
    // SAFETY: the hal device outlives this function; the buffer and memory created here go to
    // wgpu (`from_raw_managed`), which destroys and frees them when the wgpu buffer drops.
    let hal = unsafe { device.as_hal::<Vulkan>() }.ok_or("not a Vulkan device")?;
    let instance = hal.shared_instance().raw_instance();
    let raw = hal.raw_device();
    let handle_type = vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT;
    let mut external = vk::ExternalMemoryBufferCreateInfo::default().handle_types(handle_type);
    let info = vk::BufferCreateInfo::default()
        .size(size)
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
            // The guard of `daedalus_gpu`'s `settle_refused_fd` (not public): NVIDIA's proprietary
            // driver (595.104.02) closes a refused dmabuf fd itself, so it is not closed again.
            let mut driver = vk::PhysicalDeviceDriverProperties::default();
            let mut props = vk::PhysicalDeviceProperties2::default().push_next(&mut driver);
            // SAFETY: the physical device belongs to `instance`; query only.
            unsafe {
                instance.get_physical_device_properties2(hal.raw_physical_device(), &mut props)
            };
            if driver.driver_id == vk::DriverId::NVIDIA_PROPRIETARY {
                let _ = fd.into_raw_fd();
            }
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
        // A frame-sized dmabuf, as a camera allocates it, and one with a page of slack after the
        // frame (some drivers need the buffer's memory requirement to fit inside the dmabuf).
        let alloc = |len: usize| -> Result<DmaBuf, String> {
            let buf = DmaBuf::alloc(len).map_err(|err| format!("dma-heap alloc failed: {err}"))?;
            buf.write(|bytes| {
                for y in 0..height {
                    for x in 0..width {
                        let at = y as usize * stride as usize + x as usize * 4;
                        bytes[at..at + 4].copy_from_slice(&pixel(x, y));
                    }
                }
            })
            .map_err(|err| format!("mmap failed: {err}"))?;
            Ok(buf)
        };
        let (buf, padded) = match (alloc(len), alloc(len + 4096)) {
            (Ok(buf), Ok(padded)) => (buf, padded),
            (Err(err), _) | (_, Err(err)) => {
                kv(&tag, err);
                continue;
            }
        };
        let out = storage(device, u64::from(width * height) * 4);
        upload_path(
            device,
            queue,
            &unpack,
            &buf,
            &out,
            (width, height, stride),
            &tag,
        );
        verify(device, queue, &out, width, height, &format!("{tag}_upload"));
        if probe.is_ok() {
            let exact = buf
                .fd
                .try_clone()
                .map_err(|err| err.to_string())
                .and_then(|fd| import_buffer(device, fd, len as u64, buf.len as u64));
            kv(
                &format!("{tag}_buffer_import_frame_sized_dmabuf"),
                match exact {
                    Ok(_) => "ok".to_string(),
                    Err(err) => err,
                },
            );
            if buffer_path(
                device,
                queue,
                &unpack,
                &padded,
                &out,
                (width, height, stride),
                &format!("{tag}_padded"),
            ) {
                verify(
                    device,
                    queue,
                    &out,
                    width,
                    height,
                    &format!("{tag}_padded_buffer"),
                );
            }
        }
        if image_path(
            backend,
            device,
            queue,
            &unpack_texture,
            &buf,
            &out,
            (width, height, stride),
            &tag,
        ) {
            verify(device, queue, &out, width, height, &format!("{tag}_image"));
        }
    }
}

/// The copy route: the CPU reads the mapped dmabuf into a storage buffer
/// (`Queue::write_buffer`) every frame, then the kernel runs.
fn upload_path(
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
    let src = storage(device, buf.len as u64);
    let bind_group = bind(device, unpack, &[&src, out, &frame]);
    let groups = (width.div_ceil(8), height.div_ceil(8));
    let (mut upload, mut total) = (Vec::new(), Vec::new());
    let mapped = buf.write(|bytes| {
        for _ in 0..iters() {
            let start = Instant::now();
            queue.write_buffer(&src, 0, bytes);
            queue.submit([]);
            upload.push(start.elapsed());
            run_once(device, queue, unpack, &bind_group, groups);
            total.push(start.elapsed());
        }
    });
    if let Err(err) = mapped {
        return kv(&format!("{tag}_upload"), format!("mmap failed: {err}"));
    }
    kv(&format!("{tag}_upload_write_buffer"), stats(&mut upload));
    kv(&format!("{tag}_upload_and_kernel"), stats(&mut total));
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
) -> bool {
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
            Err(err) => {
                kv(&format!("{tag}_buffer"), format!("dup failed: {err}"));
                return false;
            }
        };
        let frame_len = stride * u64::from(height);
        let src = match import_buffer(device, fd, frame_len, buf.len as u64) {
            Ok(src) => src,
            Err(err) => {
                kv(&format!("{tag}_buffer"), format!("import failed: {err}"));
                return false;
            }
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
    if let Some((src, bind_group)) = cached {
        let mut kernel: Vec<_> = (0..iters())
            .map(|_| run_once(device, queue, unpack, &bind_group, groups))
            .collect();
        kv(&format!("{tag}_buffer_cached_kernel"), stats(&mut kernel));
        tfu_copy(device, queue, &src, (width, height, stride), tag);
    }
    true
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
) -> bool {
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
            Err(err) => {
                kv(&format!("{tag}_image"), format!("plane failed: {err}"));
                return false;
            }
        };
        let handle = match backend.import_dmabuf(
            ExternalFrameDescriptor::single_plane(width, height, DrmFourcc::XRGB8888, plane)
                .with_modifier(DRM_FORMAT_MOD_LINEAR),
        ) {
            Ok(handle) => handle,
            Err(err) => {
                kv(&format!("{tag}_image"), format!("import failed: {err}"));
                return false;
            }
        };
        let Some(texture) = backend.wgpu_get_texture(&handle) else {
            kv(&format!("{tag}_image"), "imported handle has no texture");
            return false;
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
    true
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

/// The imported frame buffer copied into a sampleable texture by `copy_buffer_to_texture`
/// (`vkCmdCopyBufferToImage`, a whole slice from offset 0: v3dv's TFU path), against the
/// shader copy above (`_buffer_cached_kernel`).
fn tfu_copy(
    device: &wgpu::Device,
    queue: &wgpu::Queue,
    src: &wgpu::Buffer,
    (width, height, stride): (u32, u32, u64),
    tag: &str,
) {
    let size = wgpu::Extent3d {
        width,
        height,
        depth_or_array_layers: 1,
    };
    let texture = device.create_texture(&wgpu::TextureDescriptor {
        label: Some("tfu-target"),
        size,
        mip_level_count: 1,
        sample_count: 1,
        dimension: wgpu::TextureDimension::D2,
        format: wgpu::TextureFormat::Bgra8Unorm,
        usage: wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST,
        view_formats: &[],
    });
    let mut samples: Vec<_> = (0..iters())
        .map(|_| {
            let start = Instant::now();
            let mut encoder = device.create_command_encoder(&Default::default());
            encoder.copy_buffer_to_texture(
                wgpu::TexelCopyBufferInfo {
                    buffer: src,
                    layout: wgpu::TexelCopyBufferLayout {
                        offset: 0,
                        bytes_per_row: Some(stride as u32),
                        rows_per_image: Some(height),
                    },
                },
                texture.as_image_copy(),
                size,
            );
            queue.submit([encoder.finish()]);
            wait(device);
            start.elapsed()
        })
        .collect();
    kv(
        &format!("{tag}_buffer_to_texture_copy"),
        stats(&mut samples),
    );
}

/// Readback through a system dma-heap buffer: the GPU writes it through a storage-buffer import,
/// the CPU reads it through its own (cached) mapping, with `DMA_BUF_IOCTL_SYNC` doing the cache
/// maintenance. For devices whose host-visible memory is uncached (v3dv has only
/// `DEVICE_LOCAL | HOST_VISIBLE | HOST_COHERENT`), against `read_mapped` of host-cached storage.
pub(super) fn readback() {
    use daedalus_gpu::{GpuKernel, GpuKernels, GpuOptions, GpuStorage, select_backend};
    let Ok(ctx) = select_backend(&GpuOptions {
        preferred_backend: Some(daedalus_gpu::GpuBackendKind::Wgpu),
        ..Default::default()
    }) else {
        return kv("readback", "no wgpu context");
    };
    let Ok(kernels) = GpuKernels::new(&ctx) else {
        return;
    };
    for size in [1usize << 20, 16 << 20] {
        let label = format!("readback_{}k", size >> 10);
        let data: Vec<u8> = (0..size).map(|i| (i * 7 + 3) as u8).collect();
        let src = GpuStorage::with_data(&ctx, &data, None).unwrap();
        let mut out = vec![0u8; size];
        let mut time = |read: &mut dyn FnMut(&mut [u8])| {
            let mut times = Vec::new();
            for _ in 0..10 {
                out.fill(0);
                let start = Instant::now();
                read(&mut out);
                times.push(start.elapsed());
            }
            let ok = out == data;
            (super::median(&mut times), ok)
        };
        if let Ok(cached) = GpuStorage::new_host_cached(&ctx, size as u64, None) {
            let mut reads = Vec::new();
            let (t, ok) = time(&mut |out| {
                kernels
                    .run(GpuKernel::Copy, &src, &cached, size as u64)
                    .unwrap()
                    .wait();
                let start = Instant::now();
                cached.read_mapped(0, out).unwrap();
                reads.push(start.elapsed());
            });
            kv(
                &format!("{label}_host_cached_kernel_and_read"),
                format!(
                    "{:.3} ms, ok {ok}; read {:.3} ms",
                    t.as_secs_f64() * 1e3,
                    super::median(&mut reads).as_secs_f64() * 1e3
                ),
            );
        }
        let need = match GpuStorage::dmabuf_import_len(&ctx, size as u64) {
            Ok(need) => need as usize,
            Err(err) => return kv(&label, format!("dmabuf: {err}")),
        };
        let buf = match DmaBuf::alloc(need.next_multiple_of(4096)) {
            Ok(buf) => buf,
            Err(err) => return kv(&label, format!("dma-heap: {err}")),
        };
        let imported = match GpuStorage::import_dmabuf(
            &ctx,
            buf.fd.try_clone().unwrap(),
            0,
            size as u64,
            None,
            std::time::Duration::from_secs(1),
            None,
        ) {
            Ok(storage) => storage,
            Err(err) => return kv(&label, format!("import: {err}")),
        };
        // SAFETY: a read-only mapping of the dmabuf, unmapped below.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                buf.len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                buf.fd.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return kv(&label, "mmap failed");
        }
        let sync = |flags: u64| {
            // SAFETY: valid dmabuf fd and a u64 `dma_buf_sync` argument.
            unsafe { libc::ioctl(buf.fd.as_raw_fd(), DMA_BUF_IOCTL_SYNC as _, &flags) };
        };
        const READ: u64 = 1;
        let (mut sync_only, mut copy_only) = (Vec::new(), Vec::new());
        let (t, ok) = time(&mut |out| {
            kernels
                .run(GpuKernel::Copy, &src, &imported, size as u64)
                .unwrap()
                .wait();
            let start = Instant::now();
            sync(DMA_BUF_SYNC_START | READ);
            let synced = start.elapsed();
            // SAFETY: `size` bytes of the mapping; the GPU work writing them completed.
            out.copy_from_slice(unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), size) });
            let copied = start.elapsed();
            sync(DMA_BUF_SYNC_END | READ);
            sync_only.push(synced);
            copy_only.push(copied - synced);
        });
        kv(
            &format!("{label}_dmaheap_kernel_and_read"),
            format!(
                "{:.3} ms, ok {ok}; sync {:.3} ms, copy {:.3} ms",
                t.as_secs_f64() * 1e3,
                super::median(&mut sync_only).as_secs_f64() * 1e3,
                super::median(&mut copy_only).as_secs_f64() * 1e3
            ),
        );
        // SAFETY: the mapping made above.
        unsafe { libc::munmap(ptr, buf.len) };
    }
}

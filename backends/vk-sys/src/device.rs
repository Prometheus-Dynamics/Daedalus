//! Opening a device with what wgpu doesn't ask for, and reading what a device has.

use std::ffi::CStr;
use std::sync::{Mutex, PoisonError};

use ash::{ext, khr, vk};
use wgpu::hal::api::Vulkan;
use wgpu::hal::vulkan as hal_vk;

use crate::DriverHeld;

/// Importing a dma-buf as an image needs all three. wgpu-hal enables the first two itself when
/// the adapter has them.
const DMA_BUF: [&CStr; 3] = [
    khr::external_memory_fd::NAME,
    ext::external_memory_dma_buf::NAME,
    ext::image_drm_format_modifier::NAME,
];
/// Added with the three above when the adapter has them.
const WITH_DMA_BUF: [&CStr; 2] = [
    khr::external_semaphore_fd::NAME,
    ext::queue_family_foreign::NAME,
];

/// 8-bit storage a device was opened with.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EightBit {
    /// 8-bit types in storage buffers.
    pub storage_buffer: bool,
    /// In uniform buffers too.
    pub uniform_and_storage_buffer: bool,
}

/// What each device opened here got, by handle.
static OPENED: Mutex<Vec<(vk::Device, EightBit)>> = Mutex::new(Vec::new());

/// Opens a device on `adapter` with dma-buf import and 8-bit storage added where the adapter has
/// them. `None` when the adapter isn't Vulkan, has neither, or refuses: the caller then opens a
/// plain device.
pub fn open_extended(
    _held: &impl DriverHeld,
    adapter: &wgpu::Adapter,
    desc: &wgpu::DeviceDescriptor<'_>,
) -> Option<(wgpu::Device, wgpu::Queue)> {
    let (open, eight_bit) = open_hal(adapter, desc)?;
    // SAFETY: `open` came from this adapter with `desc`'s features and limits, and the callback
    // only added extensions and features the adapter reports.
    let (device, queue) = unsafe { adopt_hal(adapter, open, desc) }.ok()?;
    // SAFETY: reads a handle.
    if let Some(hal) = unsafe { device.as_hal::<Vulkan>() } {
        let handle = hal.raw_device().handle();
        let mut opened = OPENED.lock().unwrap_or_else(PoisonError::into_inner);
        opened.retain(|(known, _)| *known != handle);
        opened.push((handle, eight_bit));
    }
    Some((device, queue))
}

/// Makes a wgpu device of one opened through wgpu-hal. This is how a device someone else set up
/// becomes one a backend can adopt.
///
/// # Safety
///
/// `open` must be a device of `adapter`, opened with the features and limits in `desc`, and
/// nothing else may use it. The caller keeps device creation from racing: see the driver lock.
// SAFETY: the contract is in the doc comment above.
pub unsafe fn adopt_hal(
    adapter: &wgpu::Adapter,
    open: wgpu::hal::OpenDevice<Vulkan>,
    desc: &wgpu::DeviceDescriptor<'_>,
) -> Result<(wgpu::Device, wgpu::Queue), wgpu::RequestDeviceError> {
    // SAFETY: the caller's contract.
    unsafe { adapter.create_device_from_hal::<Vulkan>(open, desc) }
}

fn open_hal(
    adapter: &wgpu::Adapter,
    desc: &wgpu::DeviceDescriptor<'_>,
) -> Option<(wgpu::hal::OpenDevice<Vulkan>, EightBit)> {
    // SAFETY: the adapter is only used to open a device.
    let hal_adapter = unsafe { adapter.as_hal::<Vulkan>() }?;
    let caps = hal_adapter.physical_device_capabilities();
    let instance = hal_adapter.shared_instance();
    let api = caps
        .properties()
        .api_version
        .min(instance.instance_api_version());
    if api < vk::API_VERSION_1_1 {
        return None;
    }
    let mut extensions: Vec<&'static CStr> = Vec::new();
    if DMA_BUF.iter().all(|name| caps.supports_extension(name)) {
        let wanted = DMA_BUF.into_iter().chain(WITH_DMA_BUF);
        extensions.extend(wanted.filter(|name| caps.supports_extension(name)));
    }
    // Core in Vulkan 1.2, an extension before. wgpu-hal enables neither.
    let core = api >= vk::API_VERSION_1_2;
    let mut eight_bit = vk::PhysicalDevice8BitStorageFeatures::default();
    if core || caps.supports_extension(khr::_8bit_storage::NAME) {
        let mut features = vk::PhysicalDeviceFeatures2::default().push_next(&mut eight_bit);
        // SAFETY: a feature query on the adapter's physical device.
        unsafe {
            instance
                .raw_instance()
                .get_physical_device_features2(hal_adapter.raw_physical_device(), &mut features);
        }
    }
    let enabled = EightBit {
        storage_buffer: eight_bit.storage_buffer8_bit_access != 0,
        uniform_and_storage_buffer: eight_bit.uniform_and_storage_buffer8_bit_access != 0,
    };
    eight_bit.storage_push_constant8 = 0;
    if enabled.storage_buffer && !core {
        extensions.push(khr::_8bit_storage::NAME);
    }
    if extensions.is_empty() && !enabled.storage_buffer {
        return None;
    }
    let eight_bit = &mut eight_bit;
    let callback: Box<hal_vk::CreateDeviceCallback<'_>> = Box::new(move |args| {
        for name in &extensions {
            if !args.extensions.contains(name) {
                args.extensions.push(name);
            }
        }
        if enabled.storage_buffer {
            // wgpu-hal chains the promoted feature structs one by one, and never this one.
            *args.create_info = args.create_info.push_next(eight_bit);
        }
    });
    // SAFETY: the callback adds only extensions the adapter reports and the 8-bit features the
    // device has. It removes nothing.
    let opened = unsafe {
        hal_adapter.open_with_callback(
            desc.required_features,
            &desc.required_limits,
            &desc.memory_hints,
            Some(callback),
        )
    };
    opened.ok().map(|open| (open, enabled))
}

/// What a device was opened with, beyond what wgpu reports.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Extras {
    pub eight_bit: EightBit,
    /// A dma-buf can be imported as memory.
    pub dma_buf_import: bool,
    /// A sync fd can be imported as a semaphore.
    pub sync_fd_import: bool,
    /// How many bits of a timestamp the queue writes. 0 means none.
    pub timestamp_valid_bits: u32,
    /// Nanoseconds per timestamp tick.
    pub timestamp_period_ns: f32,
}

impl Extras {
    /// `None` when the device isn't on Vulkan.
    pub fn of(device: &wgpu::Device) -> Option<Extras> {
        let raw = Raw::of(device)?;
        // SAFETY: property queries on the device's own physical device.
        let (families, properties) = unsafe {
            (
                raw.instance
                    .get_physical_device_queue_family_properties(raw.physical),
                raw.instance.get_physical_device_properties(raw.physical),
            )
        };
        let handle = raw.device.handle();
        let opened = OPENED.lock().unwrap_or_else(PoisonError::into_inner);
        Some(Extras {
            eight_bit: opened
                .iter()
                .find(|(known, _)| *known == handle)
                .map(|(_, eight_bit)| *eight_bit)
                .unwrap_or_default(),
            dma_buf_import: raw.has(khr::external_memory_fd::NAME)
                && raw.has(ext::external_memory_dma_buf::NAME),
            sync_fd_import: raw.has(khr::external_semaphore_fd::NAME),
            timestamp_valid_bits: families
                .get(raw.family as usize)
                .map_or(0, |family| family.timestamp_valid_bits),
            timestamp_period_ns: properties.limits.timestamp_period,
        })
    }
}

/// wgpu's Vulkan objects behind a device. The tables are copies that share wgpu's handles, and
/// they stay inside this crate.
pub(crate) struct Raw {
    pub(crate) instance: ash::Instance,
    pub(crate) physical: vk::PhysicalDevice,
    pub(crate) device: ash::Device,
    pub(crate) family: u32,
    extensions: Vec<&'static CStr>,
}

impl Raw {
    pub(crate) fn of(device: &wgpu::Device) -> Option<Raw> {
        // SAFETY: reads handles. Nothing is destroyed through them, and every use in this crate
        // keeps the wgpu device alive beside them.
        let hal = unsafe { device.as_hal::<Vulkan>() }?;
        Some(Raw {
            instance: hal.shared_instance().raw_instance().clone(),
            physical: hal.raw_physical_device(),
            device: hal.raw_device().clone(),
            family: hal.queue_family_index(),
            extensions: hal.enabled_device_extensions().to_vec(),
        })
    }

    pub(crate) fn has(&self, extension: &CStr) -> bool {
        self.extensions.contains(&extension)
    }
}

/// A device for this crate's tests, which can't reach the backend's.
#[cfg(test)]
pub(crate) fn test_device(software: bool) -> Option<(wgpu::Device, wgpu::Queue)> {
    struct Held;
    impl DriverHeld for Held {}
    static LOCK: Mutex<()> = Mutex::new(());
    let _held = LOCK.lock().unwrap_or_else(PoisonError::into_inner);

    let mut desc = wgpu::InstanceDescriptor::new_without_display_handle();
    desc.backends = wgpu::Backends::VULKAN;
    desc.flags -= wgpu::InstanceFlags::DEBUG;
    let instance = wgpu::Instance::new(desc);
    let mut adapters = pollster::block_on(instance.enumerate_adapters(wgpu::Backends::VULKAN));
    // Hardware first, unless the test wants the software one.
    adapters.sort_by_key(|adapter| {
        (adapter.get_info().device_type == wgpu::DeviceType::Cpu) != software
    });
    let adapter = adapters.first()?;
    let desc = wgpu::DeviceDescriptor {
        required_limits: adapter.limits(),
        ..wgpu::DeviceDescriptor::default()
    };
    open_extended(&Held, adapter, &desc)
        .or_else(|| pollster::block_on(adapter.request_device(&desc)).ok())
}

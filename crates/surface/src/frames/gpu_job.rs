//! A long GPU job on a Vulkan device, and a `sync_file` of its completion: the real acquire fence
//! of the board fence test ([`super::fence_board`]).
//!
//! The job is copies back and forth between two device buffers, recorded in one command buffer:
//! no shader, so it runs on any Vulkan driver (v3dv on the Raze). Its completion is exported as a
//! `sync_file` through an exportable binary semaphore (`VK_KHR_external_semaphore_fd`,
//! `SYNC_FD`) that the submission signals. The loader is opened at run time
//! (`libvulkan.so.1`), so nothing links against Vulkan.

use std::os::fd::{FromRawFd, OwnedFd};
use std::time::{Duration, Instant};

use ash::{khr, vk};

const SYNC_FD: vk::ExternalSemaphoreHandleTypeFlags = vk::ExternalSemaphoreHandleTypeFlags::SYNC_FD;
/// Each buffer: the job copies this many bytes per copy.
const BUFFER_BYTES: u64 = 16 << 20;

/// A Vulkan device with two buffers to copy between and a command buffer to record the job in.
pub(super) struct GpuJob {
    instance: ash::Instance,
    device: ash::Device,
    queue: vk::Queue,
    pool: vk::CommandPool,
    command: vk::CommandBuffer,
    buffers: [vk::Buffer; 2],
    memory: [vk::DeviceMemory; 2],
    fd_api: khr::external_semaphore_fd::Device,
    /// The device's name, as the driver reports it.
    pub(super) name: String,
    /// Round trips (two copies each) per job.
    rounds: u32,
    // Last: the loader stays open while the instance and device live.
    _entry: ash::Entry,
    _library: libloading::Library,
}

/// A submitted job: its completion fence (for the test's own wait) and the semaphore it signals.
pub(super) struct Pending {
    fence: vk::Fence,
    semaphore: vk::Semaphore,
    submitted: Instant,
}

/// The result of a Vulkan call, with the call named.
fn vk_ok<T>(what: &str, result: Result<T, vk::Result>) -> Result<T, String> {
    result.map_err(|error| format!("{what}: {error}"))
}

impl GpuJob {
    /// The first Vulkan device whose queue family 0 does transfers and that exports a binary
    /// semaphore as a `SYNC_FD`. `Err` (why) when there is none.
    pub(super) fn new() -> Result<Self, String> {
        // Safety: the system Vulkan loader; its entry point has the `vkGetInstanceProcAddr` type.
        let library = unsafe { libloading::Library::new("libvulkan.so.1") }
            .map_err(|error| format!("no Vulkan loader: {error}"))?;
        // Safety: as above.
        let get_instance_proc_addr = unsafe {
            *library
                .get::<vk::PFN_vkGetInstanceProcAddr>(b"vkGetInstanceProcAddr\0")
                .map_err(|error| format!("no vkGetInstanceProcAddr: {error}"))?
        };
        // Safety: the loader's own entry point, kept loaded with `library`.
        let entry = unsafe {
            ash::Entry::from_static_fn(ash::StaticFn {
                get_instance_proc_addr,
            })
        };
        let app = vk::ApplicationInfo::default().api_version(vk::API_VERSION_1_1);
        // Safety (throughout): plain Vulkan calls on handles this function creates, each
        // checked; everything is destroyed in `Drop`, or here on the error paths that matter for
        // a test (a leaked handle on a failing test is acceptable).
        let instance = vk_ok("vkCreateInstance", unsafe {
            entry.create_instance(
                &vk::InstanceCreateInfo::default().application_info(&app),
                None,
            )
        })?;
        let physical = unsafe { instance.enumerate_physical_devices() }
            .map_err(|error| format!("vkEnumeratePhysicalDevices: {error}"))?
            .into_iter()
            .find(|&physical| {
                let families =
                    unsafe { instance.get_physical_device_queue_family_properties(physical) };
                let transfers = families.first().is_some_and(|family| {
                    family.queue_flags.intersects(
                        vk::QueueFlags::TRANSFER
                            | vk::QueueFlags::COMPUTE
                            | vk::QueueFlags::GRAPHICS,
                    )
                });
                let mut external = vk::ExternalSemaphoreProperties::default();
                unsafe {
                    instance.get_physical_device_external_semaphore_properties(
                        physical,
                        &vk::PhysicalDeviceExternalSemaphoreInfo::default().handle_type(SYNC_FD),
                        &mut external,
                    );
                }
                transfers
                    && external
                        .external_semaphore_features
                        .contains(vk::ExternalSemaphoreFeatureFlags::EXPORTABLE)
            })
            .ok_or("no Vulkan device exports a semaphore as a SYNC_FD")?;
        let properties = unsafe { instance.get_physical_device_properties(physical) };
        let name = properties.device_name_as_c_str().map_or_else(
            |_| "?".to_string(),
            |name| name.to_string_lossy().into_owned(),
        );

        let priorities = [1.0];
        let queues = [vk::DeviceQueueCreateInfo::default()
            .queue_family_index(0)
            .queue_priorities(&priorities)];
        let extensions = [khr::external_semaphore_fd::NAME.as_ptr()];
        let device = vk_ok("vkCreateDevice", unsafe {
            instance.create_device(
                physical,
                &vk::DeviceCreateInfo::default()
                    .queue_create_infos(&queues)
                    .enabled_extension_names(&extensions),
                None,
            )
        })?;
        let queue = unsafe { device.get_device_queue(0, 0) };
        let memory_types = unsafe { instance.get_physical_device_memory_properties(physical) };
        let mut buffers = [vk::Buffer::null(); 2];
        let mut memory = [vk::DeviceMemory::null(); 2];
        for (buffer, memory) in buffers.iter_mut().zip(&mut memory) {
            *buffer = vk_ok("vkCreateBuffer", unsafe {
                device.create_buffer(
                    &vk::BufferCreateInfo::default().size(BUFFER_BYTES).usage(
                        vk::BufferUsageFlags::TRANSFER_SRC | vk::BufferUsageFlags::TRANSFER_DST,
                    ),
                    None,
                )
            })?;
            let needs = unsafe { device.get_buffer_memory_requirements(*buffer) };
            let index = (0..memory_types.memory_type_count)
                .find(|&i| needs.memory_type_bits & (1 << i) != 0)
                .ok_or("no memory type for the job's buffers")?;
            *memory = vk_ok("vkAllocateMemory", unsafe {
                device.allocate_memory(
                    &vk::MemoryAllocateInfo::default()
                        .allocation_size(needs.size)
                        .memory_type_index(index),
                    None,
                )
            })?;
            vk_ok("vkBindBufferMemory", unsafe {
                device.bind_buffer_memory(*buffer, *memory, 0)
            })?;
        }
        let pool = vk_ok("vkCreateCommandPool", unsafe {
            device.create_command_pool(
                &vk::CommandPoolCreateInfo::default()
                    .queue_family_index(0)
                    .flags(vk::CommandPoolCreateFlags::RESET_COMMAND_BUFFER),
                None,
            )
        })?;
        let command = vk_ok("vkAllocateCommandBuffers", unsafe {
            device.allocate_command_buffers(
                &vk::CommandBufferAllocateInfo::default()
                    .command_pool(pool)
                    .command_buffer_count(1),
            )
        })?[0];
        let fd_api = khr::external_semaphore_fd::Device::new(&instance, &device);
        Ok(Self {
            instance,
            device,
            queue,
            pool,
            command,
            buffers,
            memory,
            fd_api,
            name,
            rounds: 1,
            _entry: entry,
            _library: library,
        })
    }

    /// Records the job: `rounds` copies from buffer 0 to 1 and back, each after the last.
    fn record(&self) -> Result<(), String> {
        let device = &self.device;
        let region = [vk::BufferCopy::default().size(BUFFER_BYTES)];
        let barrier = [vk::MemoryBarrier::default()
            .src_access_mask(vk::AccessFlags::TRANSFER_WRITE)
            .dst_access_mask(vk::AccessFlags::TRANSFER_READ | vk::AccessFlags::TRANSFER_WRITE)];
        // Safety: the command buffer is not pending (every job is waited for before the next).
        unsafe {
            vk_ok(
                "vkResetCommandBuffer",
                device.reset_command_buffer(self.command, vk::CommandBufferResetFlags::empty()),
            )?;
            vk_ok(
                "vkBeginCommandBuffer",
                device.begin_command_buffer(self.command, &vk::CommandBufferBeginInfo::default()),
            )?;
            for _ in 0..self.rounds {
                for (from, to) in [(0, 1), (1, 0)] {
                    device.cmd_copy_buffer(
                        self.command,
                        self.buffers[from],
                        self.buffers[to],
                        &region,
                    );
                    device.cmd_pipeline_barrier(
                        self.command,
                        vk::PipelineStageFlags::TRANSFER,
                        vk::PipelineStageFlags::TRANSFER,
                        vk::DependencyFlags::empty(),
                        &barrier,
                        &[],
                        &[],
                    );
                }
            }
            vk_ok(
                "vkEndCommandBuffer",
                device.end_command_buffer(self.command),
            )
        }
    }

    /// Submits the job and returns a `sync_file` that signals when it completes, with the
    /// pending job (wait for it with [`Self::finish`] before the next submit).
    pub(super) fn submit(&self) -> Result<(OwnedFd, Pending), String> {
        self.record()?;
        let device = &self.device;
        let mut export = vk::ExportSemaphoreCreateInfo::default().handle_types(SYNC_FD);
        // Safety: handles created here, submitted once, exported once, destroyed in `finish`.
        unsafe {
            let semaphore = vk_ok(
                "vkCreateSemaphore",
                device.create_semaphore(
                    &vk::SemaphoreCreateInfo::default().push_next(&mut export),
                    None,
                ),
            )?;
            let fence = vk_ok(
                "vkCreateFence",
                device.create_fence(&vk::FenceCreateInfo::default(), None),
            )?;
            let commands = [self.command];
            let signals = [semaphore];
            let submit = [vk::SubmitInfo::default()
                .command_buffers(&commands)
                .signal_semaphores(&signals)];
            let submitted = Instant::now();
            vk_ok(
                "vkQueueSubmit",
                device.queue_submit(self.queue, &submit, fence),
            )?;
            // A SYNC_FD export needs the signal operation submitted, which it now is.
            let fd = vk_ok(
                "vkGetSemaphoreFdKHR",
                self.fd_api.get_semaphore_fd(
                    &vk::SemaphoreGetFdInfoKHR::default()
                        .semaphore(semaphore)
                        .handle_type(SYNC_FD),
                ),
            )?;
            if fd < 0 {
                return Err("the job completed before its sync_file was exported".into());
            }
            Ok((
                OwnedFd::from_raw_fd(fd),
                Pending {
                    fence,
                    semaphore,
                    submitted,
                },
            ))
        }
    }

    /// Waits for `pending` and returns how long the job took from its submission (an upper
    /// bound: the wait is the test's, after whatever it did meanwhile).
    pub(super) fn finish(&self, pending: Pending) -> Result<Duration, String> {
        // Safety: the job's own fence and semaphore, destroyed once it completed.
        unsafe {
            vk_ok(
                "vkWaitForFences",
                self.device
                    .wait_for_fences(&[pending.fence], true, u64::MAX),
            )?;
            let took = pending.submitted.elapsed();
            self.device.destroy_fence(pending.fence, None);
            self.device.destroy_semaphore(pending.semaphore, None);
            Ok(took)
        }
    }

    /// Sizes the job to take about `target` on this device: the fastest of three timed runs,
    /// scaled.
    pub(super) fn calibrate(&mut self, target: Duration) -> Result<Duration, String> {
        self.rounds = 8;
        let mut fastest = Duration::MAX;
        for _ in 0..3 {
            let (_fd, pending) = self.submit()?;
            fastest = fastest.min(self.finish(pending)?);
        }
        let per_round = fastest.as_secs_f64() / f64::from(self.rounds);
        self.rounds = (target.as_secs_f64() / per_round).clamp(1.0, 100_000.0) as u32;
        let (_fd, pending) = self.submit()?;
        self.finish(pending)
    }
}

impl Drop for GpuJob {
    fn drop(&mut self) {
        // Safety: nothing is pending past `device_wait_idle`; each handle is destroyed once, the
        // device before the instance.
        unsafe {
            let _ = self.device.device_wait_idle();
            self.device.destroy_command_pool(self.pool, None);
            for (buffer, memory) in self.buffers.iter().zip(&self.memory) {
                self.device.destroy_buffer(*buffer, None);
                self.device.free_memory(*memory, None);
            }
            self.device.destroy_device(None);
            self.instance.destroy_instance(None);
        }
    }
}

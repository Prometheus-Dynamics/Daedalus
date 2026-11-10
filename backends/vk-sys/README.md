# daedalus-vk-sys

The only crate that calls Vulkan or wgpu-hal's unsafe API. `backends/wgpu` is `#![forbid(unsafe_code)]` and reaches Vulkan through the safe functions here. Linux only: on other systems the crate has only the two lock types.

- `device`: opens a device with the extensions wgpu doesn't ask for (dma-buf import, 8-bit storage), and reports what a device was opened with. Device creation here needs the caller's driver lock, passed in as a guard.
- `memory`: storage buffers in memory the host can map, or in device memory without wgpu's zero-fill, and buffers imported from a dma-buf fd.
- `semaphore`: a sync fd imported as a semaphore.
- `submit`: commands recorded by the caller and submitted on wgpu's queue, under the queue's submit lock.

The crate doesn't own either lock. The driver lock is `backends/wgpu`'s, and each device's submit lock belongs to whoever holds the device. Both are passed in.

The functions that take a recording closure are `unsafe`: Vulkan can't check what the closure records. Everything else is safe to call.

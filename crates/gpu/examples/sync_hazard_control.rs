//! A deliberately wrong Vulkan program: the positive control for synchronization validation.
//!
//! It records two `vkCmdFillBuffer`s that write the same range of one buffer in one command
//! buffer, with no barrier between them. That is a write-after-write hazard: with synchronization
//! validation on, `VK_LAYER_KHRONOS_validation` reports it as `SYNC-HAZARD-WRITE-AFTER-WRITE`.
//! The device and Daedalus are not at fault, and nothing else depends on this example. It exists
//! so that a validation run with no `SYNC-HAZARD` lines can be told apart from a run where the
//! layer never checked for hazards.
//!
//! ```text
//! VK_INSTANCE_LAYERS=VK_LAYER_KHRONOS_validation VK_KHRONOS_VALIDATION_VALIDATE_SYNC=true \
//!   cargo run -p daedalus-gpu --features gpu-vulkan-raw --example sync_hazard_control
//! ```
//!
//! `./scripts/ci.sh vvl` runs it and fails unless a `SYNC-HAZARD` line appears. Without the layer
//! requested and installed, it exits 0 and says that nothing was checked.
#![allow(clippy::print_stdout)] // the report is the program's output

use daedalus_gpu::{GpuBackendKind, GpuOptions, GpuStorage, select_backend};

const LAYER: &str = "VK_LAYER_KHRONOS_validation";
const SIZE: u64 = 4096;
const FIRST: u32 = 0x1111_1111;
const SECOND: u32 = 0x2222_2222;

fn main() -> Result<(), String> {
    // The same device the raw Vulkan tests and examples select.
    let Ok(ctx) = select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        ..Default::default()
    }) else {
        nothing_checked("no Vulkan device through wgpu");
        return Ok(());
    };
    // SAFETY: the handles are only read here; the commands below use them on this context.
    let Some(raw) = (unsafe { ctx.raw_vulkan() }) else {
        nothing_checked("the context is not wgpu on Vulkan");
        return Ok(());
    };
    if let Err(reason) = layer_status(&raw.entry) {
        nothing_checked(&reason);
        return Ok(());
    }
    let sync_on = std::env::var("VK_KHRONOS_VALIDATION_VALIDATE_SYNC").is_ok_and(|v| v == "true");

    let storage = GpuStorage::new_host_cached(&ctx, SIZE, Some("sync-hazard-control"))
        .map_err(|e| format!("buffer: {e}"))?;
    // SAFETY: a buffer of this context's device; the ticket is waited for before `storage` drops.
    let buffer = unsafe { storage.raw_vk_buffer() }.ok_or("no Vulkan buffer")?;
    // SAFETY: the recorded fills are whole-buffer writes of `buffer` on its device, valid usage.
    let ticket = unsafe {
        ctx.submit_raw(|device, cmd| {
            // `cmd` is recording on `device`, and `buffer` covers the whole storage.
            device.cmd_fill_buffer(cmd, buffer.buffer, buffer.offset, buffer.size, FIRST);
            // The deliberate bug: no barrier separates the second write from the first.
            device.cmd_fill_buffer(cmd, buffer.buffer, buffer.offset, buffer.size, SECOND);
        })
    }
    .map_err(|e| format!("submit: {e}"))?;
    ticket.wait();

    println!(
        "sync_hazard_control: recorded two vkCmdFillBuffer writes to one {SIZE}-byte buffer, no barrier between them"
    );
    if sync_on {
        println!(
            "sync_hazard_control: synchronization validation is on, so the layer should report a write-after-write hazard above"
        );
    } else {
        nothing_checked("VK_KHRONOS_VALIDATION_VALIDATE_SYNC is not true, so no hazard is checked");
    }
    Ok(())
}

fn nothing_checked(reason: &str) {
    println!("sync_hazard_control: nothing was checked: {reason}");
}

/// `Ok` when the layer is requested in `VK_INSTANCE_LAYERS` and the loader finds it, so it is
/// enabled on wgpu's instance too.
fn layer_status(entry: &ash::Entry) -> Result<(), String> {
    let requested = std::env::var("VK_INSTANCE_LAYERS").unwrap_or_default();
    if !requested.split([':', ',']).any(|name| name.trim() == LAYER) {
        return Err(format!("{LAYER} is not requested in VK_INSTANCE_LAYERS"));
    }
    // SAFETY: lists the loader's layers; no handles are involved.
    let installed = unsafe { entry.enumerate_instance_layer_properties() }
        .map_err(|e| format!("layer enumeration failed: {e}"))?;
    let found = installed.iter().any(|layer| {
        layer
            .layer_name_as_c_str()
            .is_ok_and(|name| name.to_bytes() == LAYER.as_bytes())
    });
    if found {
        Ok(())
    } else {
        Err(format!(
            "the loader does not find {LAYER} (check VK_LAYER_PATH)"
        ))
    }
}

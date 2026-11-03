//! A GPU plugin's side of `daedalus-gpu`'s `plugin_submit` test: a `cdylib` with its own copy of
//! `daedalus-gpu` (and of its statics), working on the host's context. The host calls these
//! Rust-ABI functions through `libloading`; both sides are built from this workspace by the same
//! toolchain with the same `daedalus-gpu` features.

#[cfg(feature = "gpu-wgpu")]
mod wgpu_side {
    use daedalus_gpu::{GpuContextHandle, GpuStorage, SubmitLock};

    /// Whether `ctx`'s submit lock, as this copy of the crate reaches it, is the host's `host`.
    #[unsafe(no_mangle)]
    pub fn gpu_submit_plugin_same_lock(ctx: &GpuContextHandle, host: &SubmitLock) -> bool {
        ctx.submit_lock().is_some_and(|lock| lock.same_lock(host))
    }

    /// One empty submit through [`GpuContextHandle::submit`].
    #[unsafe(no_mangle)]
    pub fn gpu_submit_plugin_submit_empty(ctx: &GpuContextHandle) -> Result<(), String> {
        ctx.submit([]).map(drop).map_err(|e| e.to_string())
    }

    /// `iterations` GPU round trips, each submitting three ways: an upload (`with_data`), a
    /// buffer copy through [`GpuContextHandle::submit`], and a readback (a copy and `map_async`).
    /// Each must read back what it uploaded.
    #[unsafe(no_mangle)]
    pub fn gpu_submit_plugin_round_trips(
        ctx: &GpuContextHandle,
        iterations: usize,
    ) -> Result<(), String> {
        const LEN: usize = 64 << 10;
        let (device, _) = ctx.wgpu().ok_or("not a wgpu context")?;
        let dst =
            GpuStorage::new(ctx, LEN as u64, Some("plugin dst")).map_err(|e| e.to_string())?;
        let mut out = vec![0u8; LEN];
        for i in 0..iterations {
            let data: Vec<u8> = (0..LEN).map(|j| (i * 31 + j * 7) as u8).collect();
            let src =
                GpuStorage::with_data(ctx, &data, Some("plugin src")).map_err(|e| e.to_string())?;
            let mut encoder = device.create_command_encoder(&Default::default());
            encoder.copy_buffer_to_buffer(src.buffer(), 0, dst.buffer(), 0, LEN as u64);
            ctx.submit([encoder.finish()]).map_err(|e| e.to_string())?;
            dst.read_into(0, &mut out).map_err(|e| e.to_string())?;
            if out != data {
                return Err(format!(
                    "round trip {i}: the readback differs from the upload"
                ));
            }
        }
        Ok(())
    }
}

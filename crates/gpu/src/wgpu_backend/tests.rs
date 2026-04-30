use super::*;
#[cfg(feature = "gpu-async")]
use std::sync::Arc;
#[cfg(feature = "gpu-async")]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "gpu-async")]
use std::task::Poll;
#[cfg(feature = "gpu-async")]
use std::task::{Wake, Waker};
#[cfg(feature = "gpu-async")]
use std::{future::Future, pin::Pin, task::Context};

#[cfg(feature = "gpu-async")]
struct NoopWake;

#[cfg(feature = "gpu-async")]
impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

#[cfg(feature = "gpu-async")]
struct CountingWake {
    wakes: Arc<AtomicUsize>,
}

#[cfg(feature = "gpu-async")]
impl Wake for CountingWake {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn wgpu_backend_creates_resources() {
    let backend = WgpuBackend::new().unwrap();
    let buf = backend
        .create_buffer(&GpuRequest {
            usage: GpuUsage::UPLOAD,
            format: None,
            size_bytes: 1024,
        })
        .unwrap();
    assert_eq!(buf.size_bytes, 1024);
    let unaligned = backend
        .create_buffer(&GpuRequest {
            usage: GpuUsage::UPLOAD,
            format: None,
            size_bytes: 100,
        })
        .unwrap();
    assert_eq!(unaligned.size_bytes, 100);
    let img = backend
        .create_image(&GpuImageRequest {
            format: GpuFormat::Rgba8Unorm,
            width: 256,
            height: 256,
            samples: 1,
            usage: GpuUsage::RENDER_TARGET,
        })
        .unwrap();
    assert_eq!(img.width, 256);
}

#[cfg(feature = "gpu-async")]
#[test]
fn copy_limiter_async_acquire_yields_when_full() {
    let limiter = CopyLimiter::new(1);
    let guard = limiter.acquire();
    let mut future = Box::pin(limiter.acquire_async());
    let waker = Waker::from(Arc::new(NoopWake));
    let mut cx = Context::from_waker(&waker);

    assert!(matches!(Pin::new(&mut future).poll(&mut cx), Poll::Pending));
    assert_eq!(limiter.in_flight(), 1);

    drop(future);
    drop(guard);
    assert_eq!(limiter.in_flight(), 0);

    let async_guard = pollster::block_on(limiter.acquire_async());
    assert_eq!(limiter.in_flight(), 1);
    drop(async_guard);
    assert_eq!(limiter.in_flight(), 0);
}

#[cfg(feature = "gpu-async")]
#[test]
fn copy_limiter_async_acquire_wakes_active_waiter_after_cancelled_waiter() {
    let limiter = CopyLimiter::new(1);
    let guard = limiter.acquire();
    let active_wakes = Arc::new(AtomicUsize::new(0));
    let cancelled_wakes = Arc::new(AtomicUsize::new(0));
    let active_waker = Waker::from(Arc::new(CountingWake {
        wakes: active_wakes.clone(),
    }));
    let cancelled_waker = Waker::from(Arc::new(CountingWake {
        wakes: cancelled_wakes.clone(),
    }));
    let mut active_cx = Context::from_waker(&active_waker);
    let mut cancelled_cx = Context::from_waker(&cancelled_waker);
    let mut active = Box::pin(limiter.acquire_async());
    let mut cancelled = Box::pin(limiter.acquire_async());

    assert!(matches!(
        Pin::new(&mut active).poll(&mut active_cx),
        Poll::Pending
    ));
    assert_eq!(limiter.waiter_count(), 1);
    assert!(matches!(
        Pin::new(&mut cancelled).poll(&mut cancelled_cx),
        Poll::Pending
    ));
    assert_eq!(limiter.waiter_count(), 2);

    drop(cancelled);
    assert_eq!(limiter.waiter_count(), 1);
    drop(guard);

    assert!(active_wakes.load(Ordering::SeqCst) > 0);
    let active_guard = match Pin::new(&mut active).poll(&mut active_cx) {
        Poll::Ready(guard) => guard,
        Poll::Pending => panic!("active waiter should acquire after release"),
    };
    assert_eq!(limiter.in_flight(), 1);
    drop(active_guard);
    assert_eq!(limiter.in_flight(), 0);
}

#[test]
fn wgpu_backend_async_constructor_initializes_or_skips_without_adapter() {
    match pollster::block_on(WgpuBackend::new_async()) {
        Ok(backend) => {
            assert_eq!(backend.kind(), GpuBackendKind::Wgpu);
            assert!(!backend.adapter_info().name.is_empty());
        }
        Err(GpuError::AdapterUnavailable) => {}
        Err(err) => panic!("unexpected async wgpu init error: {err}"),
    }
}

#[test]
fn repeated_wgpu_backend_creation_registers_fresh_device_cache_keys() {
    for _ in 0..2 {
        match pollster::block_on(WgpuBackend::new_async()) {
            Ok(backend) => {
                assert_eq!(backend.kind(), GpuBackendKind::Wgpu);
                drop(backend);
            }
            Err(GpuError::AdapterUnavailable) => return,
            Err(err) => panic!("unexpected async wgpu init error: {err}"),
        }
    }
}

#[test]
fn staging_pool_config_normalizes_zero_counts() {
    let config = crate::WgpuStagingPoolConfig::new(0, 0, 1024);
    assert_eq!(config.max_size_classes, 1);
    assert_eq!(config.max_buffers_per_size, 1);
    assert_eq!(config.max_bytes, 1024);
}

#[cfg(all(feature = "gpu-async", feature = "gpu-wgpu"))]
#[test]
fn staging_pool_reuses_readback_buffers_and_tracks_stats() {
    let config = crate::WgpuStagingPoolConfig::new(4, 1, 4096);
    let Ok(backend) = WgpuBackend::new_with_staging_pool_config(config) else {
        return;
    };
    let req = GpuRequest {
        usage: GpuUsage::UPLOAD | GpuUsage::DOWNLOAD,
        format: None,
        size_bytes: 256,
    };
    let data = vec![7u8; req.size_bytes as usize];
    let handle = pollster::block_on(crate::GpuAsyncBackend::upload_buffer(&backend, &req, &data))
        .expect("upload buffer");

    let first = pollster::block_on(crate::GpuAsyncBackend::read_buffer(&backend, &handle))
        .expect("first readback");
    assert_eq!(first.len(), data.len());
    let after_first = backend.staging_pool_stats();
    assert_eq!(after_first.misses, 1);
    assert_eq!(after_first.returned, 1);
    assert_eq!(after_first.pooled_buffers, 1);
    assert_eq!(after_first.pooled_bytes, req.size_bytes);
    assert_eq!(after_first.max_size_classes, config.max_size_classes);
    assert_eq!(
        after_first.max_buffers_per_size,
        config.max_buffers_per_size
    );
    assert_eq!(after_first.max_bytes, config.max_bytes);

    let second = pollster::block_on(crate::GpuAsyncBackend::read_buffer(&backend, &handle))
        .expect("second readback");
    assert_eq!(second.len(), data.len());
    let after_second = backend.staging_pool_stats();
    assert_eq!(after_second.hits, 1);
    assert_eq!(after_second.misses, 1);
    assert_eq!(after_second.pooled_buffers, 1);
    assert_eq!(after_second.pooled_bytes, req.size_bytes);
}

#[cfg(all(feature = "gpu-async", feature = "gpu-wgpu"))]
#[test]
fn async_readback_completes_without_external_device_polling() {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = (|| -> Result<(), GpuError> {
            let backend = WgpuBackend::new_async().block_on()?;
            let req = GpuRequest {
                usage: GpuUsage::UPLOAD | GpuUsage::DOWNLOAD,
                format: None,
                size_bytes: 128,
            };
            let data = vec![11u8; req.size_bytes as usize];
            let handle = crate::GpuAsyncBackend::upload_buffer(&backend, &req, &data).block_on()?;
            let readback = crate::GpuAsyncBackend::read_buffer(&backend, &handle).block_on()?;
            if readback.len() != data.len() {
                return Err(GpuError::Internal(format!(
                    "readback len {} != {}",
                    readback.len(),
                    data.len()
                )));
            }
            Ok(())
        })();
        let _ = tx.send(result);
    });

    match rx.recv_timeout(std::time::Duration::from_secs(10)) {
        Ok(Ok(())) | Ok(Err(GpuError::AdapterUnavailable)) => {}
        Ok(Err(err)) => panic!("async readback failed: {err}"),
        Err(_) => panic!("async readback stalled without external device polling"),
    }
}

#[cfg(all(feature = "gpu-async", feature = "gpu-wgpu"))]
#[test]
fn async_shader_dispatch_readback_completes_without_external_device_polling() {
    static BINDINGS: &[crate::shader::BindingSpec] = &[
        crate::shader::BindingSpec {
            binding: 0,
            kind: crate::shader::BindingKind::Storage,
            access: crate::shader::Access::ReadOnly,
            invocation_stride: Some(4),
            texture_format: None,
            sample_type: None,
            view_dimension: None,
            sampler_kind: None,
        },
        crate::shader::BindingSpec {
            binding: 1,
            kind: crate::shader::BindingKind::Storage,
            access: crate::shader::Access::WriteOnly,
            invocation_stride: Some(4),
            texture_format: None,
            sample_type: None,
            view_dimension: None,
            sampler_kind: None,
        },
    ];
    static SPEC: crate::shader::ShaderSpec = crate::shader::ShaderSpec {
        name: "async-copy-readback",
        src: r#"
            @group(0) @binding(0)
            var<storage, read> input: array<u32>;

            @group(0) @binding(1)
            var<storage, read_write> output: array<u32>;

            @compute @workgroup_size(1)
            fn main(@builtin(global_invocation_id) id: vec3<u32>) {
                output[id.x] = input[id.x] + 1u;
            }
        "#,
        entry: "main",
        workgroup_size: Some([1, 1, 1]),
        bindings: BINDINGS,
    };

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = (|| -> Result<(), GpuError> {
            let input: [u32; 4] = [1, 2, 3, 4];
            let bindings = [
                crate::shader::ShaderBinding {
                    binding: 0,
                    kind: crate::shader::BindingKind::Storage,
                    access: crate::shader::Access::ReadOnly,
                    data: crate::shader::BindingData::Buffer(crate::shader::BufferInit::Bytes(
                        bytemuck::cast_slice(&input),
                    )),
                    readback: false,
                },
                crate::shader::ShaderBinding {
                    binding: 1,
                    kind: crate::shader::BindingKind::Storage,
                    access: crate::shader::Access::WriteOnly,
                    data: crate::shader::BindingData::Buffer(crate::shader::BufferInit::Empty(
                        std::mem::size_of_val(&input) as u64,
                    )),
                    readback: true,
                },
            ];
            let output = crate::shader::dispatch_shader_with_bindings_async(
                &SPEC,
                SPEC.src,
                &bindings,
                None,
                None,
                Some([input.len() as u32, 1, 1]),
            )
            .block_on()?;
            let bytes = output
                .buffers
                .get(&1)
                .ok_or_else(|| GpuError::Internal("missing output binding 1".into()))?;
            let values: &[u32] = bytemuck::cast_slice(bytes);
            if values != [2, 3, 4, 5] {
                return Err(GpuError::Internal(format!(
                    "unexpected shader readback: {values:?}"
                )));
            }
            Ok(())
        })();
        let _ = tx.send(result);
    });

    match rx.recv_timeout(std::time::Duration::from_secs(10)) {
        Ok(Ok(())) | Ok(Err(GpuError::AdapterUnavailable)) => {}
        Ok(Err(err)) => panic!("async shader dispatch readback failed: {err}"),
        Err(_) => panic!("async shader dispatch readback stalled without external device polling"),
    }
}

#[cfg(all(feature = "gpu-async", feature = "gpu-wgpu"))]
#[test]
fn async_shader_dispatch_maps_multiple_readbacks_together() {
    static BINDINGS: &[crate::shader::BindingSpec] = &[
        crate::shader::BindingSpec {
            binding: 0,
            kind: crate::shader::BindingKind::Storage,
            access: crate::shader::Access::ReadOnly,
            invocation_stride: Some(4),
            texture_format: None,
            sample_type: None,
            view_dimension: None,
            sampler_kind: None,
        },
        crate::shader::BindingSpec {
            binding: 1,
            kind: crate::shader::BindingKind::Storage,
            access: crate::shader::Access::WriteOnly,
            invocation_stride: Some(4),
            texture_format: None,
            sample_type: None,
            view_dimension: None,
            sampler_kind: None,
        },
        crate::shader::BindingSpec {
            binding: 2,
            kind: crate::shader::BindingKind::Storage,
            access: crate::shader::Access::WriteOnly,
            invocation_stride: Some(4),
            texture_format: None,
            sample_type: None,
            view_dimension: None,
            sampler_kind: None,
        },
    ];
    static SPEC: crate::shader::ShaderSpec = crate::shader::ShaderSpec {
        name: "async-multi-readback",
        src: r#"
            @group(0) @binding(0)
            var<storage, read> input: array<u32>;

            @group(0) @binding(1)
            var<storage, read_write> plus_one: array<u32>;

            @group(0) @binding(2)
            var<storage, read_write> doubled: array<u32>;

            @compute @workgroup_size(1)
            fn main(@builtin(global_invocation_id) id: vec3<u32>) {
                plus_one[id.x] = input[id.x] + 1u;
                doubled[id.x] = input[id.x] * 2u;
            }
        "#,
        entry: "main",
        workgroup_size: Some([1, 1, 1]),
        bindings: BINDINGS,
    };

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = (|| -> Result<(), GpuError> {
            let input: [u32; 4] = [3, 5, 8, 13];
            let out_size = std::mem::size_of_val(&input) as u64;
            let bindings = [
                crate::shader::ShaderBinding {
                    binding: 0,
                    kind: crate::shader::BindingKind::Storage,
                    access: crate::shader::Access::ReadOnly,
                    data: crate::shader::BindingData::Buffer(crate::shader::BufferInit::Bytes(
                        bytemuck::cast_slice(&input),
                    )),
                    readback: false,
                },
                crate::shader::ShaderBinding {
                    binding: 1,
                    kind: crate::shader::BindingKind::Storage,
                    access: crate::shader::Access::WriteOnly,
                    data: crate::shader::BindingData::Buffer(crate::shader::BufferInit::Empty(
                        out_size,
                    )),
                    readback: true,
                },
                crate::shader::ShaderBinding {
                    binding: 2,
                    kind: crate::shader::BindingKind::Storage,
                    access: crate::shader::Access::WriteOnly,
                    data: crate::shader::BindingData::Buffer(crate::shader::BufferInit::Empty(
                        out_size,
                    )),
                    readback: true,
                },
            ];
            let output = crate::shader::dispatch_shader_with_bindings_async(
                &SPEC,
                SPEC.src,
                &bindings,
                None,
                None,
                Some([input.len() as u32, 1, 1]),
            )
            .block_on()?;
            let plus_one: &[u32] = bytemuck::cast_slice(
                output
                    .buffers
                    .get(&1)
                    .ok_or_else(|| GpuError::Internal("missing output binding 1".into()))?,
            );
            let doubled: &[u32] = bytemuck::cast_slice(
                output
                    .buffers
                    .get(&2)
                    .ok_or_else(|| GpuError::Internal("missing output binding 2".into()))?,
            );
            if plus_one != [4, 6, 9, 14] || doubled != [6, 10, 16, 26] {
                return Err(GpuError::Internal(format!(
                    "unexpected multi-readback values: plus_one={plus_one:?} doubled={doubled:?}"
                )));
            }
            Ok(())
        })();
        let _ = tx.send(result);
    });

    match rx.recv_timeout(std::time::Duration::from_secs(10)) {
        Ok(Ok(())) | Ok(Err(GpuError::AdapterUnavailable)) => {}
        Ok(Err(err)) => panic!("async multi-readback dispatch failed: {err}"),
        Err(_) => panic!("async multi-readback dispatch stalled"),
    }
}

#[cfg(all(feature = "gpu-async", feature = "gpu-wgpu"))]
#[test]
fn async_texture_readback_completes_without_blocking_caller_thread() {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = (|| -> Result<(), GpuError> {
            let ctx = crate::select_backend_async(&GpuOptions {
                preferred_backend: Some(GpuBackendKind::Wgpu),
                ..Default::default()
            })
            .block_on()?;
            if ctx.backend_kind() != GpuBackendKind::Wgpu {
                return Err(GpuError::AdapterUnavailable);
            }
            let req = GpuImageRequest {
                format: GpuFormat::Rgba8Unorm,
                width: 2,
                height: 1,
                samples: 1,
                usage: GpuUsage::UPLOAD | GpuUsage::DOWNLOAD,
            };
            let bytes = vec![1, 2, 3, 255, 4, 5, 6, 255];
            let handle = ctx.upload_texture(&req, &bytes)?;
            let readback = ctx.read_texture_async(handle).block_on()?;
            if readback != bytes {
                return Err(GpuError::Internal(format!(
                    "unexpected texture readback: {readback:?}"
                )));
            }
            Ok(())
        })();
        let _ = tx.send(result);
    });

    match rx.recv_timeout(std::time::Duration::from_secs(10)) {
        Ok(Ok(())) | Ok(Err(GpuError::AdapterUnavailable)) => {}
        Ok(Err(err)) => panic!("async texture readback failed: {err}"),
        Err(_) => panic!("async texture readback stalled"),
    }
}

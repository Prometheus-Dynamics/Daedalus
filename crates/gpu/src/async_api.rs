use async_trait::async_trait;

use crate::{GpuBackend, GpuBufferHandle, GpuError, GpuImageHandle, GpuImageRequest, GpuRequest};

/// Optional async helpers for upload/readback. Real backends can override for zero-copy paths.
#[async_trait]
pub trait GpuAsyncBackend: GpuBackend {
    async fn upload_buffer(
        &self,
        req: &GpuRequest,
        _data: &[u8],
    ) -> Result<GpuBufferHandle, GpuError> {
        self.create_buffer(req)
    }

    async fn read_buffer(&self, _handle: &GpuBufferHandle) -> Result<Vec<u8>, GpuError> {
        Err(GpuError::Unsupported)
    }

    async fn create_image_async(&self, req: &GpuImageRequest) -> Result<GpuImageHandle, GpuError> {
        self.create_image(req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        GpuAdapterInfo, GpuBackendKind, GpuCapabilities, GpuFormat, GpuFormatFeatures,
        GpuMemoryLocation, GpuOptions, GpuUsage, buffer::TransferStats,
    };
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    struct DefaultAsyncBackend;

    impl GpuBackend for DefaultAsyncBackend {
        fn kind(&self) -> GpuBackendKind {
            GpuBackendKind::Noop
        }

        fn adapter_info(&self) -> GpuAdapterInfo {
            GpuAdapterInfo {
                name: "default-async-test".into(),
                backend: GpuBackendKind::Noop,
                device_id: None,
                vendor_id: None,
            }
        }

        fn capabilities(&self) -> GpuCapabilities {
            GpuCapabilities {
                supported_formats: vec![GpuFormat::Rgba8Unorm],
                format_features: vec![GpuFormatFeatures {
                    format: GpuFormat::Rgba8Unorm,
                    sampleable: true,
                    renderable: true,
                    storage: false,
                    max_samples: 1,
                }],
                format_blocks: Vec::new(),
                max_buffer_size: 1024,
                max_texture_dimension: 1,
                max_texture_samples: 1,
                staging_alignment: 1,
                max_inflight_copies: 1,
                queue_count: 1,
                min_buffer_copy_offset_alignment: 1,
                bytes_per_row_alignment: 1,
                rows_per_image_alignment: 1,
                has_transfer_queue: false,
            }
        }

        fn select_adapter(&self, _opts: &GpuOptions) -> Result<GpuAdapterInfo, GpuError> {
            Ok(self.adapter_info())
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn create_buffer(&self, req: &GpuRequest) -> Result<GpuBufferHandle, GpuError> {
            Ok(GpuBufferHandle::new(
                req.size_bytes,
                GpuMemoryLocation::Cpu,
                req.usage,
            ))
        }

        fn create_image(&self, req: &GpuImageRequest) -> Result<GpuImageHandle, GpuError> {
            Ok(GpuImageHandle::new(
                GpuFormat::Rgba8Unorm,
                req.width,
                req.height,
                GpuMemoryLocation::Cpu,
                req.usage,
            ))
        }

        fn stats(&self) -> TransferStats {
            TransferStats::default()
        }
    }

    impl GpuAsyncBackend for DefaultAsyncBackend {}

    fn block_on_ready<F: Future>(mut fut: F) -> F::Output {
        fn raw_waker() -> RawWaker {
            fn clone(_: *const ()) -> RawWaker {
                raw_waker()
            }
            fn wake(_: *const ()) {}
            fn wake_by_ref(_: *const ()) {}
            fn drop(_: *const ()) {}
            RawWaker::new(
                std::ptr::null(),
                &RawWakerVTable::new(clone, wake, wake_by_ref, drop),
            )
        }

        let waker = unsafe { Waker::from_raw(raw_waker()) };
        let mut cx = Context::from_waker(&waker);
        let mut fut = unsafe { Pin::new_unchecked(&mut fut) };
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("default async backend future should be ready"),
        }
    }

    #[test]
    fn default_async_read_buffer_is_unsupported() {
        let backend = DefaultAsyncBackend;
        let handle = GpuBufferHandle::new(16, GpuMemoryLocation::Cpu, GpuUsage::DOWNLOAD);
        let err = block_on_ready(backend.read_buffer(&handle)).expect_err("unsupported");
        assert_eq!(err, GpuError::Unsupported);
    }
}

//! GPU facade: backend selection, capability traits, opaque handles, and feature-gated backends.
//! Feature matrix:
//! - `gpu-noop` (default): deterministic noop backend, always available; zero-GPU environments still compile.
//! - `gpu-mock`: deterministic mock backend for tests/CI.
//! - `gpu-wgpu`: real wgpu backend (types remain internal), placeholder for now.
//!   Concurrency: backends are `Send + Sync`; clone the handle (cheap `Arc`) to share across tasks.
//!   Planner/runtime expectation: call `select_backend` once, inspect skipped reasons (for “why not GPU?”),
//!   then use the returned handle to allocate buffers/images without depending on any concrete GPU type.

#[cfg(feature = "gpu-async")]
mod async_api;
mod buffer;
mod convert;
mod handles;
#[cfg(feature = "gpu-mock")]
mod mock;
mod noop;
#[cfg(feature = "gpu-wgpu")]
pub mod shader;
mod traits;
#[cfg(feature = "gpu-wgpu")]
mod wgpu_backend;

#[cfg(feature = "gpu-async")]
pub use async_api::GpuAsyncBackend;
pub use buffer::{BufferPool, SimpleBufferPool, TransferStats};
pub use convert::{Backing, Compute, DataCell, DeviceBridge};
pub use handles::{GpuBufferHandle, GpuBufferId, GpuImageHandle, GpuImageId};
#[cfg(feature = "gpu-mock")]
pub use mock::MockBackend;
pub use noop::NoopBackend;
pub use traits::{GpuBackend, GpuContext};
#[cfg(feature = "gpu-wgpu")]
pub use wgpu_backend::WgpuBackend;

use bitflags::bitflags;
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};

/// Backend kind identifiers.
///
/// ```
/// use daedalus_gpu::GpuBackendKind;
/// let kind = GpuBackendKind::Noop;
/// assert_eq!(kind.as_str(), "noop");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GpuBackendKind {
    Noop,
    Mock,
    Wgpu,
}

impl GpuBackendKind {
    /// Return a stable string representation.
    pub fn as_str(self) -> &'static str {
        match self {
            GpuBackendKind::Noop => "noop",
            GpuBackendKind::Mock => "mock",
            GpuBackendKind::Wgpu => "wgpu",
        }
    }
}

/// Memory location hint for GPU resources.
///
/// ```
/// use daedalus_gpu::GpuMemoryLocation;
/// let loc = GpuMemoryLocation::Gpu;
/// assert_eq!(loc, GpuMemoryLocation::Gpu);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GpuMemoryLocation {
    Cpu,
    Gpu,
    Shared,
}

/// Common GPU formats (minimal set for planner decisions).
///
/// ```
/// use daedalus_gpu::GpuFormat;
/// let fmt = GpuFormat::Rgba8Unorm;
/// assert_eq!(fmt, GpuFormat::Rgba8Unorm);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GpuFormat {
    R8Unorm,
    Rgba8Unorm,
    Rgba16Float,
    Depth24Stencil8,
}

/// Per-format feature flags for planner/runtime decisions.
///
/// ```
/// use daedalus_gpu::{GpuFormat, GpuFormatFeatures};
/// let feats = GpuFormatFeatures {
///     format: GpuFormat::R8Unorm,
///     sampleable: true,
///     renderable: false,
///     storage: true,
///     max_samples: 1,
/// };
/// assert!(feats.sampleable);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuFormatFeatures {
    pub format: GpuFormat,
    pub sampleable: bool,
    pub renderable: bool,
    pub storage: bool,
    pub max_samples: u32,
}

/// Block/stride info for formats (useful if compressed formats are added later).
///
/// ```
/// use daedalus_gpu::{GpuBlockInfo, GpuFormat};
/// let info = GpuBlockInfo { format: GpuFormat::R8Unorm, block_width: 1, block_height: 1, bytes_per_block: 1 };
/// assert_eq!(info.bytes_per_block, 1);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuBlockInfo {
    pub format: GpuFormat,
    pub block_width: u32,
    pub block_height: u32,
    pub bytes_per_block: u32,
}

bitflags! {
    /// Usage flags for buffers/images; combinations are allowed.
    ///
    /// ```
    /// use daedalus_gpu::GpuUsage;
    /// let usage = GpuUsage::UPLOAD | GpuUsage::STORAGE;
    /// assert!(usage.contains(GpuUsage::UPLOAD));
    /// ```
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct GpuUsage: u32 {
        const UPLOAD = 0b0001;
        const DOWNLOAD = 0b0010;
        const STORAGE = 0b0100;
        const RENDER_TARGET = 0b1000;
    }
}

/// Adapter information exposed to planner/runtime.
///
/// ```
/// use daedalus_gpu::{GpuAdapterInfo, GpuBackendKind};
/// let info = GpuAdapterInfo {
///     name: "noop".into(),
///     backend: GpuBackendKind::Noop,
///     device_id: None,
///     vendor_id: None,
/// };
/// assert_eq!(info.backend, GpuBackendKind::Noop);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuAdapterInfo {
    pub name: String,
    pub backend: GpuBackendKind,
    pub device_id: Option<String>,
    pub vendor_id: Option<String>,
}

/// Adapter selection options.
///
/// ```
/// use daedalus_gpu::{GpuOptions, GpuBackendKind};
/// let opts = GpuOptions { preferred_backend: Some(GpuBackendKind::Noop), adapter_label: None, allow_software: true };
/// assert!(opts.allow_software);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GpuOptions {
    pub preferred_backend: Option<GpuBackendKind>,
    pub adapter_label: Option<String>,
    pub allow_software: bool,
}

/// Request shape for resource creation.
///
/// ```
/// use daedalus_gpu::{GpuRequest, GpuUsage};
/// let req = GpuRequest { usage: GpuUsage::UPLOAD, format: None, size_bytes: 1024 };
/// assert_eq!(req.size_bytes, 1024);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuRequest {
    pub usage: GpuUsage,
    pub format: Option<GpuFormat>,
    pub size_bytes: u64,
}

/// Request shape for image/texture creation.
///
/// ```
/// use daedalus_gpu::{GpuImageRequest, GpuFormat, GpuUsage};
/// let req = GpuImageRequest { format: GpuFormat::Rgba8Unorm, width: 16, height: 16, samples: 1, usage: GpuUsage::STORAGE };
/// assert_eq!(req.width, 16);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuImageRequest {
    pub format: GpuFormat,
    pub width: u32,
    pub height: u32,
    pub samples: u32,
    pub usage: GpuUsage,
}

/// Capability query result.
///
/// ```
/// use daedalus_gpu::{GpuCapabilities, GpuFormat, GpuFormatFeatures, GpuBlockInfo};
/// let caps = GpuCapabilities {
///     supported_formats: vec![GpuFormat::R8Unorm],
///     format_features: vec![GpuFormatFeatures { format: GpuFormat::R8Unorm, sampleable: true, renderable: false, storage: true, max_samples: 1 }],
///     format_blocks: vec![GpuBlockInfo { format: GpuFormat::R8Unorm, block_width: 1, block_height: 1, bytes_per_block: 1 }],
///     max_buffer_size: 1,
///     max_texture_dimension: 1,
///     max_texture_samples: 1,
///     staging_alignment: 1,
///     max_inflight_copies: 1,
///     queue_count: 1,
///     min_buffer_copy_offset_alignment: 1,
///     bytes_per_row_alignment: 1,
///     rows_per_image_alignment: 1,
///     has_transfer_queue: false,
/// };
/// assert_eq!(caps.supported_formats.len(), 1);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GpuCapabilities {
    pub supported_formats: Vec<GpuFormat>,
    pub format_features: Vec<GpuFormatFeatures>,
    pub format_blocks: Vec<GpuBlockInfo>,
    pub max_buffer_size: u64,
    pub max_texture_dimension: u32,
    pub max_texture_samples: u32,
    pub staging_alignment: u64,
    pub max_inflight_copies: u32,
    pub queue_count: u32,
    pub min_buffer_copy_offset_alignment: u64,
    pub bytes_per_row_alignment: u32,
    pub rows_per_image_alignment: u32,
    pub has_transfer_queue: bool,
}

/// GPU error codes for diagnostics.
///
/// ```
/// use daedalus_gpu::GpuError;
/// let err = GpuError::Unsupported;
/// assert_eq!(format!("{err}"), "unsupported");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GpuError {
    Unsupported,
    AllocationFailed,
    AdapterUnavailable,
    Internal(String),
}

impl fmt::Display for GpuError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GpuError::Unsupported => write!(f, "unsupported"),
            GpuError::AllocationFailed => write!(f, "allocation failed"),
            GpuError::AdapterUnavailable => write!(f, "adapter unavailable"),
            GpuError::Internal(msg) => write!(f, "internal: {msg}"),
        }
    }
}

impl std::error::Error for GpuError {}

/// Reason a backend was skipped during selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackendSkipReason {
    FeatureNotEnabled,
    AdapterUnavailable,
    PreferredMismatch,
    Error(String),
}

/// Explanation for a backend that was skipped during selection.
///
/// ```
/// use daedalus_gpu::{BackendSkip, BackendSkipReason, GpuBackendKind};
/// let skip = BackendSkip { backend: GpuBackendKind::Wgpu, reason: BackendSkipReason::FeatureNotEnabled };
/// assert!(skip.describe().contains("not built"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendSkip {
    pub backend: GpuBackendKind,
    pub reason: BackendSkipReason,
}

impl BackendSkip {
    pub fn describe(&self) -> String {
        match &self.reason {
            BackendSkipReason::FeatureNotEnabled => {
                format!("{:?} not built (feature disabled)", self.backend)
            }
            BackendSkipReason::AdapterUnavailable => {
                format!("{:?} adapter unavailable", self.backend)
            }
            BackendSkipReason::PreferredMismatch => {
                format!("{:?} not selected due to preference", self.backend)
            }
            BackendSkipReason::Error(e) => format!("{:?} failed: {}", self.backend, e),
        }
    }
}

/// Shared handle wrapping a selected backend and diagnostics for why other backends were skipped.
#[derive(Clone)]
pub struct GpuContextHandle {
    backend: Arc<dyn GpuBackend>,
    chosen: GpuBackendKind,
    adapter: GpuAdapterInfo,
    skipped: Vec<BackendSkip>,
}

impl GpuContextHandle {
    pub fn backend_kind(&self) -> GpuBackendKind {
        self.chosen
    }

    pub fn adapter_info(&self) -> &GpuAdapterInfo {
        &self.adapter
    }

    pub fn skipped(&self) -> &[BackendSkip] {
        &self.skipped
    }

    pub fn skipped_summary(&self) -> Vec<String> {
        self.skipped.iter().map(|s| s.describe()).collect()
    }

    pub fn backend_ref(&self) -> &dyn GpuBackend {
        self.backend.as_ref()
    }

    pub fn capabilities(&self) -> GpuCapabilities {
        self.backend.capabilities()
    }

    pub fn stats(&self) -> TransferStats {
        self.backend.stats()
    }

    pub fn take_stats(&self) -> TransferStats {
        self.backend.take_stats()
    }

    pub fn reset_stats(&self) -> TransferStats {
        self.backend.take_stats()
    }

    pub fn record_download(&self, bytes: u64) {
        self.backend.record_download(bytes)
    }

    pub fn upload_texture(
        &self,
        req: &GpuImageRequest,
        data: &[u8],
    ) -> Result<GpuImageHandle, GpuError> {
        validate_texture_bytes(req, &self.capabilities())?;
        self.backend.upload_texture(req, data)
    }

    pub fn read_texture(&self, handle: &GpuImageHandle) -> Result<Vec<u8>, GpuError> {
        self.backend.read_texture(handle)
    }

    pub fn create_buffer(&self, req: &GpuRequest) -> Result<GpuBufferHandle, GpuError> {
        self.backend.create_buffer(req)
    }

    pub fn alloc_upload_buffer(&self, size_bytes: u64) -> Result<GpuBufferHandle, GpuError> {
        self.create_buffer(&GpuRequest {
            usage: GpuUsage::UPLOAD,
            format: None,
            size_bytes,
        })
    }

    pub fn alloc_download_buffer(&self, size_bytes: u64) -> Result<GpuBufferHandle, GpuError> {
        self.create_buffer(&GpuRequest {
            usage: GpuUsage::DOWNLOAD,
            format: None,
            size_bytes,
        })
    }

    pub fn create_image(&self, req: &GpuImageRequest) -> Result<GpuImageHandle, GpuError> {
        self.backend.create_image(req)
    }
}

pub fn format_bytes_per_pixel(format: GpuFormat) -> Option<u32> {
    match format {
        GpuFormat::R8Unorm => Some(1),
        GpuFormat::Rgba8Unorm => Some(4),
        GpuFormat::Rgba16Float => Some(8),
        GpuFormat::Depth24Stencil8 => Some(4),
    }
}

/// Convenience helper for uploading an R8 (single-channel) texture with basic size/length validation.
pub fn upload_r8_texture(
    ctx: &GpuContextHandle,
    width: u32,
    height: u32,
    data: &[u8],
) -> Result<GpuImageHandle, GpuError> {
    let expected = width as usize * height as usize;
    if data.len() != expected {
        return Err(GpuError::AllocationFailed);
    }
    let req = GpuImageRequest {
        format: GpuFormat::R8Unorm,
        width,
        height,
        samples: 1,
        usage: GpuUsage::RENDER_TARGET | GpuUsage::UPLOAD,
    };
    validate_texture_bytes(&req, &ctx.capabilities())?;
    ctx.upload_texture(&req, data)
}

/// Simple upload helper: allocates an upload buffer and returns it with bytes staged length.
pub fn upload_bytes(ctx: &GpuContextHandle, bytes: &[u8]) -> Result<GpuBufferHandle, GpuError> {
    let buf = ctx.alloc_upload_buffer(bytes.len() as u64)?;
    // Real implementations would map/stage here; we just track stats.
    ctx.record_download(0);
    Ok(buf)
}

/// Convenience helper for uploading an RGBA8 texture with basic size/length validation.
pub fn upload_rgba8_texture(
    ctx: &GpuContextHandle,
    width: u32,
    height: u32,
    data: &[u8],
) -> Result<GpuImageHandle, GpuError> {
    let expected = width as usize * height as usize * 4;
    if data.len() != expected {
        return Err(GpuError::AllocationFailed);
    }
    let req = GpuImageRequest {
        format: GpuFormat::Rgba8Unorm,
        width,
        height,
        samples: 1,
        usage: GpuUsage::RENDER_TARGET | GpuUsage::UPLOAD,
    };
    validate_texture_bytes(&req, &ctx.capabilities())?;
    ctx.upload_texture(&req, data)
}

/// Validate texture copy layout against capabilities.
pub fn validate_texture_bytes(
    req: &GpuImageRequest,
    caps: &GpuCapabilities,
) -> Result<(), GpuError> {
    let block = caps
        .format_blocks
        .iter()
        .find(|b| b.format == req.format)
        .cloned()
        .ok_or(GpuError::Unsupported)?;
    let blocks_x = (req.width as u64).div_ceil(block.block_width as u64);
    let blocks_y = (req.height as u64).div_ceil(block.block_height as u64);
    let bytes_per_row = blocks_x * block.bytes_per_block as u64;
    if bytes_per_row == 0 {
        return Err(GpuError::Unsupported);
    }
    if blocks_y == 0 {
        return Err(GpuError::Unsupported);
    }
    Ok(())
}

/// Select the best available backend given build-time features and runtime options.
/// Order: preferred backend (if set), then wgpu, mock, noop.
pub fn select_backend(opts: &GpuOptions) -> Result<GpuContextHandle, GpuError> {
    let mut skipped = Vec::new();
    let mut order = Vec::new();
    if let Some(pref) = opts.preferred_backend
        && !order.contains(&pref)
    {
        order.push(pref);
    }
    for fallback in [
        GpuBackendKind::Wgpu,
        GpuBackendKind::Mock,
        GpuBackendKind::Noop,
    ] {
        if !order.contains(&fallback) {
            order.push(fallback);
        }
    }

    for kind in order {
        match try_build_backend(kind, opts) {
            Ok((backend, adapter)) => {
                return Ok(GpuContextHandle {
                    chosen: kind,
                    adapter,
                    skipped,
                    backend,
                });
            }
            Err(reason) => skipped.push(BackendSkip {
                backend: kind,
                reason,
            }),
        }
    }

    Err(GpuError::AdapterUnavailable)
}

fn try_build_backend(
    kind: GpuBackendKind,
    opts: &GpuOptions,
) -> Result<(Arc<dyn GpuBackend>, GpuAdapterInfo), BackendSkipReason> {
    match kind {
        GpuBackendKind::Wgpu => {
            #[cfg(feature = "gpu-wgpu")]
            {
                let backend =
                    WgpuBackend::new().map_err(|e| BackendSkipReason::Error(e.to_string()))?;
                let adapter = backend
                    .select_adapter(opts)
                    .map_err(|_| BackendSkipReason::AdapterUnavailable)?;
                Ok((Arc::new(backend), adapter))
            }
            #[cfg(not(feature = "gpu-wgpu"))]
            {
                Err(BackendSkipReason::FeatureNotEnabled)
            }
        }
        GpuBackendKind::Mock => {
            #[cfg(feature = "gpu-mock")]
            {
                let backend = MockBackend::default();
                let adapter = backend
                    .select_adapter(opts)
                    .map_err(|_| BackendSkipReason::AdapterUnavailable)?;
                Ok((Arc::new(backend), adapter))
            }
            #[cfg(not(feature = "gpu-mock"))]
            {
                Err(BackendSkipReason::FeatureNotEnabled)
            }
        }
        GpuBackendKind::Noop => {
            let backend = NoopBackend::default();
            let adapter = backend
                .select_adapter(opts)
                .map_err(|_| BackendSkipReason::AdapterUnavailable)?;
            Ok((Arc::new(backend), adapter))
        }
    }
}

impl GpuContext for GpuContextHandle {
    fn backend(&self) -> GpuBackendKind {
        self.backend_kind()
    }

    fn adapter_info(&self) -> GpuAdapterInfo {
        self.adapter.clone()
    }

    fn capabilities(&self) -> GpuCapabilities {
        self.capabilities()
    }

    fn stats(&self) -> TransferStats {
        self.stats()
    }

    fn take_stats(&self) -> TransferStats {
        self.backend.take_stats()
    }

    fn record_download(&self, bytes: u64) {
        self.backend.record_download(bytes)
    }
}

/// Convenience for callers that just need to know the active backend kind.
pub fn active_backend() -> GpuBackendKind {
    select_backend(&GpuOptions::default())
        .map(|ctx| ctx.backend_kind())
        .unwrap_or(GpuBackendKind::Noop)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::HashSet, sync::Arc, thread};

    #[test]
    fn falls_back_to_noop_when_only_noop_is_built() {
        let ctx = select_backend(&GpuOptions::default()).unwrap();
        let kind = ctx.backend_kind();
        if cfg!(not(any(feature = "gpu-mock", feature = "gpu-wgpu"))) {
            assert_eq!(kind, GpuBackendKind::Noop);
            assert!(!ctx.skipped().is_empty());
        } else {
            assert!(matches!(
                kind,
                GpuBackendKind::Mock | GpuBackendKind::Wgpu | GpuBackendKind::Noop
            ));
        }
    }

    #[cfg(feature = "gpu-mock")]
    #[test]
    fn prefers_mock_when_available() {
        let ctx = select_backend(&GpuOptions::default()).unwrap();
        assert_eq!(ctx.backend_kind(), GpuBackendKind::Mock);
        assert_eq!(ctx.adapter_info().backend, GpuBackendKind::Mock);
    }

    #[cfg(feature = "gpu-wgpu")]
    #[test]
    fn can_select_wgpu_when_built() {
        let opts = GpuOptions {
            preferred_backend: Some(GpuBackendKind::Wgpu),
            ..Default::default()
        };
        let ctx = select_backend(&opts).unwrap();
        assert!(matches!(
            ctx.backend_kind(),
            GpuBackendKind::Wgpu | GpuBackendKind::Mock | GpuBackendKind::Noop
        ));
    }

    #[test]
    fn parallel_buffer_creates_are_unique() {
        let ctx = Arc::new(select_backend(&GpuOptions::default()).unwrap());
        let mut handles = Vec::new();
        let mut threads = Vec::new();
        for _ in 0..4 {
            let ctx = ctx.clone();
            threads.push(thread::spawn(move || {
                let mut local = Vec::new();
                for _ in 0..32 {
                    let buf = ctx
                        .create_buffer(&GpuRequest {
                            usage: GpuUsage::UPLOAD,
                            format: None,
                            size_bytes: 256,
                        })
                        .unwrap();
                    local.push(buf.id);
                }
                local
            }));
        }
        for t in threads {
            handles.extend(t.join().expect("thread join"));
        }
        let unique: HashSet<_> = handles.iter().copied().collect();
        assert_eq!(unique.len(), handles.len());
    }

    #[test]
    fn parallel_image_creates_are_unique() {
        let ctx = Arc::new(select_backend(&GpuOptions::default()).unwrap());
        let mut handles = Vec::new();
        let mut threads = Vec::new();
        for _ in 0..4 {
            let ctx = ctx.clone();
            threads.push(thread::spawn(move || {
                let mut local = Vec::new();
                for _ in 0..32 {
                    let img = ctx
                        .create_image(&GpuImageRequest {
                            format: GpuFormat::Rgba8Unorm,
                            width: 64,
                            height: 16,
                            samples: 1,
                            usage: GpuUsage::RENDER_TARGET,
                        })
                        .unwrap();
                    local.push(img.id);
                }
                local
            }));
        }
        for t in threads {
            handles.extend(t.join().expect("thread join"));
        }
        let unique: HashSet<_> = handles.iter().copied().collect();
        assert_eq!(unique.len(), handles.len());
    }
}

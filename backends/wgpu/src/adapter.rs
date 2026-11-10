//! Which adapter to open.

/// A graphics API an adapter is reached through.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Api {
    Vulkan,
    Metal,
    Dx12,
    Gl,
    Other,
}

/// A set of [`Api`]s to look for adapters on.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Apis(u8);

impl Apis {
    pub const VULKAN: Apis = Apis(1);
    pub const METAL: Apis = Apis(2);
    pub const DX12: Apis = Apis(4);
    pub const GL: Apis = Apis(8);
    pub const ALL: Apis = Apis(15);

    pub const fn union(self, other: Apis) -> Apis {
        Apis(self.0 | other.0)
    }

    pub const fn contains(self, other: Apis) -> bool {
        self.0 & other.0 == other.0
    }

    /// Parses a comma-separated list such as `vulkan,gl`, the form of `WGPU_BACKEND`. Unknown
    /// names are skipped, and a list with no known name is `None`.
    pub fn from_names(names: &str) -> Option<Apis> {
        let mut out = 0;
        for name in names.split(',') {
            out |= match name.trim().to_ascii_lowercase().as_str() {
                "vulkan" | "vk" => Apis::VULKAN.0,
                "metal" | "mtl" => Apis::METAL.0,
                "dx12" | "d3d12" => Apis::DX12.0,
                "gl" | "gles" | "opengl" => Apis::GL.0,
                _ => 0,
            };
        }
        (out != 0).then_some(Apis(out))
    }

    pub(crate) fn to_wgpu(self) -> wgpu::Backends {
        let mut out = wgpu::Backends::empty();
        out.set(wgpu::Backends::VULKAN, self.contains(Apis::VULKAN));
        out.set(wgpu::Backends::METAL, self.contains(Apis::METAL));
        out.set(wgpu::Backends::DX12, self.contains(Apis::DX12));
        out.set(wgpu::Backends::GL, self.contains(Apis::GL));
        out
    }
}

impl Default for Apis {
    fn default() -> Self {
        Apis::ALL
    }
}

/// What kind of device an adapter says it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DeviceKind {
    Discrete,
    Integrated,
    Virtual,
    Cpu,
    Other,
}

/// An adapter, as its driver describes it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AdapterInfo {
    pub name: String,
    /// The PCI vendor id, or 0.
    pub vendor: u32,
    pub device: u32,
    pub api: Api,
    pub kind: DeviceKind,
}

impl AdapterInfo {
    pub(crate) fn from_wgpu(info: &wgpu::AdapterInfo) -> AdapterInfo {
        AdapterInfo {
            name: info.name.clone(),
            vendor: info.vendor,
            device: info.device,
            api: match info.backend {
                wgpu::Backend::Vulkan => Api::Vulkan,
                wgpu::Backend::Metal => Api::Metal,
                wgpu::Backend::Dx12 => Api::Dx12,
                wgpu::Backend::Gl => Api::Gl,
                _ => Api::Other,
            },
            kind: match info.device_type {
                wgpu::DeviceType::DiscreteGpu => DeviceKind::Discrete,
                wgpu::DeviceType::IntegratedGpu => DeviceKind::Integrated,
                wgpu::DeviceType::VirtualGpu => DeviceKind::Virtual,
                wgpu::DeviceType::Cpu => DeviceKind::Cpu,
                wgpu::DeviceType::Other => DeviceKind::Other,
            },
        }
    }
}

/// How [`WgpuBackend::open`](crate::WgpuBackend::open) picks its adapter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Options {
    /// Where to look. If nothing is found there, every API is tried.
    pub apis: Apis,
    /// Only adapters of this PCI vendor.
    pub vendor: Option<u32>,
    /// Only adapters whose name contains this, ignoring case. Naming an adapter opens a device of
    /// this backend's own, not the one the process shares.
    pub adapter: Option<String>,
    /// Only this one of the adapters that pass the rest, counted from 0 with the best first.
    /// Like naming an adapter, this opens a device of this backend's own.
    pub index: Option<usize>,
    /// Where a graph's host ports are, for the nodes of this backend. By default they are on
    /// the host: what the host feeds is uploaded, and what it takes was read back. With this
    /// set they are on the device: the host feeds and takes [`Storage`](crate::Storage), and
    /// nothing is copied for it.
    pub host_ports_on_device: bool,
    /// What an earlier run learned about the adapter, so planning can use it before a device
    /// opens.
    pub known: Option<crate::CapsRecord>,
    /// How the device sizes its memory blocks.
    pub hints: crate::MemoryHints,
    /// Limits of the pool of readback buffers.
    pub staging: crate::StagingConfig,
    /// How many scratch buffers and textures the device keeps.
    pub temp_pool: crate::TempPoolLimits,
    /// The bound on any wait the backend makes. One that outlives it fails and marks the device
    /// lost.
    pub wait_timeout: std::time::Duration,
    /// Submissions a device has unfinished at once. Past it, a run is told to wait for the
    /// oldest.
    #[doc(hidden)]
    pub max_in_flight: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            apis: Apis::default(),
            vendor: None,
            adapter: None,
            index: None,
            host_ports_on_device: false,
            known: None,
            hints: crate::MemoryHints::default(),
            staging: crate::StagingConfig::default(),
            temp_pool: crate::TempPoolLimits::default(),
            wait_timeout: crate::wait::DEFAULT_WAIT_TIMEOUT,
            max_in_flight: crate::inflight::DEFAULT_IN_FLIGHT,
        }
    }
}

/// What ranking looks at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Candidate<'a> {
    pub api: Api,
    pub kind: DeviceKind,
    pub name: &'a str,
    /// RGBA8 can be a storage binding.
    pub storage: bool,
    /// RGBA8 can be sampled.
    pub sampled: bool,
    /// RGBA8 can be copied both ways.
    pub copy: bool,
}

/// Whether a name is one of the CPU implementations.
pub fn is_software(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    ["llvmpipe", "lavapipe", "softpipe", "swiftshader"]
        .iter()
        .any(|software| lower.contains(software))
}

/// Higher is better. A software adapter always ranks below a hardware one, and is still usable
/// when nothing else exists.
pub fn rank(candidate: &Candidate<'_>) -> i64 {
    let mut score = match candidate.api {
        Api::Vulkan => 500,
        Api::Metal => 450,
        Api::Dx12 => 400,
        Api::Gl => 250,
        Api::Other => 0,
    };
    score += match candidate.kind {
        DeviceKind::Discrete => 120,
        DeviceKind::Integrated => 90,
        DeviceKind::Virtual => 40,
        DeviceKind::Cpu => -300,
        DeviceKind::Other => 0,
    };
    if is_software(candidate.name) {
        score -= 800;
    }
    score += if candidate.storage { 90 } else { -200 };
    score += if candidate.sampled { 20 } else { -50 };
    score += if candidate.copy { 20 } else { -120 };
    score
}

/// The adapters that pass `options`, best first.
pub(crate) fn choose(mut adapters: Vec<wgpu::Adapter>, options: &Options) -> Vec<wgpu::Adapter> {
    if let Some(vendor) = options.vendor {
        adapters.retain(|adapter| adapter.get_info().vendor == vendor);
    }
    if let Some(name) = &options.adapter {
        let wanted = name.to_ascii_lowercase();
        adapters.retain(|adapter| {
            adapter
                .get_info()
                .name
                .to_ascii_lowercase()
                .contains(&wanted)
        });
    }
    adapters.sort_by_cached_key(|adapter| {
        let info = adapter.get_info();
        let shown = AdapterInfo::from_wgpu(&info);
        let usages = adapter
            .get_texture_format_features(wgpu::TextureFormat::Rgba8Unorm)
            .allowed_usages;
        std::cmp::Reverse(rank(&Candidate {
            api: shown.api,
            kind: shown.kind,
            name: &info.name,
            storage: usages.contains(wgpu::TextureUsages::STORAGE_BINDING),
            sampled: usages.contains(wgpu::TextureUsages::TEXTURE_BINDING),
            copy: usages.contains(wgpu::TextureUsages::COPY_SRC | wgpu::TextureUsages::COPY_DST),
        }))
    });
    match options.index {
        Some(index) => adapters.into_iter().skip(index).take(1).collect(),
        None => adapters,
    }
}

#[cfg(test)]
mod tests {
    use super::{Api, Apis, Candidate, DeviceKind, rank};

    #[test]
    fn a_software_adapter_ranks_below_any_hardware_one() {
        let apis = [Api::Vulkan, Api::Metal, Api::Dx12, Api::Gl, Api::Other];
        let kinds = [
            DeviceKind::Discrete,
            DeviceKind::Integrated,
            DeviceKind::Virtual,
            DeviceKind::Other,
        ];
        let flags = [false, true];
        let mut worst_hardware = i64::MAX;
        let mut best_software = i64::MIN;
        for api in apis {
            for storage in flags {
                for sampled in flags {
                    for copy in flags {
                        let usable = |kind, name| {
                            rank(&Candidate {
                                api,
                                kind,
                                name,
                                storage,
                                sampled,
                                copy,
                            })
                        };
                        for kind in kinds {
                            worst_hardware = worst_hardware.min(usable(kind, "Some GPU"));
                        }
                        for name in ["llvmpipe (LLVM 19)", "lavapipe", "softpipe", "SwiftShader"] {
                            best_software = best_software.max(usable(DeviceKind::Cpu, name));
                        }
                    }
                }
            }
        }
        assert!(
            best_software < worst_hardware,
            "{best_software} {worst_hardware}"
        );
    }

    #[test]
    fn api_names_parse_as_a_host_passes_them() {
        assert_eq!(Apis::from_names("vulkan"), Some(Apis::VULKAN));
        assert_eq!(
            Apis::from_names(" GL , dx12 "),
            Some(Apis::GL.union(Apis::DX12))
        );
        assert_eq!(Apis::from_names("webgpu"), None);
        assert!(Apis::default().contains(Apis::METAL));
    }
}

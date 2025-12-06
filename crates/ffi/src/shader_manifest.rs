use base64::Engine as _;
use daedalus_gpu::shader::{
    Access, BindingData, BindingKind, BufferInit, ShaderBinding, ShaderContext, ShaderInstance,
    ShaderSpec,
};
use daedalus_runtime::{NodeError, io::NodeIo, plugins::PluginRegistry};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::bridge::any_to_json;
use crate::manifest::{
    ManifestShaderAccess, ManifestShaderBindingKind, ManifestShaderStateBackend, NodeManifest,
};
use crate::python::ImagePayload;

fn load_wgsl_src(
    manifest_dir: Option<&std::path::Path>,
    src_path: Option<&str>,
    src: Option<&str>,
) -> Result<String, String> {
    if let Some(p) = src_path {
        let base = manifest_dir
            .ok_or_else(|| "shader.src_path requires a manifest directory".to_string())?;
        let abs = base.join(p);
        std::fs::read_to_string(&abs)
            .map_err(|e| format!("failed to read shader src_path {}: {e}", abs.display()))
    } else if let Some(s) = src {
        Ok(s.to_string())
    } else {
        Err("shader requires either `src` or `src_path`".into())
    }
}

fn leak_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

pub(crate) fn install_shader_node(
    node: &NodeManifest,
    manifest_dir: Option<&std::path::Path>,
    registry: &mut PluginRegistry,
) -> Result<(), String> {
    let shader = node
        .shader
        .as_ref()
        .ok_or_else(|| "missing shader spec".to_string())?;

    if node.stateful || node.state.is_some() {
        // Supported as CPU-side persistent bytes, keyed by binding.from_state/to_state.
    }

    let mut instances_vec: Vec<ShaderInstance> = Vec::new();
    if !shader.shaders.is_empty() {
        for s in &shader.shaders {
            if s.name.trim().is_empty() {
                return Err("shader.shaders entries require a non-empty name".into());
            }
        }
        for s in &shader.shaders {
            let src = load_wgsl_src(manifest_dir, s.src_path.as_deref(), s.src.as_deref())?;
            let leaked_name = leak_str(s.name.clone());
            let leaked_src = leak_str(src);
            let leaked_entry = leak_str(s.entry.clone());
            let spec: &'static ShaderSpec = Box::leak(Box::new(ShaderSpec {
                name: leaked_name,
                src: leaked_src,
                entry: leaked_entry,
                workgroup_size: s.workgroup_size,
                bindings: &[],
            }));
            instances_vec.push(ShaderInstance {
                name: leaked_name,
                spec,
            });
        }
    } else {
        let src = load_wgsl_src(
            manifest_dir,
            shader.src_path.as_deref(),
            shader.src.as_deref(),
        )?;
        let name = shader.name.clone().unwrap_or_else(|| "default".to_string());
        let leaked_name = leak_str(name);
        let leaked_src = leak_str(src);
        let leaked_entry = leak_str(shader.entry.clone());
        let spec: &'static ShaderSpec = Box::leak(Box::new(ShaderSpec {
            name: leaked_name,
            src: leaked_src,
            entry: leaked_entry,
            workgroup_size: shader.workgroup_size,
            bindings: &[],
        }));
        instances_vec.push(ShaderInstance {
            name: leaked_name,
            spec,
        });
    }

    let instances: &'static [ShaderInstance] = Box::leak(instances_vec.into_boxed_slice());

    let bindings_spec = shader.bindings.clone();
    let has_explicit_bindings = !bindings_spec.is_empty();
    let legacy_input_binding = shader.input_binding;
    let legacy_output_binding = shader.output_binding;
    let invocations_override = shader.invocations;
    let dispatch_default = shader.dispatch.clone();
    let dispatch_from_port = shader.dispatch_from_port.clone();
    let legacy_inputs_len = node.inputs.len();
    let legacy_outputs_len = node.outputs.len();
    let legacy_input_port = node
        .inputs
        .first()
        .map(|p| p.name.clone())
        .unwrap_or_default();
    let legacy_output_port = node
        .outputs
        .first()
        .map(|p| p.name.clone())
        .unwrap_or_default();
    type StateBytes = Arc<Mutex<HashMap<String, Vec<u8>>>>;
    let state_bytes: Option<StateBytes> = if node.stateful || node.state.is_some() {
        Some(Arc::new(Mutex::new(HashMap::new())))
    } else {
        None
    };

    if has_explicit_bindings {
        let _uses_gpu_state = bindings_spec.iter().any(|b| {
            matches!(b.state_backend, Some(ManifestShaderStateBackend::Gpu))
                && (b.from_state.is_some() || b.to_state.is_some())
        });
        #[cfg(not(feature = "gpu-wgpu"))]
        if _uses_gpu_state {
            return Err(
                "shader bindings with state_backend=gpu require --features gpu-wgpu".into(),
            );
        }

        for b in &bindings_spec {
            let has_from = b.from_port.is_some() || b.from_state.is_some();
            match b.kind {
                ManifestShaderBindingKind::Texture2dRgba8 => {
                    if b.from_port.is_none() {
                        return Err("texture2d_rgba8 binding requires from_port".into());
                    }
                }
                ManifestShaderBindingKind::StorageTexture2dRgba8 => {}
                ManifestShaderBindingKind::UniformBuffer => {
                    if !matches!(b.access, ManifestShaderAccess::ReadOnly) {
                        return Err("uniform_buffer binding requires access=read_only".into());
                    }
                    if !has_from {
                        return Err(
                            "uniform_buffer binding requires from_port or from_state".into()
                        );
                    }
                }
                ManifestShaderBindingKind::StorageBuffer => {
                    if !has_from && b.size_bytes.is_none() {
                        return Err(
                            "storage_buffer binding requires from_port/from_state or size_bytes"
                                .into(),
                        );
                    }
                }
            }
            if b.readback && b.to_port.is_none() && b.to_state.is_none() {
                return Err("readback binding requires to_port or to_state".into());
            }
            if (b.from_state.is_some() || b.to_state.is_some()) && state_bytes.is_none() {
                return Err("from_state/to_state requires stateful shader node".into());
            }
            if matches!(b.state_backend, Some(ManifestShaderStateBackend::Gpu))
                && b.from_state.is_none()
                && b.to_state.is_none()
            {
                return Err("state_backend=gpu requires from_state or to_state".into());
            }
        }
    }

    #[cfg(feature = "gpu-wgpu")]
    #[derive(Clone)]
    struct GpuStateBuffer {
        buffer: Arc<wgpu::Buffer>,
        size: u64,
        device_key: usize,
    }

    #[cfg(feature = "gpu-wgpu")]
    let state_gpu_buffers: Option<Arc<Mutex<HashMap<String, GpuStateBuffer>>>> =
        if node.stateful || node.state.is_some() {
            Some(Arc::new(Mutex::new(HashMap::new())))
        } else {
            None
        };

    registry.handlers.on(&node.id, move |_node_rt, ctx_rt, io: &mut NodeIo| {
        let gpu = ctx_rt
            .gpu
            .as_ref()
            .ok_or_else(|| NodeError::Handler("GPU required for shader node".into()))?;

        let mut width: u32 = 1;
        let mut height: u32 = 1;
        let mut image_bytes: Vec<u8> = Vec::new();
        let mut owned_bytes: Vec<Box<[u8]>> = Vec::new();
        if has_explicit_bindings {
            if let Some(from) = bindings_spec.iter().find_map(|b| {
                if matches!(b.kind, ManifestShaderBindingKind::Texture2dRgba8) {
                    b.from_port.clone()
                } else {
                    None
                }
            }) {
                let (w, h, bytes) = read_rgba8_image(io, &from)?;
                width = w;
                height = h;
                image_bytes = bytes;
            } else if invocations_override.is_none() {
                return Err(NodeError::Handler(
                    "shader.invocations required when no texture2d input is provided".into(),
                ));
            }
        } else {
            // Legacy single-input/single-output mode: require exactly 1 input and 1 output.
            if legacy_inputs_len != 1 || legacy_outputs_len != 1 {
                return Err(NodeError::Handler(
                    "legacy shader node requires exactly 1 input and 1 output (or use shader.bindings)".into(),
                ));
            }
            let (w, h, bytes) = read_rgba8_image(io, &legacy_input_port)?;
            width = w;
            height = h;
            image_bytes = bytes;
        }

        let bindings: Vec<ShaderBinding<'_>> = if has_explicit_bindings {
            #[cfg(feature = "gpu-wgpu")]
            let mut wgpu_device_queue: Option<(&wgpu::Device, &wgpu::Queue, usize)> = None;

            #[cfg(feature = "gpu-wgpu")]
            let mut resolve_wgpu = || -> Result<(&wgpu::Device, &wgpu::Queue, usize), NodeError> {
                if let Some(v) = wgpu_device_queue {
                    return Ok(v);
                }
                let backend = gpu.backend_ref();
                let (device, queue) = backend.wgpu_device_queue().ok_or_else(|| {
                    NodeError::Handler("state_backend=gpu requires a wgpu backend".into())
                })?;
                let key = device as *const _ as usize;
                let v = (device, queue, key);
                wgpu_device_queue = Some(v);
                Ok(v)
            };

            enum PreparedData {
                TextureIn,
                TextureOut,
                BufferBytes(usize),
                BufferZeroed(u64),
                #[cfg(feature = "gpu-wgpu")]
                BufferDevice { buffer: Arc<wgpu::Buffer>, size: u64, device_key: usize },
            }
            struct Prepared {
                binding: u32,
                kind: BindingKind,
                access: Access,
                readback: bool,
                data: PreparedData,
            }

            let mut prepared: Vec<Prepared> = Vec::with_capacity(bindings_spec.len());
            for b in &bindings_spec {
                let access = match b.access {
                    ManifestShaderAccess::ReadOnly => Access::ReadOnly,
                    ManifestShaderAccess::WriteOnly => Access::WriteOnly,
                    ManifestShaderAccess::ReadWrite => Access::ReadWrite,
                };
                let state_backend = b.state_backend.unwrap_or(ManifestShaderStateBackend::Cpu);

                match b.kind {
                    ManifestShaderBindingKind::Texture2dRgba8 => {
                        let _ = b.from_port.as_deref().ok_or_else(|| {
                            NodeError::Handler("texture2d_rgba8 binding requires from_port".into())
                        })?;
                        prepared.push(Prepared {
                            binding: b.binding,
                            kind: BindingKind::Texture2D,
                            access,
                            readback: b.readback,
                            data: PreparedData::TextureIn,
                        });
                    }
                    ManifestShaderBindingKind::StorageTexture2dRgba8 => {
                        prepared.push(Prepared {
                            binding: b.binding,
                            kind: BindingKind::StorageTexture2D,
                            access,
                            readback: b.readback,
                            data: PreparedData::TextureOut,
                        });
                    }
                    ManifestShaderBindingKind::UniformBuffer => {
                        let data = match state_backend {
                            ManifestShaderStateBackend::Cpu => {
                                let bytes = if let Some(key) = b.from_state.as_deref() {
                                    let map = state_bytes.as_ref().ok_or_else(|| {
                                        NodeError::Handler(
                                            "from_state requires stateful shader node".into(),
                                        )
                                    })?;
                                    map.lock()
                                        .ok()
                                        .and_then(|m| m.get(key).cloned())
                                        .or_else(|| b.size_bytes.map(|n| vec![0u8; n as usize]))
                                        .ok_or_else(|| {
                                            NodeError::Handler(
                                                "from_state missing and size_bytes not set".into(),
                                            )
                                        })?
                                } else {
                                    let from = b.from_port.as_deref().ok_or_else(|| {
                                        NodeError::Handler(
                                            "uniform_buffer binding requires from_port or from_state"
                                                .into(),
                                        )
                                    })?;
                                    read_bytes(io, from)?
                                };
                                owned_bytes.push(bytes.into_boxed_slice());
                                PreparedData::BufferBytes(owned_bytes.len() - 1)
                            }
                            ManifestShaderStateBackend::Gpu => {
                                #[cfg(not(feature = "gpu-wgpu"))]
                                {
                                    return Err(NodeError::Handler(
                                        "state_backend=gpu requires --features gpu-wgpu".into(),
                                    ));
                                }
                                #[cfg(feature = "gpu-wgpu")]
                                {
                                    let key = b
                                        .from_state
                                        .as_deref()
                                        .or(b.to_state.as_deref())
                                        .ok_or_else(|| {
                                            NodeError::Handler(
                                                "state_backend=gpu requires from_state or to_state"
                                                    .into(),
                                            )
                                        })?;
                                    let map = state_gpu_buffers.as_ref().ok_or_else(|| {
                                        NodeError::Handler(
                                            "state_backend=gpu requires stateful shader node".into(),
                                        )
                                    })?;
                                    let (device, queue, device_key) = resolve_wgpu()?;
                                    let mut guard = map.lock().map_err(|_| {
                                        NodeError::Handler("gpu state lock poisoned".into())
                                    })?;

                                    let gs = match guard.get(key) {
                                        Some(existing)
                                            if existing.device_key == device_key
                                                && b.size_bytes.map(|n| existing.size >= n).unwrap_or(true) =>
                                        {
                                            existing.clone()
                                        }
                                        _ => {
                                            let init_bytes = if let Some(from) = b.from_port.as_deref() {
                                                Some(read_bytes(io, from)?)
                                            } else if let Some(st) = b.from_state.as_deref() {
                                                state_bytes
                                                    .as_ref()
                                                    .and_then(|m| m.lock().ok())
                                                    .and_then(|m| m.get(st).cloned())
                                            } else {
                                                None
                                            };
                                            let desired_size = b
                                                .size_bytes
                                                .or_else(|| init_bytes.as_ref().map(|v| v.len() as u64))
                                                .filter(|n| *n > 0)
                                                .ok_or_else(|| {
                                                    NodeError::Handler(
                                                        "state_backend=gpu uniform buffer requires size_bytes when init bytes are empty".into(),
                                                    )
                                                })?;
                                            let usage = wgpu::BufferUsages::UNIFORM
                                                | wgpu::BufferUsages::COPY_DST
                                                | wgpu::BufferUsages::COPY_SRC
                                                | wgpu::BufferUsages::MAP_READ;
                                            let buf = device.create_buffer(&wgpu::BufferDescriptor {
                                                label: Some("manifest-shader-state-uniform"),
                                                size: desired_size,
                                                usage,
                                                mapped_at_creation: false,
                                            });
                                            let gs = GpuStateBuffer {
                                                buffer: Arc::new(buf),
                                                size: desired_size,
                                                device_key,
                                            };
                                            guard.insert(key.to_string(), gs.clone());
                                            if let Some(init) = init_bytes {
                                                if init.len() > desired_size as usize {
                                                    return Err(NodeError::Handler(
                                                        "gpu state init bytes larger than size_bytes".into(),
                                                    ));
                                                }
                                                let mut full = vec![0u8; desired_size as usize];
                                                full[..init.len()].copy_from_slice(&init);
                                                queue.write_buffer(gs.buffer.as_ref(), 0, &full);
                                            } else {
                                                queue.write_buffer(
                                                    gs.buffer.as_ref(),
                                                    0,
                                                    &vec![0u8; desired_size as usize],
                                                );
                                            }
                                            gs
                                        }
                                    };

                                    PreparedData::BufferDevice {
                                        buffer: gs.buffer.clone(),
                                        size: gs.size,
                                        device_key: gs.device_key,
                                    }
                                }
                            }
                        };

                        prepared.push(Prepared {
                            binding: b.binding,
                            kind: BindingKind::Uniform,
                            access,
                            readback: b.readback,
                            data,
                        });
                    }
                    ManifestShaderBindingKind::StorageBuffer => {
                        let data = match state_backend {
                            ManifestShaderStateBackend::Cpu => {
                                if let Some(key) = b.from_state.as_deref() {
                                    let map = state_bytes.as_ref().ok_or_else(|| {
                                        NodeError::Handler(
                                            "from_state requires stateful shader node".into(),
                                        )
                                    })?;
                                    let bytes = map
                                        .lock()
                                        .ok()
                                        .and_then(|m| m.get(key).cloned())
                                        .or_else(|| b.size_bytes.map(|n| vec![0u8; n as usize]))
                                        .ok_or_else(|| {
                                            NodeError::Handler(
                                                "from_state missing and size_bytes not set".into(),
                                            )
                                        })?;
                                    owned_bytes.push(bytes.into_boxed_slice());
                                    PreparedData::BufferBytes(owned_bytes.len() - 1)
                                } else if let Some(from) = b.from_port.as_deref() {
                                    let bytes = read_bytes(io, from)?;
                                    owned_bytes.push(bytes.into_boxed_slice());
                                    PreparedData::BufferBytes(owned_bytes.len() - 1)
                                } else {
                                    let size = b.size_bytes.ok_or_else(|| {
                                        NodeError::Handler(
                                            "storage_buffer binding requires size_bytes when from_port is not set".into(),
                                        )
                                    })?;
                                    PreparedData::BufferZeroed(size)
                                }
                            }
                            ManifestShaderStateBackend::Gpu => {
                                #[cfg(not(feature = "gpu-wgpu"))]
                                {
                                    return Err(NodeError::Handler(
                                        "state_backend=gpu requires --features gpu-wgpu".into(),
                                    ));
                                }
                                #[cfg(feature = "gpu-wgpu")]
                                {
                                    let key = b
                                        .from_state
                                        .as_deref()
                                        .or(b.to_state.as_deref())
                                        .ok_or_else(|| {
                                            NodeError::Handler(
                                                "state_backend=gpu requires from_state or to_state"
                                                    .into(),
                                            )
                                        })?;
                                    let map = state_gpu_buffers.as_ref().ok_or_else(|| {
                                        NodeError::Handler(
                                            "state_backend=gpu requires stateful shader node".into(),
                                        )
                                    })?;
                                    let (device, queue, device_key) = resolve_wgpu()?;
                                    let mut guard = map.lock().map_err(|_| {
                                        NodeError::Handler("gpu state lock poisoned".into())
                                    })?;

                                    let gs = match guard.get(key) {
                                        Some(existing)
                                            if existing.device_key == device_key
                                                && b.size_bytes.map(|n| existing.size >= n).unwrap_or(true) =>
                                        {
                                            existing.clone()
                                        }
                                        _ => {
                                            let init_bytes = if let Some(from) = b.from_port.as_deref() {
                                                Some(read_bytes(io, from)?)
                                            } else if let Some(st) = b.from_state.as_deref() {
                                                state_bytes
                                                    .as_ref()
                                                    .and_then(|m| m.lock().ok())
                                                    .and_then(|m| m.get(st).cloned())
                                            } else {
                                                None
                                            };
                                            let desired_size = b
                                                .size_bytes
                                                .or_else(|| init_bytes.as_ref().map(|v| v.len() as u64))
                                                .filter(|n| *n > 0)
                                                .ok_or_else(|| {
                                                    NodeError::Handler(
                                                        "state_backend=gpu storage buffer requires size_bytes when init bytes are empty".into(),
                                                    )
                                                })?;
                                            let usage = wgpu::BufferUsages::STORAGE
                                                | wgpu::BufferUsages::COPY_DST
                                                | wgpu::BufferUsages::COPY_SRC
                                                | wgpu::BufferUsages::MAP_READ;
                                            let buf = device.create_buffer(&wgpu::BufferDescriptor {
                                                label: Some("manifest-shader-state-storage"),
                                                size: desired_size,
                                                usage,
                                                mapped_at_creation: false,
                                            });
                                            let gs = GpuStateBuffer {
                                                buffer: Arc::new(buf),
                                                size: desired_size,
                                                device_key,
                                            };
                                            guard.insert(key.to_string(), gs.clone());
                                            if let Some(init) = init_bytes {
                                                if init.len() > desired_size as usize {
                                                    return Err(NodeError::Handler(
                                                        "gpu state init bytes larger than size_bytes".into(),
                                                    ));
                                                }
                                                let mut full = vec![0u8; desired_size as usize];
                                                full[..init.len()].copy_from_slice(&init);
                                                queue.write_buffer(gs.buffer.as_ref(), 0, &full);
                                            } else {
                                                queue.write_buffer(
                                                    gs.buffer.as_ref(),
                                                    0,
                                                    &vec![0u8; desired_size as usize],
                                                );
                                            }
                                            gs
                                        }
                                    };

                                    PreparedData::BufferDevice {
                                        buffer: gs.buffer.clone(),
                                        size: gs.size,
                                        device_key: gs.device_key,
                                    }
                                }
                            }
                        };

                        prepared.push(Prepared {
                            binding: b.binding,
                            kind: BindingKind::Storage,
                            access,
                            readback: b.readback,
                            data,
                        });
                    }
                }
            }

            let mut out_bindings: Vec<ShaderBinding<'_>> = Vec::with_capacity(prepared.len());
            for p in prepared {
                let data = match p.data {
                    PreparedData::TextureIn => BindingData::TextureRgba8 {
                        width,
                        height,
                        bytes: std::borrow::Cow::Borrowed(&image_bytes),
                    },
                    PreparedData::TextureOut => BindingData::TextureAlloc { width, height },
                    PreparedData::BufferBytes(idx) => {
                        let slice: &[u8] = owned_bytes[idx].as_ref();
                        BindingData::Buffer(BufferInit::Bytes(slice))
                    }
                    PreparedData::BufferZeroed(size) => BindingData::Buffer(BufferInit::Zeroed(size)),
                    #[cfg(feature = "gpu-wgpu")]
                    PreparedData::BufferDevice { buffer, size, device_key } => {
                        BindingData::BufferDevice {
                            buffer,
                            size,
                            device_key,
                        }
                    }
                };
                out_bindings.push(ShaderBinding {
                    binding: p.binding,
                    kind: p.kind,
                    access: p.access,
                    data,
                    readback: p.readback,
                });
            }

            out_bindings
        } else {
            vec![
                ShaderBinding {
                    binding: legacy_input_binding,
                    kind: BindingKind::Texture2D,
                    access: Access::ReadOnly,
                    data: BindingData::TextureRgba8 {
                        width,
                        height,
                        bytes: std::borrow::Cow::Borrowed(&image_bytes),
                    },
                    readback: false,
                },
                ShaderBinding {
                    binding: legacy_output_binding,
                    kind: BindingKind::StorageTexture2D,
                    access: Access::WriteOnly,
                    data: BindingData::TextureAlloc { width, height },
                    readback: true,
                },
            ]
        };

        let shader_ctx = ShaderContext {
            shaders: instances,
            gpu: Some(gpu.clone()),
        };
        let invocations = invocations_override.unwrap_or([width, height, 1]);
        let dispatch_name = if let Some(port) = dispatch_from_port.as_deref() {
            Some(read_string(io, port)?)
        } else {
            dispatch_default.as_deref().map(|s| s.to_string())
        };

        let out = if let Some(name) = dispatch_name.as_deref() {
            shader_ctx
                .dispatch_by_name(name, &bindings, None, None, Some(invocations))
                .map_err(|e| NodeError::Handler(e.to_string()))?
        } else {
            shader_ctx
                .dispatch_first(&bindings, None, None, Some(invocations))
                .map_err(|e| NodeError::Handler(e.to_string()))?
        };

        if has_explicit_bindings {
            for b in &bindings_spec {
                if !b.readback {
                    continue;
                }
                match b.kind {
                    ManifestShaderBindingKind::StorageTexture2dRgba8 => {
                        let out_img = out
                            .storage_rgba8_image(b.binding, width, height)
                            .ok_or_else(|| {
                                NodeError::Handler("missing shader output texture readback".into())
                            })?;
                        let json = any_to_json(&out_img).ok_or_else(|| {
                            NodeError::Handler("failed to convert shader output image".into())
                        })?;
                        if let Some(to_port) = b.to_port.as_deref() {
                            io.push_any(Some(to_port), json);
                        }
                    }
                    ManifestShaderBindingKind::StorageBuffer
                    | ManifestShaderBindingKind::UniformBuffer => {
                        let bytes = out
                            .buffers
                            .get(&b.binding)
                            .cloned()
                            .ok_or_else(|| {
                                NodeError::Handler(format!(
                                    "missing shader output buffer readback binding {}",
                                    b.binding
                                ))
                            })?;
                        if let Some(to_port) = b.to_port.as_deref() {
                            io.push_any(Some(to_port), bytes.clone());
                        }
                        if let Some(key) = b.to_state.as_deref() {
                            let map = state_bytes
                                .as_ref()
                                .ok_or_else(|| NodeError::Handler("to_state requires stateful shader node".into()))?;
                            if let Ok(mut m) = map.lock() {
                                m.insert(key.to_string(), bytes);
                            }
                        }
                    }
                    ManifestShaderBindingKind::Texture2dRgba8 => {}
                }
            }
        } else {
            let out_img = out
                .storage_rgba8_image(legacy_output_binding, width, height)
                .ok_or_else(|| {
                    NodeError::Handler("missing shader output texture readback".into())
                })?;
            let json = any_to_json(&out_img)
                .ok_or_else(|| NodeError::Handler("failed to convert shader output image".into()))?;
            io.push_any(Some(&legacy_output_port), json);
        }
        Ok(())
    });

    Ok(())
}

fn read_rgba8_image(io: &mut NodeIo, port: &str) -> Result<(u32, u32, Vec<u8>), NodeError> {
    if let Some(img) = io.get_any::<image::DynamicImage>(port) {
        let rgba = img.to_rgba8();
        return Ok((rgba.width(), rgba.height(), rgba.into_raw()));
    }

    if let Some(payload) = io.get_any::<ImagePayload>(port) {
        return decode_payload_rgba(&payload);
    }

    if let Some(v) = io.get_any::<serde_json::Value>(port) {
        let payload: ImagePayload = serde_json::from_value(v)
            .map_err(|e| NodeError::Handler(format!("invalid image payload: {e}")))?;
        return decode_payload_rgba(&payload);
    }

    Err(NodeError::InvalidInput(format!(
        "missing image input '{port}'"
    )))
}

fn decode_payload_rgba(payload: &ImagePayload) -> Result<(u32, u32, Vec<u8>), NodeError> {
    // Prefer raw RGBA8 bytes (fast path) when available.
    if payload.layout == "HWC" && payload.dtype == "u8" && payload.channels == 4 {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(payload.data_b64.as_bytes())
            .map_err(|e| NodeError::Handler(format!("invalid image base64: {e}")))?;
        let expected = payload
            .width
            .checked_mul(payload.height)
            .and_then(|px| px.checked_mul(4))
            .unwrap_or(0) as usize;
        if bytes.len() == expected {
            return Ok((payload.width as u32, payload.height as u32, bytes));
        }
    }

    // Legacy fallback: `data_b64` is a PNG.
    let png = base64::engine::general_purpose::STANDARD
        .decode(payload.data_b64.as_bytes())
        .map_err(|e| NodeError::Handler(format!("invalid image base64: {e}")))?;
    let img = image::load_from_memory(&png)
        .map_err(|e| NodeError::Handler(format!("invalid image bytes: {e}")))?;
    let rgba = img.to_rgba8();
    Ok((rgba.width(), rgba.height(), rgba.into_raw()))
}

fn read_bytes(io: &mut NodeIo, port: &str) -> Result<Vec<u8>, NodeError> {
    if let Some(bytes) = io.get_any::<Vec<u8>>(port) {
        return Ok(bytes.clone());
    }
    if let Some(v) = io.get_any::<serde_json::Value>(port) {
        if let Some(arr) = v.as_array() {
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                let b = item
                    .as_u64()
                    .ok_or_else(|| NodeError::InvalidInput(format!("invalid byte in {port}")))?;
                out.push((b & 0xFF) as u8);
            }
            return Ok(out);
        }
        if let Some(s) = v.as_str() {
            return Ok(s.as_bytes().to_vec());
        }
    }
    Err(NodeError::InvalidInput(format!(
        "missing bytes input '{port}'"
    )))
}

fn read_string(io: &mut NodeIo, port: &str) -> Result<String, NodeError> {
    if let Some(s) = io.get_any::<String>(port) {
        return Ok(s.clone());
    }
    if let Some(v) = io.get_any::<serde_json::Value>(port)
        && let Some(s) = v.as_str()
    {
        return Ok(s.to_string());
    }
    Err(NodeError::InvalidInput(format!(
        "missing string input '{port}'"
    )))
}

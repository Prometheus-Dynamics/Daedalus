//! Reads a WGSL module with naga: the entry point, its workgroup size, and every binding. Only
//! real declarations count, so text in a comment is nothing.

use naga::{AddressSpace, ImageClass, ImageDimension, ScalarKind, StorageAccess, TypeInner};

/// The limits every WebGPU device meets. A shader within them runs anywhere, and a device's own
/// limits are checked when the shader loads.
pub(crate) const MAX_STORAGE_BUFFERS: usize = 8;
pub(crate) const MAX_STORAGE_TEXTURES: usize = 4;
pub(crate) const MAX_WORKGROUP_STORAGE: u32 = 16_384;
pub(crate) const MAX_INVOCATIONS: u32 = 256;
pub(crate) const MAX_WORKGROUP_SIZE: [u32; 3] = [256, 256, 64];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Storage {
        read_only: bool,
        stride: u32,
    },
    Uniform {
        size: u32,
    },
    Texture {
        sample: &'static str,
    },
    StorageTexture {
        format: &'static str,
        access: &'static str,
    },
    Sampler {
        comparison: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Binding {
    pub(crate) index: u32,
    pub(crate) name: String,
    pub(crate) kind: Kind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Shader {
    pub(crate) entry: String,
    pub(crate) workgroup: [u32; 3],
    /// Bytes of `var<workgroup>` memory.
    pub(crate) workgroup_storage: u32,
    /// In binding order.
    pub(crate) bindings: Vec<Binding>,
}

fn storage_format(format: naga::StorageFormat) -> Option<&'static str> {
    Some(match format {
        naga::StorageFormat::Rgba8Unorm => "Rgba8",
        naga::StorageFormat::R32Float => "R32F",
        naga::StorageFormat::Rg32Float => "Rg32F",
        naga::StorageFormat::Rgba16Uint => "Rgba16Uint",
        naga::StorageFormat::Rg32Uint => "Rg32Uint",
        _ => return None,
    })
}

/// Reads `source`. `entry` picks a compute entry point by name: without it the module must have
/// exactly one.
pub(crate) fn read(source: &str, entry: Option<&str>) -> Result<Shader, String> {
    let module = naga::front::wgsl::parse_str(source).map_err(|error| {
        let text = error.emit_to_string(source);
        text.trim_end().trim_start_matches("error: ").to_string()
    })?;
    let compute: Vec<&naga::EntryPoint> = module
        .entry_points
        .iter()
        .filter(|point| point.stage == naga::ShaderStage::Compute)
        .collect();
    let point = match entry {
        Some(name) => compute
            .iter()
            .find(|point| point.name == name)
            .ok_or_else(|| format!("the shader has no compute entry point named `{name}`"))?,
        None => match compute.as_slice() {
            [only] => only,
            [] => {
                return Err(
                    "the shader has no compute entry point: a whole-shader node is one \
                     `@compute @workgroup_size(..) fn`"
                        .into(),
                );
            }
            several => {
                let names: Vec<&str> = several.iter().map(|point| &*point.name).collect();
                return Err(format!(
                    "the shader has {} compute entry points ({}): say which with `entry = \"..\"`",
                    several.len(),
                    names.join(", ")
                ));
            }
        },
    };
    if point.workgroup_size.contains(&0) {
        return Err(format!(
            "`{}` has no @workgroup_size the macro can read: it must be numbers or consts, not overrides",
            point.name
        ));
    }

    let mut layouter = naga::proc::Layouter::default();
    layouter
        .update(module.to_ctx())
        .map_err(|error| format!("the shader's types can't be laid out: {error}"))?;
    let size_of = |ty| layouter[ty].size;
    let stride_of = |ty: naga::Handle<naga::Type>| match &module.types[ty].inner {
        TypeInner::Array { stride, .. } => *stride,
        TypeInner::Struct { members, .. } => {
            match members.last().map(|last| &module.types[last.ty].inner) {
                Some(TypeInner::Array { stride, .. }) => *stride,
                _ => 0,
            }
        }
        _ => 0,
    };

    let mut bindings = Vec::new();
    let mut workgroup_storage = 0u32;
    for (_, global) in module.global_variables.iter() {
        if global.space == AddressSpace::WorkGroup {
            workgroup_storage = workgroup_storage.saturating_add(size_of(global.ty));
        }
        let Some(at) = &global.binding else { continue };
        let name = global.name.clone().unwrap_or_default();
        if at.group != 0 {
            return Err(format!(
                "`{name}` is in @group({}): a shader node's bindings are all in @group(0)",
                at.group
            ));
        }
        let kind = match (&global.space, &module.types[global.ty].inner) {
            (AddressSpace::Storage { access }, _) => Kind::Storage {
                read_only: !access.contains(StorageAccess::STORE),
                stride: stride_of(global.ty),
            },
            (AddressSpace::Uniform, _) => Kind::Uniform {
                size: size_of(global.ty),
            },
            (_, TypeInner::Sampler { comparison }) => Kind::Sampler {
                comparison: *comparison,
            },
            (
                _,
                TypeInner::Image {
                    dim,
                    arrayed,
                    class,
                },
            ) => {
                if *dim != ImageDimension::D2 || *arrayed {
                    return Err(format!(
                        "`{name}` isn't a plain 2D texture, the one kind supported"
                    ));
                }
                match class {
                    ImageClass::Sampled { kind, multi: false } => Kind::Texture {
                        sample: match kind {
                            ScalarKind::Uint => "Uint",
                            ScalarKind::Sint => "Sint",
                            _ => "Float",
                        },
                    },
                    ImageClass::Depth { multi: false } => Kind::Texture { sample: "Depth" },
                    ImageClass::Storage { format, access } => Kind::StorageTexture {
                        format: storage_format(*format).ok_or_else(|| {
                            format!(
                                "`{name}` is a storage texture of {format:?}: the formats are \
                                 rgba8unorm, r32float, rg32float, rgba16uint and rg32uint"
                            )
                        })?,
                        access: match (
                            access.contains(StorageAccess::LOAD),
                            access.contains(StorageAccess::STORE),
                        ) {
                            (true, true) => "ReadWrite",
                            (_, true) => "Write",
                            _ => "Read",
                        },
                    },
                    _ => return Err(format!("`{name}` is a multisampled or external texture")),
                }
            }
            _ => {
                return Err(format!(
                    "`{name}` is a kind of binding a shader node can't take"
                ));
            }
        };
        bindings.push(Binding {
            index: at.binding,
            name,
            kind,
        });
    }
    bindings.sort_by_key(|binding| binding.index);

    let shader = Shader {
        entry: point.name.clone(),
        workgroup: point.workgroup_size,
        workgroup_storage,
        bindings,
    };
    check_limits(&shader)?;
    Ok(shader)
}

/// The limits every device meets. An error names the count.
fn check_limits(shader: &Shader) -> Result<(), String> {
    let count = |want: fn(&Kind) -> bool| shader.bindings.iter().filter(|b| want(&b.kind)).count();
    let buffers = count(|kind| matches!(kind, Kind::Storage { .. }));
    if buffers > MAX_STORAGE_BUFFERS {
        return Err(format!(
            "the shader binds {buffers} storage buffers, and a device only has to allow {MAX_STORAGE_BUFFERS}"
        ));
    }
    let textures = count(|kind| matches!(kind, Kind::StorageTexture { .. }));
    if textures > MAX_STORAGE_TEXTURES {
        return Err(format!(
            "the shader binds {textures} storage textures, and a device only has to allow {MAX_STORAGE_TEXTURES}"
        ));
    }
    let [x, y, z] = shader.workgroup;
    let invocations = u64::from(x) * u64::from(y) * u64::from(z);
    let over = shader
        .workgroup
        .iter()
        .zip(MAX_WORKGROUP_SIZE)
        .any(|(&size, max)| size > max);
    if over || invocations > u64::from(MAX_INVOCATIONS) {
        return Err(format!(
            "@workgroup_size({x}, {y}, {z}) is {invocations} invocations: a device only has to allow \
             {MAX_INVOCATIONS}, at most 256 x 256 x 64"
        ));
    }
    if shader.workgroup_storage > MAX_WORKGROUP_STORAGE {
        return Err(format!(
            "the shader uses {} bytes of workgroup memory, and a device only has to allow {MAX_WORKGROUP_STORAGE}",
            shader.workgroup_storage
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Kind, read};

    const COPY: &str = "
        // A stray @group(0) @binding(3) var<storage> ghost: array<u32>; in a comment.
        const WIDTH: u32 = 32;
        struct Config { gain: f32, bias: f32 }
        @group(0) @binding(0) var<storage, read> src: array<f32>;
        @group(0) @binding(1) var<storage, read_write> dst: array<f32>;
        @group(0) @binding(2) var<uniform> config: Config;
        @compute @workgroup_size(WIDTH * 2)
        fn main(@builtin(global_invocation_id) id: vec3<u32>) {
            if id.x < arrayLength(&dst) { dst[id.x] = src[id.x] * config.gain + config.bias; }
        }
    ";

    #[test]
    fn a_shader_is_read_from_its_declarations() {
        let shader = read(COPY, None).expect("a valid shader");
        assert_eq!(shader.entry, "main");
        assert_eq!(
            shader.workgroup,
            [64, 1, 1],
            "a size built from a const is read"
        );
        let kinds: Vec<(u32, &str, Kind)> = shader
            .bindings
            .iter()
            .map(|binding| (binding.index, &*binding.name, binding.kind))
            .collect();
        assert_eq!(
            kinds,
            [
                (
                    0,
                    "src",
                    Kind::Storage {
                        read_only: true,
                        stride: 4
                    }
                ),
                (
                    1,
                    "dst",
                    Kind::Storage {
                        read_only: false,
                        stride: 4
                    }
                ),
                (2, "config", Kind::Uniform { size: 8 }),
            ],
            "the binding in the comment isn't one"
        );
    }

    #[test]
    fn textures_and_samplers_classify() {
        let shader = read(
            "@group(0) @binding(0) var input: texture_2d<f32>;
             @group(0) @binding(1) var linear: sampler;
             @group(0) @binding(2) var output: texture_storage_2d<rgba8unorm, write>;
             @group(0) @binding(3) var ids: texture_2d<u32>;
             var<workgroup> scratch: array<u32, 16>;
             @compute @workgroup_size(8, 8)
             fn main(@builtin(global_invocation_id) id: vec3<u32>) {
                 let size = vec2<f32>(textureDimensions(output));
                 let color = textureSampleLevel(input, linear, vec2<f32>(id.xy) / size, 0.0);
                 scratch[0] = textureLoad(ids, vec2<i32>(id.xy), 0).x;
                 textureStore(output, vec2<i32>(id.xy), color);
             }",
            None,
        )
        .expect("a valid shader");
        let kinds: Vec<Kind> = shader.bindings.iter().map(|binding| binding.kind).collect();
        assert_eq!(
            kinds,
            [
                Kind::Texture { sample: "Float" },
                Kind::Sampler { comparison: false },
                Kind::StorageTexture {
                    format: "Rgba8",
                    access: "Write"
                },
                Kind::Texture { sample: "Uint" },
            ]
        );
        assert_eq!(
            (shader.workgroup, shader.workgroup_storage),
            ([8, 8, 1], 64)
        );
    }

    #[test]
    fn what_cannot_run_is_refused_with_its_count() {
        let many: String = (0..9)
            .map(|at| {
                format!("@group(0) @binding({at}) var<storage, read_write> b{at}: array<u32>;\n")
            })
            .collect();
        let error = read(
            &format!("{many} @compute @workgroup_size(1) fn main() {{}}"),
            None,
        );
        assert!(error.unwrap_err().contains("binds 9 storage buffers"));

        let error = read("@compute @workgroup_size(32, 32) fn main() {}", None).unwrap_err();
        assert!(error.contains("1024 invocations"), "{error}");
        let error = read("var<workgroup> big: array<u32, 5000>; @compute @workgroup_size(1) fn main() { big[0] = 1u; }", None).unwrap_err();
        assert!(error.contains("20000 bytes of workgroup memory"), "{error}");
        let error = read("@compute fn main() {}", None).unwrap_err();
        assert!(error.contains("workgroup"), "{error}");
        let error = read(
            "@fragment fn main() -> @location(0) vec4f { return vec4f(); }",
            None,
        );
        assert!(error.unwrap_err().contains("no compute entry point"));
        let two = "@compute @workgroup_size(1) fn a() {} @compute @workgroup_size(1) fn b() {}";
        assert!(
            read(two, None)
                .unwrap_err()
                .contains("2 compute entry points (a, b)")
        );
        assert_eq!(read(two, Some("b")).expect("named").entry, "b");
        assert!(read(two, Some("c")).unwrap_err().contains("named `c`"));
        let group = "@group(1) @binding(0) var<storage> x: array<u32>; @compute @workgroup_size(1) fn main() {}";
        assert!(read(group, None).unwrap_err().contains("@group(1)"));
    }
}

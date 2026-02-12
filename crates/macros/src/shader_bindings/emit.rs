use quote::quote;

use super::map::{
    map_address_mode, map_mipmap_filter, map_sample_type, map_sampler_kind, map_texture_format,
    map_view_dimension,
};
use super::types::{FieldBinding, FieldKind, InferredAccess, Spec};

pub struct Emitted {
    pub binding_specs: Vec<proc_macro2::TokenStream>,
    pub binding_inits: Vec<proc_macro2::TokenStream>,
}

pub fn emit_bindings(
    fields: &[FieldBinding],
    auto_sampler_binding: Option<u32>,
    inferred_map: &std::collections::HashMap<u32, InferredAccess>,
) -> Emitted {
    let mut binding_specs: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|f| {
            let binding = f.binding;
            let kind = match &f.kind {
                FieldKind::StorageRead
                | FieldKind::StorageReadWrite { .. }
                | FieldKind::StorageWrite { .. }
                | FieldKind::State => {
                    quote! { ::daedalus::gpu::shader::BindingKind::Storage }
                }
                FieldKind::Uniform => quote! { ::daedalus::gpu::shader::BindingKind::Uniform },
                FieldKind::Texture { write, .. } => {
                    if *write {
                        quote! { ::daedalus::gpu::shader::BindingKind::StorageTexture2D }
                    } else {
                        quote! { ::daedalus::gpu::shader::BindingKind::Texture2D }
                    }
                }
                FieldKind::Sampler { .. } => {
                    quote! { ::daedalus::gpu::shader::BindingKind::Sampler }
                }
            };
            let access = match &f.kind {
                FieldKind::StorageRead => quote! { ::daedalus::gpu::shader::Access::ReadOnly },
                FieldKind::StorageReadWrite { .. } => {
                    quote! { ::daedalus::gpu::shader::Access::ReadWrite }
                }
                FieldKind::StorageWrite { .. } => {
                    quote! { ::daedalus::gpu::shader::Access::WriteOnly }
                }
                FieldKind::Uniform => quote! { ::daedalus::gpu::shader::Access::ReadOnly },
                FieldKind::State => match inferred_map.get(&binding) {
                    Some(InferredAccess::StorageRead) => {
                        quote! { ::daedalus::gpu::shader::Access::ReadOnly }
                    }
                    _ => quote! { ::daedalus::gpu::shader::Access::ReadWrite },
                },
                FieldKind::Texture { write, .. } => {
                    if *write {
                        quote! { ::daedalus::gpu::shader::Access::WriteOnly }
                    } else {
                        quote! { ::daedalus::gpu::shader::Access::ReadOnly }
                    }
                }
                FieldKind::Sampler { .. } => quote! { ::daedalus::gpu::shader::Access::ReadOnly },
            };
            let (tex_format, sample_type, view_dim, sampler_kind) = match &f.kind {
                FieldKind::Texture {
                    format_override,
                    sample_type_override,
                    view_override,
                    write,
                    ..
                } => {
                    let fmt = format_override
                        .as_ref()
                        .and_then(|s| map_texture_format(s))
                        .unwrap_or(quote! { Some(::wgpu::TextureFormat::Rgba8Unorm) });
                    let sample = if *write {
                        quote! { None }
                    } else {
                        sample_type_override
                            .as_ref()
                            .and_then(|s| map_sample_type(s))
                            .unwrap_or(quote! { None })
                    };
                    let view = view_override
                        .as_ref()
                        .and_then(|s| map_view_dimension(s))
                        .unwrap_or(quote! { Some(::wgpu::TextureViewDimension::D2) });
                    (fmt, sample, view, quote! { None })
                }
                FieldKind::Sampler { kind_override, .. } => {
                    let kind =
                        if let Some(k) = kind_override.as_ref().and_then(|s| map_sampler_kind(s)) {
                            quote! { Some(#k) }
                        } else {
                            quote! { Some(::daedalus::gpu::shader::SamplerKind::Filtering) }
                        };
                    (
                        quote! { None },
                        quote! { None },
                        quote! { None },
                        quote! { Some(#kind) },
                    )
                }
                _ => (
                    quote! { None },
                    quote! { None },
                    quote! { None },
                    quote! { None },
                ),
            };
            let invocation_stride = match &f.kind {
                FieldKind::StorageRead
                | FieldKind::StorageReadWrite { .. }
                | FieldKind::StorageWrite { .. }
                | FieldKind::State => quote! { None },
                _ => quote! { None },
            };
            quote! {
                ::daedalus::gpu::shader::BindingSpec {
                    binding: #binding,
                    kind: #kind,
                    access: #access,
                    invocation_stride: #invocation_stride,
                    texture_format: #tex_format,
                    sample_type: #sample_type,
                    view_dimension: #view_dim,
                    sampler_kind: #sampler_kind,
                }
            }
        })
        .collect();

    if let Some(b) = auto_sampler_binding {
        let sampler_kind = if let Some(InferredAccess::Sampler(Some(k))) = inferred_map.get(&b) {
            map_sampler_kind(k)
                .unwrap_or(quote! { ::daedalus::gpu::shader::SamplerKind::Filtering })
        } else {
            quote! { ::daedalus::gpu::shader::SamplerKind::Filtering }
        };
        binding_specs.push(quote! {
            ::daedalus::gpu::shader::BindingSpec {
                binding: #b,
                kind: ::daedalus::gpu::shader::BindingKind::Sampler,
                access: ::daedalus::gpu::shader::Access::ReadOnly,
                invocation_stride: None,
                texture_format: None,
                sample_type: None,
                view_dimension: None,
                sampler_kind: Some(#sampler_kind),
            }
        });
    }

    let mut binding_inits: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .map(|f| {
            let ident = &f.ident;
            let binding = f.binding;
            match &f.kind {
                FieldKind::StorageRead => quote! {
                    ::daedalus::gpu::shader::ShaderBinding {
                        binding: #binding,
                        kind: ::daedalus::gpu::shader::BindingKind::Storage,
                        access: ::daedalus::gpu::shader::Access::ReadOnly,
                        data: ::daedalus::gpu::shader::BindingData::Buffer(
                            ::daedalus::gpu::shader::BufferInit::Bytes(::daedalus::gpu::shader::as_bytes(&self.#ident))
                        ),
                        readback: false,
                    }
                },
                FieldKind::StorageReadWrite { zeroed, readback } => {
                    let rb = *readback;
                    if *zeroed {
                        quote! {
                            ::daedalus::gpu::shader::ShaderBinding {
                                binding: #binding,
                                kind: ::daedalus::gpu::shader::BindingKind::Storage,
                                access: ::daedalus::gpu::shader::Access::ReadWrite,
                                data: ::daedalus::gpu::shader::BindingData::Buffer(::daedalus::gpu::shader::BufferInit::Zeroed(::daedalus::gpu::shader::buffer_len(&self.#ident))),
                                readback: #rb,
                            }
                        }
                    } else {
                        quote! {
                            ::daedalus::gpu::shader::ShaderBinding {
                                binding: #binding,
                                kind: ::daedalus::gpu::shader::BindingKind::Storage,
                                access: ::daedalus::gpu::shader::Access::ReadWrite,
                                data: ::daedalus::gpu::shader::BindingData::Buffer(::daedalus::gpu::shader::BufferInit::Bytes(::daedalus::gpu::shader::as_bytes(&self.#ident))),
                                readback: #rb,
                            }
                        }
                    }
                }
                FieldKind::StorageWrite { zeroed, readback } => {
                    let rb = *readback;
                    let init = if *zeroed {
                        quote! { ::daedalus::gpu::shader::BufferInit::Zeroed(::daedalus::gpu::shader::buffer_len(&self.#ident)) }
                    } else {
                        quote! { ::daedalus::gpu::shader::BufferInit::Empty(::daedalus::gpu::shader::buffer_len(&self.#ident)) }
                    };
                    quote! {
                        ::daedalus::gpu::shader::ShaderBinding {
                            binding: #binding,
                            kind: ::daedalus::gpu::shader::BindingKind::Storage,
                            access: ::daedalus::gpu::shader::Access::WriteOnly,
                            data: ::daedalus::gpu::shader::BindingData::Buffer(#init),
                            readback: #rb,
                        }
                    }
                }
                FieldKind::Uniform => quote! {
                    ::daedalus::gpu::shader::ShaderBinding {
                        binding: #binding,
                        kind: ::daedalus::gpu::shader::BindingKind::Uniform,
                        access: ::daedalus::gpu::shader::Access::ReadOnly,
                        data: ::daedalus::gpu::shader::BindingData::Buffer(::daedalus::gpu::shader::BufferInit::Bytes(::daedalus::gpu::shader::as_bytes(&self.#ident))),
                        readback: false,
                    }
                },
                FieldKind::State => {
                    match inferred_map.get(&binding) {
                        Some(InferredAccess::StorageRead) => quote! {
                            self.#ident.binding_stateful(#binding, ::daedalus::gpu::shader::Access::ReadOnly)
                        },
                        _ => quote! {
                            self.#ident.binding_stateful(#binding, ::daedalus::gpu::shader::Access::ReadWrite)
                        },
                    }
                }
                FieldKind::Texture { source, write, .. } => {
                    match source {
                        super::types::TextureSource::PayloadDynamic => quote! {{
                            let binding = #binding;
                            let write = #write;
                            let kind = if write {
                                ::daedalus::gpu::shader::BindingKind::StorageTexture2D
                            } else {
                                ::daedalus::gpu::shader::BindingKind::Texture2D
                            };
                            match &self.#ident {
                                ::daedalus::Payload::Gpu(handle) if !write => {
                                    ::daedalus::gpu::shader::ShaderBinding {
                                        binding,
                                        kind,
                                        access: ::daedalus::gpu::shader::Access::ReadOnly,
                                        data: ::daedalus::gpu::shader::BindingData::TextureHandle { handle: handle.clone() },
                                        readback: false,
                                    }
                                }
                                payload => {
                                    let (bytes, w, h) = payload
                                        .to_rgba_bytes(gpu)
                                        .map_err(|e| ::daedalus::gpu::GpuError::Internal(e.to_string()))?;
                                    ::daedalus::gpu::shader::ShaderBinding {
                                        binding,
                                        kind,
                                        access: if write {
                                            ::daedalus::gpu::shader::Access::WriteOnly
                                        } else {
                                            ::daedalus::gpu::shader::Access::ReadOnly
                                        },
                                        data: if write {
                                            ::daedalus::gpu::shader::BindingData::TextureAlloc { width: w, height: h }
                                        } else {
                                            ::daedalus::gpu::shader::BindingData::TextureRgba8 {
                                                width: w,
                                                height: h,
                                                bytes: ::std::borrow::Cow::Owned(bytes),
                                            }
                                        },
                                        readback: false,
                                    }
                                }
                            }
                        }},
                        super::types::TextureSource::DynamicImage => quote! {{
                            let rgba = self.#ident.to_rgba8();
                            let (w, h) = rgba.dimensions();
                            let kind = if #write {
                                ::daedalus::gpu::shader::BindingKind::StorageTexture2D
                            } else {
                                ::daedalus::gpu::shader::BindingKind::Texture2D
                            };
                            ::daedalus::gpu::shader::ShaderBinding {
                                binding: #binding,
                                kind,
                                access: if #write {
                                    ::daedalus::gpu::shader::Access::WriteOnly
                                } else {
                                    ::daedalus::gpu::shader::Access::ReadOnly
                                },
                                data: if #write {
                                    ::daedalus::gpu::shader::BindingData::TextureAlloc { width: w, height: h }
                                } else {
                                    ::daedalus::gpu::shader::BindingData::TextureRgba8 {
                                        width: w,
                                        height: h,
                                        bytes: ::std::borrow::Cow::Owned(rgba.into_raw()),
                                    }
                                },
                                readback: false,
                            }
                        }},
                        super::types::TextureSource::GpuHandle => quote! {{
                            let handle = self.#ident.clone();
                            let kind = if #write {
                                ::daedalus::gpu::shader::BindingKind::StorageTexture2D
                            } else {
                                ::daedalus::gpu::shader::BindingKind::Texture2D
                            };
                            ::daedalus::gpu::shader::ShaderBinding {
                                binding: #binding,
                                kind,
                                access: if #write {
                                    ::daedalus::gpu::shader::Access::WriteOnly
                                } else {
                                    ::daedalus::gpu::shader::Access::ReadOnly
                                },
                                data: if #write {
                                    ::daedalus::gpu::shader::BindingData::TextureAlloc { width: handle.width, height: handle.height }
                                } else {
                                    ::daedalus::gpu::shader::BindingData::TextureHandle { handle }
                                },
                                readback: false,
                            }
                        }},
                        super::types::TextureSource::RefGpuHandle => quote! {{
                            let handle = self.#ident.clone();
                            let kind = if #write {
                                ::daedalus::gpu::shader::BindingKind::StorageTexture2D
                            } else {
                                ::daedalus::gpu::shader::BindingKind::Texture2D
                            };
                            ::daedalus::gpu::shader::ShaderBinding {
                                binding: #binding,
                                kind,
                                access: if #write {
                                    ::daedalus::gpu::shader::Access::WriteOnly
                                } else {
                                    ::daedalus::gpu::shader::Access::ReadOnly
                                },
                                data: if #write {
                                    ::daedalus::gpu::shader::BindingData::TextureAlloc { width: handle.width, height: handle.height }
                                } else {
                                    ::daedalus::gpu::shader::BindingData::TextureHandle { handle }
                                },
                                readback: false,
                            }
                        }},
                        super::types::TextureSource::TextureOut => quote! {{
                            let dims = &self.#ident;
                            let kind = ::daedalus::gpu::shader::BindingKind::StorageTexture2D;
                            ::daedalus::gpu::shader::ShaderBinding {
                                binding: #binding,
                                kind,
                                access: ::daedalus::gpu::shader::Access::WriteOnly,
                                data: ::daedalus::gpu::shader::BindingData::TextureAlloc {
                                    width: dims.width,
                                    height: dims.height,
                                },
                                readback: dims.readback,
                            }
                        }},
                    }
                }
                FieldKind::Sampler { kind_override, address_override, mipmap_override } => {
                    let kind_token = kind_override.as_ref().and_then(|s| map_sampler_kind(s)).unwrap_or(quote! { ::daedalus::gpu::shader::SamplerKind::Filtering });
                    let addr = address_override.as_ref().and_then(|s| map_address_mode(s)).unwrap_or(quote! { ::wgpu::AddressMode::ClampToEdge });
                    let mip = mipmap_override.as_ref().and_then(|s| map_mipmap_filter(s)).unwrap_or(quote! { ::wgpu::FilterMode::Nearest });
                    quote! {
                        ::daedalus::gpu::shader::ShaderBinding {
                            binding: #binding,
                            kind: ::daedalus::gpu::shader::BindingKind::Sampler,
                            access: ::daedalus::gpu::shader::Access::ReadOnly,
                            data: ::daedalus::gpu::shader::BindingData::Sampler(::daedalus::gpu::shader::SamplerDesc {
                                address_u: #addr,
                                address_v: #addr,
                                address_w: #addr,
                                mag_filter: if matches!(#kind_token, ::daedalus::gpu::shader::SamplerKind::Filtering) { ::wgpu::FilterMode::Linear } else { ::wgpu::FilterMode::Nearest },
                                min_filter: if matches!(#kind_token, ::daedalus::gpu::shader::SamplerKind::Filtering) { ::wgpu::FilterMode::Linear } else { ::wgpu::FilterMode::Nearest },
                                mipmap_filter: #mip,
                            }),
                            readback: false,
                        }
                    }
                },
            }
        })
        .collect();

    if let Some(b) = auto_sampler_binding {
        let kind_token = if let Some(InferredAccess::Sampler(Some(k))) = inferred_map.get(&b) {
            map_sampler_kind(k)
                .unwrap_or(quote! { ::daedalus::gpu::shader::SamplerKind::Filtering })
        } else {
            quote! { ::daedalus::gpu::shader::SamplerKind::Filtering }
        };
        let addr = super::map::map_address_mode("clamp").unwrap();
        let mip = super::map::map_mipmap_filter("nearest").unwrap();
        binding_inits.push(quote! {
            ::daedalus::gpu::shader::ShaderBinding {
                binding: #b,
                kind: ::daedalus::gpu::shader::BindingKind::Sampler,
                access: ::daedalus::gpu::shader::Access::ReadOnly,
                data: ::daedalus::gpu::shader::BindingData::Sampler(::daedalus::gpu::shader::SamplerDesc {
                    address_u: #addr,
                    address_v: #addr,
                    address_w: #addr,
                    mag_filter: if matches!(#kind_token, ::daedalus::gpu::shader::SamplerKind::Filtering) { ::wgpu::FilterMode::Linear } else { ::wgpu::FilterMode::Nearest },
                    min_filter: if matches!(#kind_token, ::daedalus::gpu::shader::SamplerKind::Filtering) { ::wgpu::FilterMode::Linear } else { ::wgpu::FilterMode::Nearest },
                    mipmap_filter: #mip,
                }),
                readback: false,
            }
        });
    }

    Emitted {
        binding_specs,
        binding_inits,
    }
}

pub fn emit_invocation_hint(fields: &[FieldBinding]) -> proc_macro2::TokenStream {
    let invocation_hints: Vec<proc_macro2::TokenStream> = fields
        .iter()
        .filter_map(|f| f.invocation_hint.clone())
        .collect();
    if invocation_hints.is_empty() {
        quote! { None }
    } else {
        quote! {
            #(
                if let Some(v) = { #invocation_hints } {
                    return Some(v);
                }
            )*
            None
        }
    }
}

pub fn emit_spec_const(
    name: &syn::Ident,
    vis: &syn::Visibility,
    spec: &Spec,
    inferred_workgroup: Option<[u32; 3]>,
    binding_specs: &[proc_macro2::TokenStream],
) -> proc_macro2::TokenStream {
    let gen_spec_ident = syn::Ident::new(
        &format!("__{}_SPEC", name.to_string().to_uppercase()),
        proc_macro2::Span::call_site(),
    );
    let spec_src = &spec.src;
    let spec_entry = &spec.entry;
    let workgroup = spec.workgroup.or(inferred_workgroup);
    let spec_wg = if let Some([x, y, z]) = workgroup {
        quote! { Some([#x, #y, #z]) }
    } else {
        quote! { None }
    };
    quote! {
        #vis const #gen_spec_ident: ::daedalus::gpu::shader::ShaderSpec = ::daedalus::gpu::shader::ShaderSpec {
            name: stringify!(#name),
            src: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/", #spec_src)),
            entry: #spec_entry,
            workgroup_size: #spec_wg,
            bindings: &[ #(#binding_specs),* ],
        };

        impl #name<'_> {
            pub const SPEC: ::daedalus::gpu::shader::ShaderSpec = #gen_spec_ident;
        }
    }
}

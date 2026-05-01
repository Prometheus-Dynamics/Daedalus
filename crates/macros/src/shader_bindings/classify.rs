use std::collections::{HashMap, HashSet};

use proc_macro2::Span;
use quote::quote;

use super::map::{
    map_address_mode, map_mipmap_filter, map_sample_type, map_sampler_kind, map_texture_format,
    map_view_dimension,
};
use super::types::{
    FieldBinding, FieldKind, InferredAccess, InferredBinding, ParsedField, TextureSource,
};

pub struct Classified {
    pub fields: Vec<FieldBinding>,
    pub auto_sampler_binding: Option<u32>,
    pub inferred_map: HashMap<u32, InferredAccess>,
}

pub fn classify_fields(
    parsed_fields: Vec<ParsedField>,
    inferred: &[InferredBinding],
) -> syn::Result<Classified> {
    let mut remaining_inferred = inferred.to_vec();
    let mut inferred_map = HashMap::new();
    let mut unclaimed: HashSet<u32> = HashSet::new();
    for b in inferred {
        inferred_map.insert(b.binding, b.access.clone());
        unclaimed.insert(b.binding);
    }

    let mut fields = Vec::new();
    let mut auto_sampler_binding: Option<u32> = None;
    for pf in parsed_fields {
        let binding = if let Some(idx) = pf.binding {
            idx
        } else if let Some(next) = remaining_inferred.first() {
            let idx = next.binding;
            remaining_inferred.remove(0);
            idx
        } else {
            return Err(syn::Error::new(
                pf.ident.span(),
                "could not infer binding index; add #[gpu(binding = N)] or ensure WGSL declares enough bindings",
            ));
        };
        unclaimed.remove(&binding);

        let inferred_kind = inferred_map.get(&binding).cloned();
        let field_name = pf.ident.to_string();
        let kind = if pf.uniform_flag {
            if let Some(inf) = inferred_kind.clone() {
                if inf != InferredAccess::Uniform {
                    return Err(syn::Error::new(
                        pf.ident.span(),
                        format!(
                            "field `{}` (binding {}) is uniform in Rust but WGSL declares it as {:?}",
                            field_name, binding, inf
                        ),
                    ));
                }
            } else {
                return Err(syn::Error::new(
                    pf.ident.span(),
                    format!(
                        "field `{}` (binding {}) not found in WGSL; uniform expected",
                        field_name, binding
                    ),
                ));
            }
            FieldKind::Uniform
        } else if pf.state_flag || pf.is_state {
            if let Some(inf) = inferred_kind.clone() {
                if !matches!(
                    inf,
                    InferredAccess::StorageReadWrite
                        | InferredAccess::StorageWrite
                        | InferredAccess::StorageRead
                ) {
                    return Err(syn::Error::new(
                        pf.ident.span(),
                        format!(
                            "field `{}` (binding {}) state expects storage in WGSL, found {:?}",
                            field_name, binding, inf
                        ),
                    ));
                }
            } else {
                return Err(syn::Error::new(
                    pf.ident.span(),
                    format!(
                        "field `{}` (binding {}) not found as storage in WGSL",
                        field_name, binding
                    ),
                ));
            }
            FieldKind::State
        } else if pf.storage_flag {
            if let Some(inf) = inferred_kind.clone() {
                match (pf.storage_rw, inf.clone()) {
                    (true, InferredAccess::StorageReadWrite) => {}
                    (true, InferredAccess::StorageWrite) => {}
                    (false, InferredAccess::StorageRead) => {}
                    _ => {
                        return Err(syn::Error::new(
                            pf.ident.span(),
                            format!(
                                "field `{}` (binding {}) access mismatch with WGSL ({:?})",
                                field_name, binding, inf
                            ),
                        ));
                    }
                }
            } else {
                return Err(syn::Error::new(
                    pf.ident.span(),
                    format!(
                        "field `{}` (binding {}) not found in WGSL; storage expected",
                        field_name, binding
                    ),
                ));
            }
            if pf.storage_rw {
                FieldKind::StorageReadWrite {
                    zeroed: pf.zeroed,
                    readback: pf.readback,
                }
            } else {
                FieldKind::StorageRead
            }
        } else if pf.texture_flag {
            let mut format_override = pf.tex_format.clone();
            let mut sample_override = pf.tex_sample_type.clone();
            let mut view_override = pf.tex_view.clone();
            match inferred_kind.clone() {
                Some(InferredAccess::Texture {
                    format,
                    sample_type,
                    view,
                }) => {
                    if let Some(fmt) = format {
                        if let Some(over) = &format_override {
                            if !over.eq_ignore_ascii_case(&fmt) {
                                return Err(syn::Error::new(
                                    pf.ident.span(),
                                    format!(
                                        "field `{}` (binding {}) texture format mismatch: WGSL `{}` vs override `{}`",
                                        field_name, binding, fmt, over
                                    ),
                                ));
                            }
                        } else {
                            format_override = Some(fmt);
                        }
                    }
                    if let Some(sty) = sample_type {
                        if let Some(over) = &sample_override {
                            if !over.eq_ignore_ascii_case(&sty) {
                                return Err(syn::Error::new(
                                    pf.ident.span(),
                                    format!(
                                        "field `{}` (binding {}) texture sample type mismatch: WGSL `{}` vs override `{}`",
                                        field_name, binding, sty, over
                                    ),
                                ));
                            }
                        } else {
                            sample_override = Some(sty);
                        }
                    }
                    if let Some(view) = view {
                        if let Some(over) = &view_override {
                            if !over.eq_ignore_ascii_case(&view) {
                                return Err(syn::Error::new(
                                    pf.ident.span(),
                                    format!(
                                        "field `{}` (binding {}) texture view mismatch: WGSL `{}` vs override `{}`",
                                        field_name, binding, view, over
                                    ),
                                ));
                            }
                        } else {
                            view_override = Some(view);
                        }
                    }
                }
                Some(InferredAccess::StorageTexture { format, view }) => {
                    if let Some(fmt) = format {
                        if let Some(over) = &format_override {
                            if !over.eq_ignore_ascii_case(&fmt) {
                                return Err(syn::Error::new(
                                    pf.ident.span(),
                                    format!(
                                        "field `{}` (binding {}) storage texture format mismatch: WGSL `{}` vs override `{}`",
                                        field_name, binding, fmt, over
                                    ),
                                ));
                            }
                        } else {
                            format_override = Some(fmt);
                        }
                    }
                    if let Some(view) = view {
                        if let Some(over) = &view_override {
                            if !over.eq_ignore_ascii_case(&view) {
                                return Err(syn::Error::new(
                                    pf.ident.span(),
                                    format!(
                                        "field `{}` (binding {}) storage texture view mismatch: WGSL `{}` vs override `{}`",
                                        field_name, binding, view, over
                                    ),
                                ));
                            }
                        } else {
                            view_override = Some(view);
                        }
                    }
                    // Storage textures ignore sample types.
                    sample_override = None;
                }
                _ => {
                    return Err(syn::Error::new(
                        pf.ident.span(),
                        format!(
                            "field `{}` (binding {}) not found as texture in WGSL",
                            field_name, binding
                        ),
                    ));
                }
            }
            let final_format = format_override;
            if let Some(fmt) = &final_format
                && map_texture_format(fmt).is_none()
            {
                return Err(syn::Error::new(
                    pf.ident.span(),
                    format!(
                        "field `{}` (binding {}) texture format `{}` is not supported",
                        field_name, binding, fmt
                    ),
                ));
            }
            let final_sample = sample_override;
            if let Some(sty) = &final_sample
                && map_sample_type(sty).is_none()
            {
                return Err(syn::Error::new(
                    pf.ident.span(),
                    format!(
                        "field `{}` (binding {}) texture sample_type `{}` is not supported",
                        field_name, binding, sty
                    ),
                ));
            }
            let final_view = view_override;
            if let Some(v) = &final_view
                && map_view_dimension(v).is_none()
            {
                return Err(syn::Error::new(
                    pf.ident.span(),
                    format!(
                        "field `{}` (binding {}) texture view `{}` is not supported",
                        field_name, binding, v
                    ),
                ));
            }
            if !pf.storage_rw {
                auto_sampler_binding = auto_sampler_binding.or_else(|| {
                    inferred.iter().find_map(|b| {
                        if matches!(b.access, InferredAccess::Sampler(_)) {
                            Some(b.binding)
                        } else {
                            None
                        }
                    })
                });
            }
            FieldKind::Texture {
                source: pf
                    .texture_source
                    .clone()
                    .ok_or_else(|| syn::Error::new(pf.ident.span(), "texture source unknown"))?,
                write: pf.storage_rw,
                format_override: final_format,
                sample_type_override: final_sample,
                view_override: final_view,
            }
        } else if pf.sampler_flag {
            let mut kind_override = pf.sampler_kind.clone();
            if let Some(InferredAccess::Sampler(kind)) = inferred_kind.clone() {
                if let Some(k) = kind {
                    if let Some(over) = &kind_override {
                        if !over.eq_ignore_ascii_case(&k) {
                            return Err(syn::Error::new(
                                pf.ident.span(),
                                format!(
                                    "field `{}` (binding {}) sampler type mismatch: WGSL `{}` vs override `{}`",
                                    field_name, binding, k, over
                                ),
                            ));
                        }
                    } else {
                        kind_override = Some(k);
                    }
                }
            } else {
                return Err(syn::Error::new(
                    pf.ident.span(),
                    format!(
                        "field `{}` (binding {}) not found as sampler in WGSL",
                        field_name, binding
                    ),
                ));
            }
            if let Some(k) = &kind_override
                && map_sampler_kind(k).is_none()
            {
                return Err(syn::Error::new(
                    pf.ident.span(),
                    format!(
                        "field `{}` (binding {}) sampler kind `{}` is not supported",
                        field_name, binding, k
                    ),
                ));
            }
            if let Some(addr) = &pf.sampler_address
                && map_address_mode(addr).is_none()
            {
                return Err(syn::Error::new(
                    pf.ident.span(),
                    format!(
                        "field `{}` (binding {}) sampler address `{}` is not supported",
                        field_name, binding, addr
                    ),
                ));
            }
            if let Some(mip) = &pf.sampler_mipmap
                && map_mipmap_filter(mip).is_none()
            {
                return Err(syn::Error::new(
                    pf.ident.span(),
                    format!(
                        "field `{}` (binding {}) sampler mipmap `{}` is not supported",
                        field_name, binding, mip
                    ),
                ));
            }
            FieldKind::Sampler {
                kind_override,
                address_override: pf.sampler_address.clone(),
                mipmap_override: pf.sampler_mipmap.clone(),
            }
        } else {
            let inferred = inferred_kind.ok_or_else(|| {
                syn::Error::new(
                    pf.ident.span(),
                    format!(
                        "field `{}` (binding {}) not found in WGSL; add #[gpu(binding = N)]",
                        field_name, binding
                    ),
                )
            })?;
            match inferred {
                InferredAccess::StorageRead => FieldKind::StorageRead,
                InferredAccess::StorageReadWrite => FieldKind::StorageReadWrite {
                    zeroed: false,
                    readback: pf.readback,
                },
                InferredAccess::StorageWrite => FieldKind::StorageWrite {
                    zeroed: pf.zeroed,
                    readback: pf.readback,
                },
                InferredAccess::Uniform => FieldKind::Uniform,
                InferredAccess::Texture { .. } => {
                    return Err(syn::Error::new(
                        pf.ident.span(),
                        format!(
                            "field `{}` (binding {}) is a texture in WGSL; add #[gpu(texture2d)]",
                            field_name, binding
                        ),
                    ));
                }
                InferredAccess::StorageTexture { .. } => {
                    return Err(syn::Error::new(
                        pf.ident.span(),
                        format!(
                            "field `{}` (binding {}) is a storage texture in WGSL; add #[gpu(texture2d(write = true))]",
                            field_name, binding
                        ),
                    ));
                }
                InferredAccess::Sampler(_) => {
                    return Err(syn::Error::new(
                        pf.ident.span(),
                        format!(
                            "field `{}` (binding {}) is a sampler in WGSL; add #[gpu(sampler)]",
                            field_name, binding
                        ),
                    ));
                }
            }
        };

        if matches!(kind, FieldKind::Uniform) && (pf.zeroed || pf.readback) {
            return Err(syn::Error::new(
                pf.ident.span(),
                "gpu(zeroed)/gpu(readback) only apply to storage buffers",
            ));
        }

        let ident = pf.ident.clone();
        let inv_ident = ident.clone();
        fields.push(FieldBinding {
            ident,
            binding,
            kind,
            invocation_hint: match &pf.texture_source {
                Some(TextureSource::ComputeDynamic) => Some(quote! {
                    match &self.#inv_ident {
                        ::daedalus::Compute::Cpu(img) => {
                            let (w, h) = img.dimensions();
                            Some([w, h, 1])
                        }
                        ::daedalus::Compute::Gpu(handle) => Some([handle.width, handle.height, 1]),
                    }
                }),
                Some(TextureSource::DynamicImage) => Some(quote! {
                    let (w, h) = self.#inv_ident.dimensions();
                    Some([w, h, 1])
                }),
                Some(TextureSource::GpuHandle) | Some(TextureSource::RefGpuHandle) => {
                    Some(quote! {
                        let h = self.#inv_ident;
                        Some([h.width, h.height, 1])
                    })
                }
                Some(TextureSource::TextureOut) => {
                    Some(quote! { Some([self.#inv_ident.width, self.#inv_ident.height, 1]) })
                }
                None => None,
            },
        });
    }

    if let Some(b) = auto_sampler_binding {
        unclaimed.remove(&b);
    }

    if !unclaimed.is_empty() {
        let missing: Vec<String> = unclaimed.into_iter().map(|b| format!("{b}")).collect();
        return Err(syn::Error::new(
            Span::call_site(),
            format!(
                "bindings declared in WGSL but not mapped in struct: {}",
                missing.join(", ")
            ),
        ));
    }

    Ok(Classified {
        fields,
        auto_sampler_binding,
        inferred_map,
    })
}

use proc_macro2::Span;
use syn::{DeriveInput, LitInt, LitStr};

use super::types::{ParsedField, Spec};

pub fn parse_spec(input: &DeriveInput) -> syn::Result<Spec> {
    let mut spec: Option<Spec> = None;
    for attr in &input.attrs {
        if !attr.path().is_ident("gpu") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("spec") {
                let mut src = None;
                let mut entry = "main".to_string();
                let mut workgroup = None;
                meta.parse_nested_meta(|inner| {
                    if inner.path.is_ident("src") {
                        let lit: LitStr = inner.value()?.parse()?;
                        src = Some(lit.value());
                        return Ok(());
                    }
                    if inner.path.is_ident("entry") {
                        let lit: LitStr = inner.value()?.parse()?;
                        entry = lit.value();
                        return Ok(());
                    }
                    if inner.path.is_ident("workgroup_size") {
                        let lit: LitInt = inner.value()?.parse()?;
                        if let Ok(v) = lit.base10_parse::<u32>() {
                            workgroup = Some([v, 1, 1]);
                        }
                        return Ok(());
                    }
                    Ok(())
                })?;
                let Some(src) = src else {
                    return Err(syn::Error::new(
                        Span::call_site(),
                        "spec(...) requires src=\"path\"",
                    ));
                };
                spec = Some(Spec {
                    src,
                    entry,
                    workgroup,
                });
            }
            Ok(())
        })?;
    }

    spec.ok_or_else(|| syn::Error::new(Span::call_site(), "missing #[gpu(spec(...))] attribute"))
}

pub fn parse_fields(data: &syn::DataStruct) -> syn::Result<Vec<ParsedField>> {
    let mut parsed_fields = Vec::new();
    for field in &data.fields {
        let ident = field
            .ident
            .clone()
            .ok_or_else(|| syn::Error::new(Span::call_site(), "named fields required"))?;
        let mut binding_idx = None;
        let mut storage_rw = false;
        let mut storage_flag = false;
        let mut uniform_flag = false;
        let mut zeroed = false;
        let mut readback = false;
        let mut texture_flag = false;
        let mut state_flag = false;
        let mut sampler_flag = false;
        let mut tex_format = None;
        let mut tex_sample_type = None;
        let mut tex_view = None;
        let mut sampler_kind = None;
        let mut sampler_address = None;
        let mut sampler_mipmap = None;
        let mut is_buffer_out = false;
        for attr in &field.attrs {
            if !attr.path().is_ident("gpu") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("binding") {
                    let lit: LitInt = meta.value()?.parse()?;
                    binding_idx = lit.base10_parse().ok();
                    return Ok(());
                }
                if meta.path.is_ident("zeroed") {
                    zeroed = true;
                    return Ok(());
                }
                if meta.path.is_ident("readback") {
                    readback = true;
                    return Ok(());
                }
                if meta.path.is_ident("storage") {
                    meta.parse_nested_meta(|inner| {
                        if inner.path.is_ident("read_write") {
                            storage_rw = true;
                        }
                        Ok(())
                    })?;
                    storage_flag = true;
                    return Ok(());
                }
                if meta.path.is_ident("uniform") {
                    uniform_flag = true;
                    return Ok(());
                }
                if meta.path.is_ident("state") {
                    state_flag = true;
                    return Ok(());
                }
                if meta.path.is_ident("texture2d") {
                    meta.parse_nested_meta(|inner| {
                        if inner.path.is_ident("read_write") || inner.path.is_ident("write") {
                            storage_rw = true;
                        }
                        if inner.path.is_ident("format") {
                            let lit: LitStr = inner.value()?.parse()?;
                            tex_format = Some(lit.value());
                        }
                        if inner.path.is_ident("sample_type") {
                            let lit: LitStr = inner.value()?.parse()?;
                            tex_sample_type = Some(lit.value());
                        }
                        if inner.path.is_ident("view") {
                            let lit: LitStr = inner.value()?.parse()?;
                            tex_view = Some(lit.value());
                        }
                        Ok(())
                    })?;
                    texture_flag = true;
                    return Ok(());
                }
                if meta.path.is_ident("sampler") {
                    meta.parse_nested_meta(|inner| {
                        if inner.path.is_ident("filter") {
                            let lit: LitStr = inner.value()?.parse()?;
                            sampler_kind = Some(lit.value());
                        }
                        if inner.path.is_ident("address") {
                            let lit: LitStr = inner.value()?.parse()?;
                            sampler_address = Some(lit.value());
                        }
                        if inner.path.is_ident("mipmap") {
                            let lit: LitStr = inner.value()?.parse()?;
                            sampler_mipmap = Some(lit.value());
                        }
                        Ok(())
                    })?;
                    sampler_flag = true;
                    return Ok(());
                }
                Ok(())
            })?;
        }
        let texture_source = if texture_flag {
            super::types::detect_texture_source(&field.ty)?
        } else {
            None
        };
        let is_state = if let syn::Type::Path(tp) = &field.ty {
            tp.path
                .segments
                .last()
                .map(|s| s.ident == "GpuState")
                .unwrap_or(false)
        } else {
            false
        };
        if let syn::Type::Path(tp) = &field.ty {
            if tp
                .path
                .segments
                .last()
                .map(|s| s.ident == "BufferOut")
                .unwrap_or(false)
            {
                is_buffer_out = true;
            }
        }
        parsed_fields.push(ParsedField {
            ident,
            binding: binding_idx,
            storage_flag,
            storage_rw,
            uniform_flag,
            zeroed,
            readback,
            texture_flag,
            state_flag,
            sampler_flag,
            texture_source,
            tex_format,
            tex_sample_type,
            tex_view,
            sampler_kind,
            sampler_address,
            sampler_mipmap,
            _is_buffer_out: is_buffer_out,
            is_state,
        });
    }
    Ok(parsed_fields)
}

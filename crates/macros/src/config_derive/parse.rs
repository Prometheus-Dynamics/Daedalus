//! Attribute parsing for `#[derive(NodeConfig)]`.

use proc_macro2::Span;
use quote::ToTokens;
use syn::{Attribute, Field, Lit, LitStr, Meta, MetaNameValue};

use crate::helpers::{NestedMeta, compile_error, lit_from_expr, lit_str_arg, parse_nested};

use super::model::PortSpec;

/// Parse the optional struct-level `#[validate(fn = path::to::validator)]` attribute.
pub(super) fn parse_validate_fn(
    attrs: &[Attribute],
) -> Result<Option<syn::Path>, proc_macro2::TokenStream> {
    let mut validate_fn: Option<syn::Path> = None;
    for attr in attrs {
        if attr.path().is_ident("validate") {
            let Meta::List(list) = &attr.meta else {
                return Err(compile_error(
                    "validate attribute expects validate(fn = path::to::validator)".into(),
                ));
            };
            let Ok(items) = parse_nested(list) else {
                return Err(compile_error(
                    "validate(...) expects comma-separated arguments".into(),
                ));
            };
            for item in items {
                if let NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. })) = item
                    && path.is_ident("fn")
                {
                    if let Some(Lit::Str(s)) = lit_from_expr(&value) {
                        match syn::parse_str::<syn::Path>(&s.value()) {
                            Ok(p) => validate_fn = Some(p),
                            Err(_) => {
                                return Err(compile_error(
                                    "validate fn must be a valid path".into(),
                                ));
                            }
                        }
                        continue;
                    }
                    match syn::parse2::<syn::Path>(value.to_token_stream()) {
                        Ok(p) => validate_fn = Some(p),
                        Err(_) => {
                            return Err(compile_error("validate fn must be a valid path".into()));
                        }
                    }
                }
            }
        }
    }
    Ok(validate_fn)
}

/// Parse one named struct field and its `#[port(...)]` attributes.
pub(super) fn parse_port_spec(field: Field) -> Result<PortSpec, proc_macro2::TokenStream> {
    let field_ident = field.ident.clone().expect("named field ident");
    let field_ty = field.ty.clone();
    let mut name = LitStr::new(&field_ident.to_string(), Span::call_site());
    let mut source: Option<LitStr> = None;
    let mut description: Option<LitStr> = None;
    let mut default_value: Option<Lit> = None;
    let mut min_value: Option<Lit> = None;
    let mut max_value: Option<Lit> = None;
    let mut odd = false;
    let mut policy: Option<LitStr> = None;
    let mut ty_override: Option<proc_macro2::TokenStream> = None;
    let mut meta: Vec<(LitStr, Lit)> = Vec::new();

    for attr in &field.attrs {
        if !attr.path().is_ident("port") {
            continue;
        }
        let Meta::List(list) = &attr.meta else {
            return Err(compile_error("port attribute expects port(...)".into()));
        };
        let Ok(items) = parse_nested(list) else {
            return Err(compile_error(
                "port(...) expects comma-separated arguments".into(),
            ));
        };
        for item in items {
            match item {
                NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. })) => {
                    if path.is_ident("name")
                        && let Some(Lit::Str(s)) = lit_from_expr(&value)
                    {
                        name = s;
                        continue;
                    }
                    if path.is_ident("source")
                        && let Some(Lit::Str(s)) = lit_from_expr(&value)
                    {
                        source = Some(s);
                        continue;
                    }
                    if path.is_ident("description") {
                        description = Some(lit_str_arg(&value, "port description")?);
                        continue;
                    }
                    if path.is_ident("default")
                        && let Some(lit) = lit_from_expr(&value)
                    {
                        default_value = Some(lit);
                        continue;
                    }
                    if path.is_ident("min")
                        && let Some(lit) = lit_from_expr(&value)
                    {
                        min_value = Some(lit);
                        continue;
                    }
                    if path.is_ident("max")
                        && let Some(lit) = lit_from_expr(&value)
                    {
                        max_value = Some(lit);
                        continue;
                    }
                    if path.is_ident("odd")
                        && let Some(Lit::Bool(b)) = lit_from_expr(&value)
                    {
                        odd = b.value;
                        continue;
                    }
                    if path.is_ident("policy") {
                        policy = Some(lit_str_arg(&value, "policy")?);
                        continue;
                    }
                    if path.is_ident("ty") {
                        ty_override = Some(value.to_token_stream());
                        continue;
                    }
                    return Err(compile_error("unsupported port attribute".into()));
                }
                NestedMeta::Meta(Meta::List(list))
                    if list.path.is_ident("meta") || list.path.is_ident("metadata") =>
                {
                    let Ok(items) = parse_nested(&list) else {
                        return Err(compile_error(
                            "meta(...) expects comma-separated arguments".into(),
                        ));
                    };
                    for item in items {
                        let NestedMeta::Meta(Meta::NameValue(MetaNameValue {
                            path, value, ..
                        })) = item
                        else {
                            return Err(compile_error(
                                "meta(...) entries must be name/value pairs".into(),
                            ));
                        };
                        let Some(ident) = path.get_ident() else {
                            return Err(compile_error(
                                "meta keys must be simple identifiers".into(),
                            ));
                        };
                        let Some(lit) = lit_from_expr(&value) else {
                            return Err(compile_error("meta values must be literal values".into()));
                        };
                        meta.push((LitStr::new(&ident.to_string(), Span::call_site()), lit));
                    }
                }
                NestedMeta::Meta(Meta::Path(path)) => {
                    if path.is_ident("odd") {
                        odd = true;
                        continue;
                    }
                    return Err(compile_error("unsupported port flag".into()));
                }
                _ => {
                    return Err(compile_error(
                        "port(...) entries must be name/value pairs".into(),
                    ));
                }
            }
        }
    }

    Ok(PortSpec {
        field_ident,
        field_ty,
        name,
        source,
        description,
        default_value,
        min_value,
        max_value,
        odd,
        policy,
        ty_override,
        meta,
    })
}

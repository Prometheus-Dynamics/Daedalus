use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::Span;
use quote::{ToTokens, quote};
use syn::{Data, DeriveInput, Fields, Lit, LitStr, Meta, MetaNameValue, parse_macro_input};

use crate::helpers::{NestedMeta, compile_error, lit_from_expr, parse_nested};

#[derive(Clone, Copy, PartialEq, Eq)]
enum NumberKind {
    Int,
    Float,
}

fn number_kind(ty: &syn::Type) -> Option<NumberKind> {
    match ty {
        syn::Type::Path(p) if p.qself.is_none() => {
            let ident = p.path.segments.last()?.ident.to_string();
            match ident.as_str() {
                "f32" | "f64" => Some(NumberKind::Float),
                "i8" | "i16" | "i32" | "i64" | "i128" | "isize" | "u8" | "u16" | "u32" | "u64"
                | "u128" | "usize" => Some(NumberKind::Int),
                _ => None,
            }
        }
        syn::Type::Reference(r) => number_kind(&r.elem),
        _ => None,
    }
}

struct PortSpec {
    field_ident: syn::Ident,
    field_ty: syn::Type,
    name: LitStr,
    source: Option<LitStr>,
    description: Option<LitStr>,
    default_value: Option<Lit>,
    min_value: Option<Lit>,
    max_value: Option<Lit>,
    odd: bool,
    policy: Option<LitStr>,
    ty_override: Option<proc_macro2::TokenStream>,
    meta: Vec<(LitStr, Lit)>,
}

pub fn node_config(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let struct_ident = input.ident.clone();
    let generics = input.generics.clone();
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let daedalus_root: Option<String> = crate_name("daedalus-rs")
        .or_else(|_| crate_name("daedalus"))
        .ok()
        .map(|fc| match fc {
            FoundCrate::Itself => "daedalus".to_string(),
            FoundCrate::Name(name) => {
                if name == "daedalus_rs" {
                    "daedalus".to_string()
                } else {
                    name
                }
            }
        });
    let crate_path = |pkg: &str, fallback: &str, subpath: Option<&str>| {
        if let Some(root) = &daedalus_root {
            let root_ident = syn::Ident::new(root, Span::call_site());
            if let Some(sub) = subpath {
                let sub_ident = syn::Ident::new(sub, Span::call_site());
                return quote! { ::#root_ident::#sub_ident };
            }
            return quote! { ::#root_ident };
        }
        let name = crate_name(pkg)
            .ok()
            .map(|fc| match fc {
                FoundCrate::Itself => pkg.replace('-', "_"),
                FoundCrate::Name(name) => name,
            })
            .unwrap_or_else(|| fallback.to_string());
        let ident = syn::Ident::new(&name, Span::call_site());
        quote! { ::#ident }
    };
    let runtime_crate = crate_path("daedalus-runtime", "daedalus_runtime", Some("runtime"));
    let registry_crate = crate_path("daedalus-registry", "daedalus_registry", Some("registry"));
    let data_crate = crate_path("daedalus-data", "daedalus_data", Some("data"));

    let mut validate_fn: Option<syn::Path> = None;
    for attr in &input.attrs {
        if attr.path().is_ident("validate") {
            let Meta::List(list) = &attr.meta else {
                return TokenStream::from(compile_error(
                    "validate attribute expects validate(fn = path::to::validator)".into(),
                ));
            };
            let Ok(items) = parse_nested(list) else {
                return TokenStream::from(compile_error(
                    "validate(...) expects comma-separated arguments".into(),
                ));
            };
            for item in items {
                if let NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. })) = item {
                    if path.is_ident("fn") {
                        if let Some(Lit::Str(s)) = lit_from_expr(&value) {
                            match syn::parse_str::<syn::Path>(&s.value()) {
                                Ok(p) => validate_fn = Some(p),
                                Err(_) => {
                                    return TokenStream::from(compile_error(
                                        "validate fn must be a valid path".into(),
                                    ));
                                }
                            }
                            continue;
                        }
                        match syn::parse2::<syn::Path>(value.to_token_stream()) {
                            Ok(p) => validate_fn = Some(p),
                            Err(_) => {
                                return TokenStream::from(compile_error(
                                    "validate fn must be a valid path".into(),
                                ));
                            }
                        }
                    }
                }
            }
        }
    }

    let fields = match input.data {
        Data::Struct(ds) => ds.fields,
        _ => {
            return TokenStream::from(compile_error(
                "NodeConfig can only be derived for structs".into(),
            ));
        }
    };
    let named_fields = match fields {
        Fields::Named(named) => named.named,
        _ => {
            return TokenStream::from(compile_error(
                "NodeConfig requires named struct fields".into(),
            ));
        }
    };

    let generic_type_params: ::std::collections::HashSet<::std::string::String> = input
        .generics
        .type_params()
        .map(|tp| tp.ident.to_string())
        .collect();

    let mut specs: Vec<PortSpec> = Vec::new();
    for field in named_fields {
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
                return TokenStream::from(compile_error("port attribute expects port(...)".into()));
            };
            let Ok(items) = parse_nested(list) else {
                return TokenStream::from(compile_error(
                    "port(...) expects comma-separated arguments".into(),
                ));
            };
            for item in items {
                match item {
                    NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. })) => {
                        if path.is_ident("name") {
                            if let Some(Lit::Str(s)) = lit_from_expr(&value) {
                                name = s;
                                continue;
                            }
                        }
                        if path.is_ident("source") {
                            if let Some(Lit::Str(s)) = lit_from_expr(&value) {
                                source = Some(s);
                                continue;
                            }
                        }
                        if path.is_ident("description") {
                            if let Some(Lit::Str(s)) = lit_from_expr(&value) {
                                description = Some(s);
                                continue;
                            }
                            return TokenStream::from(compile_error(
                                "port description must be a string literal".into(),
                            ));
                        }
                        if path.is_ident("default") {
                            if let Some(lit) = lit_from_expr(&value) {
                                default_value = Some(lit);
                                continue;
                            }
                        }
                        if path.is_ident("min") {
                            if let Some(lit) = lit_from_expr(&value) {
                                min_value = Some(lit);
                                continue;
                            }
                        }
                        if path.is_ident("max") {
                            if let Some(lit) = lit_from_expr(&value) {
                                max_value = Some(lit);
                                continue;
                            }
                        }
                        if path.is_ident("odd") {
                            if let Some(Lit::Bool(b)) = lit_from_expr(&value) {
                                odd = b.value;
                                continue;
                            }
                        }
                        if path.is_ident("policy") {
                            if let Some(Lit::Str(s)) = lit_from_expr(&value) {
                                policy = Some(s);
                                continue;
                            }
                            return TokenStream::from(compile_error(
                                "policy must be a string literal".into(),
                            ));
                        }
                        if path.is_ident("ty") {
                            ty_override = Some(value.to_token_stream());
                            continue;
                        }
                        return TokenStream::from(compile_error(
                            "unsupported port attribute".into(),
                        ));
                    }
                    NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("meta") || list.path.is_ident("metadata") => {
                        let Ok(items) = parse_nested(&list) else {
                            return TokenStream::from(compile_error(
                                "meta(...) expects comma-separated arguments".into(),
                            ));
                        };
                        for item in items {
                            let NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. })) = item else {
                                return TokenStream::from(compile_error(
                                    "meta(...) entries must be name/value pairs".into(),
                                ));
                            };
                            let Some(ident) = path.get_ident() else {
                                return TokenStream::from(compile_error(
                                    "meta keys must be simple identifiers".into(),
                                ));
                            };
                            let Some(lit) = lit_from_expr(&value) else {
                                return TokenStream::from(compile_error(
                                    "meta values must be literal values".into(),
                                ));
                            };
                            meta.push((LitStr::new(&ident.to_string(), Span::call_site()), lit));
                        }
                    }
                    NestedMeta::Meta(Meta::Path(path)) => {
                        if path.is_ident("odd") {
                            odd = true;
                            continue;
                        }
                        return TokenStream::from(compile_error("unsupported port flag".into()));
                    }
                    _ => {
                        return TokenStream::from(compile_error(
                            "port(...) entries must be name/value pairs".into(),
                        ));
                    }
                }
            }
        }

        specs.push(PortSpec {
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
        });
    }

    fn type_expr_for(
        ty: &syn::Type,
        generic_type_params: &::std::collections::HashSet<::std::string::String>,
        data_crate: &proc_macro2::TokenStream,
    ) -> Option<proc_macro2::TokenStream> {
        match ty {
            syn::Type::Path(p) if p.qself.is_none() => {
                if p.path.segments.len() == 1
                    && matches!(
                        p.path.segments.first().map(|s| &s.arguments),
                        Some(syn::PathArguments::None)
                    )
                {
                    let ident = p.path.segments.first()?.ident.to_string();
                    if generic_type_params.contains(&ident) {
                        return None;
                    }
                }
                let ident = p.path.segments.last().map(|s| s.ident.to_string())?;
                match ident.as_str() {
                    "Result" => p.path.segments.last().and_then(|s| match &s.arguments {
                        syn::PathArguments::AngleBracketed(ab) => ab.args.first(),
                        _ => None,
                    }).and_then(|arg| {
                        if let syn::GenericArgument::Type(inner) = arg {
                            type_expr_for(inner, generic_type_params, data_crate)
                        } else {
                            None
                        }
                    }),
                    "Vec" => p
                        .path
                        .segments
                        .last()
                        .and_then(|s| match &s.arguments {
                            syn::PathArguments::AngleBracketed(ab) => ab.args.first(),
                            _ => None,
                        })
                        .and_then(|arg| {
                            if let syn::GenericArgument::Type(inner) = arg {
                                if let Some(inner_ty) = type_expr_for(inner, generic_type_params, data_crate) {
                                    return Some(
                                        quote! {
                                            if let Some(explicit) = #data_crate::typing::override_type_expr::<#ty>() {
                                                explicit
                                            } else {
                                                #data_crate::model::TypeExpr::List(Box::new(#inner_ty))
                                            }
                                        },
                                    );
                                }
                            }
                            None
                        }),
                    "Option" => p
                        .path
                        .segments
                        .last()
                        .and_then(|s| match &s.arguments {
                            syn::PathArguments::AngleBracketed(ab) => ab.args.first(),
                            _ => None,
                        })
                        .and_then(|arg| {
                            if let syn::GenericArgument::Type(inner) = arg {
                                if let Some(inner_ty) = type_expr_for(inner, generic_type_params, data_crate) {
                                    return Some(
                                        quote! {
                                            if let Some(explicit) = #data_crate::typing::override_type_expr::<#ty>() {
                                                explicit
                                            } else {
                                                #data_crate::model::TypeExpr::Optional(Box::new(#inner_ty))
                                            }
                                        },
                                    );
                                }
                            }
                            None
                        }),
                    _ => Some(quote! { #data_crate::typing::type_expr::<#ty>() }),
                }
            }
            syn::Type::Reference(r) => {
                if let syn::Type::Path(p) = &*r.elem {
                    let ident = p.path.segments.last().map(|s| s.ident.to_string())?;
                    match ident.as_str() {
                        "str" => Some(
                            quote! { #data_crate::model::TypeExpr::Scalar(#data_crate::model::ValueType::String) },
                        ),
                        _ => type_expr_for(&r.elem, generic_type_params, data_crate),
                    }
                } else {
                    type_expr_for(&r.elem, generic_type_params, data_crate)
                }
            }
            syn::Type::Tuple(t) => {
                if t.elems.is_empty() {
                    return Some(
                        quote! { #data_crate::model::TypeExpr::Scalar(#data_crate::model::ValueType::Unit) },
                    );
                }
                let mut elems = Vec::new();
                for elem in &t.elems {
                    if let Some(ts) = type_expr_for(elem, generic_type_params, data_crate) {
                        elems.push(ts);
                    } else {
                        return None;
                    }
                }
                Some(quote! { #data_crate::model::TypeExpr::Tuple(vec![#(#elems),*]) })
            }
            _ => None,
        }
    }

    fn opaque_fallback_type_expr_for(
        ty: &syn::Type,
        data_crate: &proc_macro2::TokenStream,
    ) -> proc_macro2::TokenStream {
        let mut raw = ty.to_token_stream().to_string();
        raw.retain(|c| !c.is_whitespace());
        let lit = LitStr::new(&format!("rust:{raw}"), Span::call_site());
        quote! { #data_crate::model::TypeExpr::Opaque(::std::string::String::from(#lit)) }
    }

    let mut errors: Vec<proc_macro2::TokenStream> = Vec::new();
    for spec in &specs {
        let needs_numeric = spec.min_value.is_some() || spec.max_value.is_some() || spec.odd;
        if needs_numeric && number_kind(&spec.field_ty).is_none() {
            errors.push(compile_error(format!(
                "port `{}` uses numeric constraints but field type is not numeric",
                spec.field_ident
            )));
        }
        if let Some(Lit::Bool(_)) = spec.min_value {
            errors.push(compile_error("min must be an int/float literal".into()));
        }
        if let Some(Lit::Bool(_)) = spec.max_value {
            errors.push(compile_error("max must be an int/float literal".into()));
        }
        if let Some(policy) = &spec.policy {
            if !matches!(policy.value().as_str(), "clamp" | "error") {
                errors.push(compile_error(
                    "policy must be \"clamp\" or \"error\"".into(),
                ));
            }
        }
    }
    if !errors.is_empty() {
        return TokenStream::from(quote! { #(#errors)* });
    }

    let ports_tokens: Vec<proc_macro2::TokenStream> = specs
        .iter()
        .map(|spec| {
            let name = &spec.name;
            let source = spec
                .source
                .as_ref()
                .map(|s| quote! { Some(#s.into()) })
                .unwrap_or_else(|| quote! { None });
            let ty_expr = if let Some(ty) = &spec.ty_override {
                quote! { (#ty) }
            } else if let Some(ts) =
                type_expr_for(&spec.field_ty, &generic_type_params, &data_crate)
            {
                ts
            } else {
                opaque_fallback_type_expr_for(&spec.field_ty, &data_crate)
            };
            let default_value = spec.default_value.as_ref().map(|lit| match lit {
                Lit::Str(s) => {
                    quote! { Some(#data_crate::model::Value::String(::std::borrow::Cow::from(#s))) }
                }
                Lit::Int(i) => {
                    let v: i64 = i.base10_parse().unwrap_or(0);
                    quote! { Some(#data_crate::model::Value::Int(#v)) }
                }
                Lit::Float(f) => {
                    let v: f64 = f.base10_parse().unwrap_or(0.0);
                    quote! { Some(#data_crate::model::Value::Float(#v)) }
                }
                Lit::Bool(b) => {
                    let v = b.value;
                    quote! { Some(#data_crate::model::Value::Bool(#v)) }
                }
                _ => quote! { None },
            }).unwrap_or_else(|| quote! { None });
            quote! {
                #registry_crate::store::Port {
                    name: #name.into(),
                    ty: #ty_expr,
                    access: ::core::default::Default::default(),
                    source: #source,
                    const_value: #default_value,
                }
            }
        })
        .collect();

    let metadata_tokens: Vec<proc_macro2::TokenStream> = specs
        .iter()
        .flat_map(|spec| {
            let mut entries = Vec::new();
            if let Some(desc) = &spec.description {
                let key = LitStr::new(
                    &format!("inputs.{}.description", spec.name.value()),
                    Span::call_site(),
                );
                entries.push(quote! {
                    __meta.insert(
                        ::std::string::String::from(#key),
                        #data_crate::model::Value::String(::std::borrow::Cow::from(#desc)),
                    );
                });
            }
            if let Some(policy) = &spec.policy {
                let key = LitStr::new(
                    &format!("inputs.{}.policy", spec.name.value()),
                    Span::call_site(),
                );
                entries.push(quote! {
                    __meta.insert(
                        ::std::string::String::from(#key),
                        #data_crate::model::Value::String(::std::borrow::Cow::from(#policy)),
                    );
                });
            }
            if spec.odd {
                let key = LitStr::new(
                    &format!("inputs.{}.odd", spec.name.value()),
                    Span::call_site(),
                );
                entries.push(quote! {
                    __meta.insert(
                        ::std::string::String::from(#key),
                        #data_crate::model::Value::Bool(true),
                    );
                });
            }
            if let Some(min) = &spec.min_value {
                let key = LitStr::new(
                    &format!("inputs.{}.min", spec.name.value()),
                    Span::call_site(),
                );
                let val = match min {
                    Lit::Int(i) => {
                        let v: i64 = i.base10_parse().unwrap_or(0);
                        quote! { #data_crate::model::Value::Int(#v) }
                    }
                    Lit::Float(f) => {
                        let v: f64 = f.base10_parse().unwrap_or(0.0);
                        quote! { #data_crate::model::Value::Float(#v) }
                    }
                    _ => quote! { #data_crate::model::Value::Int(0) },
                };
                entries.push(quote! {
                    __meta.insert(
                        ::std::string::String::from(#key),
                        #val,
                    );
                });
            }
            if let Some(max) = &spec.max_value {
                let key = LitStr::new(
                    &format!("inputs.{}.max", spec.name.value()),
                    Span::call_site(),
                );
                let val = match max {
                    Lit::Int(i) => {
                        let v: i64 = i.base10_parse().unwrap_or(0);
                        quote! { #data_crate::model::Value::Int(#v) }
                    }
                    Lit::Float(f) => {
                        let v: f64 = f.base10_parse().unwrap_or(0.0);
                        quote! { #data_crate::model::Value::Float(#v) }
                    }
                    _ => quote! { #data_crate::model::Value::Int(0) },
                };
                entries.push(quote! {
                    __meta.insert(
                        ::std::string::String::from(#key),
                        #val,
                    );
                });
            }
            if !spec.meta.is_empty() {
                for (meta_key, meta_value) in &spec.meta {
                    let key = LitStr::new(
                        &format!("inputs.{}.{}", spec.name.value(), meta_key.value()),
                        Span::call_site(),
                    );
                    let value = match meta_value {
                        Lit::Str(s) => {
                            quote! { #data_crate::model::Value::String(::std::borrow::Cow::from(#s)) }
                        }
                        Lit::Int(i) => {
                            let v: i64 = i.base10_parse().unwrap_or(0);
                            quote! { #data_crate::model::Value::Int(#v) }
                        }
                        Lit::Float(f) => {
                            let v: f64 = f.base10_parse().unwrap_or(0.0);
                            quote! { #data_crate::model::Value::Float(#v) }
                        }
                        Lit::Bool(b) => {
                            let v = b.value;
                            quote! { #data_crate::model::Value::Bool(#v) }
                        }
                        _ => quote! { #data_crate::model::Value::Unit },
                    };
                    entries.push(quote! {
                        __meta.insert(
                            ::std::string::String::from(#key),
                            #value,
                        );
                    });
                }
            }
            entries
        })
        .collect();

    let from_io_fields: Vec<proc_macro2::TokenStream> = specs
        .iter()
        .map(|spec| {
            let ident = &spec.field_ident;
            let ty = &spec.field_ty;
            let name = &spec.name;
            quote! {
                let #ident = io
                    .get_typed::<#ty>(#name)
                    .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #name)))?;
            }
        })
        .collect();

    let sanitize_fields: Vec<proc_macro2::TokenStream> = specs
        .iter()
        .enumerate()
        .map(|(idx, spec)| {
            let ident = &spec.field_ident;
            let needs_sanitize = spec.min_value.is_some() || spec.max_value.is_some() || spec.odd;
            if !needs_sanitize {
                return quote! {
                    let #ident = self.#ident;
                };
            }
            let name = &spec.name;
            let policy_str = spec
                .policy
                .clone()
                .unwrap_or_else(|| LitStr::new("clamp", Span::call_site()));
            let policy_variant = match policy_str.value().as_str() {
                "error" => quote! { #runtime_crate::config::ConfigPolicy::Error },
                _ => quote! { #runtime_crate::config::ConfigPolicy::Clamp },
            };
            let number_kind = number_kind(&spec.field_ty);
            let to_value = match number_kind {
                Some(NumberKind::Float) => {
                    quote! { #data_crate::model::Value::Float(value as f64) }
                }
                _ => quote! { #data_crate::model::Value::Int(value as i64) },
            };
            let min_check = spec.min_value.as_ref().map(|min| {
                quote! {
                    if value < #min {
                        if matches!(#policy_variant, #runtime_crate::config::ConfigPolicy::Error) {
                            return Err(#runtime_crate::config::ConfigError::for_port(
                                #name,
                                format!("must be >= {}", #min),
                            ));
                        }
                        value = #min;
                        changed = true;
                    }
                }
            });
            let max_check = spec.max_value.as_ref().map(|max| {
                quote! {
                    if value > #max {
                        if matches!(#policy_variant, #runtime_crate::config::ConfigPolicy::Error) {
                            return Err(#runtime_crate::config::ConfigError::for_port(
                                #name,
                                format!("must be <= {}", #max),
                            ));
                        }
                        value = #max;
                        changed = true;
                    }
                }
            });
            let odd_check = if spec.odd {
                let min_guard = spec.min_value.as_ref().map(|min| {
                    quote! {
                        if candidate < #min {
                            candidate = value + 1;
                        }
                    }
                });
                let max_guard = spec.max_value.as_ref().map(|max| {
                    quote! {
                        if candidate > #max {
                            candidate = value - 1;
                        }
                    }
                });
                Some(quote! {
                    if value % 2 == 0 {
                        if matches!(#policy_variant, #runtime_crate::config::ConfigPolicy::Error) {
                            return Err(#runtime_crate::config::ConfigError::for_port(
                                #name,
                                "must be odd",
                            ));
                        }
                        let mut candidate = value + 1;
                        #max_guard
                        #min_guard
                        if candidate == value {
                            return Err(#runtime_crate::config::ConfigError::for_port(
                                #name,
                                "unable to coerce even value to odd",
                            ));
                        }
                        value = candidate;
                        changed = true;
                    }
                })
            } else {
                None
            };
            if number_kind.is_none() {
                return quote! { compile_error!("numeric constraints require numeric types"); };
            }
            let change_ident = syn::Ident::new(&format!("__cfg_change_{idx}"), Span::call_site());
            quote! {
                let mut #ident = self.#ident;
                let original = #ident;
                let mut value = #ident;
                let mut changed = false;
                #min_check
                #max_check
                #odd_check
                if changed {
                    let #change_ident = #runtime_crate::config::ConfigChange {
                        port: #name,
                        previous: { let value = original; #to_value },
                        next: { let value = value; #to_value },
                        policy: #policy_variant,
                    };
                    changes.push(#change_ident);
                }
                #ident = value;
            }
        })
        .collect();

    let validate_call = validate_fn.as_ref().map(|path| {
        quote! {
            #path(self)?;
        }
    });

    let struct_fields: Vec<syn::Ident> =
        specs.iter().map(|spec| spec.field_ident.clone()).collect();

    TokenStream::from(quote! {
        impl #impl_generics #runtime_crate::config::NodeConfig for #struct_ident #ty_generics #where_clause {
            fn ports() -> Vec<#registry_crate::store::Port> {
                vec![#(#ports_tokens),*]
            }

            fn metadata() -> ::std::collections::BTreeMap<String, #data_crate::model::Value> {
                let mut __meta: ::std::collections::BTreeMap<String, #data_crate::model::Value> =
                    ::std::collections::BTreeMap::new();
                #(#metadata_tokens)*
                __meta
            }

            fn from_io(io: &#runtime_crate::NodeIo) -> Result<Self, #runtime_crate::NodeError> {
                #(#from_io_fields)*
                Ok(Self { #(#struct_fields),* })
            }

            fn sanitize(self) -> Result<#runtime_crate::config::Sanitized<Self>, #runtime_crate::config::ConfigError> {
                let mut changes = Vec::new();
                #(#sanitize_fields)*
                Ok(#runtime_crate::config::Sanitized {
                    value: Self { #(#struct_fields),* },
                    changes,
                })
            }

            fn validate(&self) -> Result<(), #runtime_crate::config::ConfigError> {
                #validate_call
                Ok(())
            }
        }
    })
}

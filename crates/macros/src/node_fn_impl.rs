use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::Span;
use quote::{ToTokens, quote};
use syn::parse::Parser;
use syn::parse::discouraged::Speculative;
use syn::{ItemFn, Lit, LitStr, Member, Meta, MetaNameValue, parse_macro_input};

use crate::helpers::{AttributeArgs, NestedMeta, compile_error, lit_from_expr, parse_nested};

fn is_rust_keyword(ident: &str) -> bool {
    matches!(
        ident,
        "as" | "break"
            | "const"
            | "continue"
            | "crate"
            | "else"
            | "enum"
            | "extern"
            | "false"
            | "fn"
            | "for"
            | "if"
            | "impl"
            | "in"
            | "let"
            | "loop"
            | "match"
            | "mod"
            | "move"
            | "mut"
            | "pub"
            | "ref"
            | "return"
            | "self"
            | "Self"
            | "static"
            | "struct"
            | "super"
            | "trait"
            | "true"
            | "type"
            | "unsafe"
            | "use"
            | "where"
            | "while"
            | "async"
            | "await"
            | "dyn"
            | "abstract"
            | "become"
            | "box"
            | "do"
            | "final"
            | "macro"
            | "override"
            | "priv"
            | "try"
            | "typeof"
            | "unsized"
            | "virtual"
            | "yield"
    )
}

fn port_ident(name: &str) -> syn::Ident {
    if is_rust_keyword(name) {
        syn::Ident::new_raw(name, Span::call_site())
    } else {
        syn::Ident::new(name, Span::call_site())
    }
}

struct BindingShorthand {
    _name: syn::Ident,
    _as: syn::Token![as],
    kind: syn::Ident,
}

impl syn::parse::Parse for BindingShorthand {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Ok(Self {
            _name: input.parse()?,
            _as: input.parse()?,
            kind: input.parse()?,
        })
    }
}

enum BindingEntry {
    Shorthand(BindingShorthand),
    Other(proc_macro2::TokenStream),
}

impl syn::parse::Parse for BindingEntry {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let fork = input.fork();
        if let Ok(sh) = fork.parse::<BindingShorthand>() {
            input.advance_to(&fork);
            return Ok(BindingEntry::Shorthand(sh));
        }
        let expr: syn::Expr = input.parse()?;
        Ok(BindingEntry::Other(expr.to_token_stream()))
    }
}

pub fn node(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args with AttributeArgs::parse_terminated);
    let mut input = parse_macro_input!(item as ItemFn);

    let mut id: Option<LitStr> = None;
    let mut _bundle: Option<LitStr> = None;
    let mut summary_attr: Option<LitStr> = None;
    let mut description_attr: Option<LitStr> = None;
    let mut generics_attr: Option<proc_macro2::TokenStream> = None;
    #[derive(Clone)]
    struct PortMeta {
        name: LitStr,
        source: Option<LitStr>,
        default_value: Option<proc_macro2::TokenStream>,
        ty_override: Option<proc_macro2::TokenStream>,
        description: Option<LitStr>,
        meta: Vec<(LitStr, Lit)>,
    }
    impl PortMeta {
        fn name_only(name: LitStr) -> Self {
            Self {
                name,
                source: None,
                default_value: None,
                ty_override: None,
                description: None,
                meta: Vec::new(),
            }
        }
    }
    let mut inputs: Vec<PortMeta> = Vec::new();
    let mut config_types: Vec<syn::Type> = Vec::new();
    #[derive(Clone)]
    struct OutputPortMeta {
        name: LitStr,
        source: Option<LitStr>,
        ty_override: Option<proc_macro2::TokenStream>,
        description: Option<LitStr>,
        meta: Vec<(LitStr, Lit)>,
    }
    impl OutputPortMeta {
        fn name_only(name: LitStr) -> Self {
            Self {
                name,
                source: None,
                ty_override: None,
                description: None,
                meta: Vec::new(),
            }
        }
    }
    let mut outputs: Vec<OutputPortMeta> = Vec::new();
    let mut shader_path: Option<LitStr> = None;
    let mut shader_paths: Vec<LitStr> = Vec::new();
    let mut shader_entry: LitStr = LitStr::new("main", Span::call_site());
    let mut shader_entry_explicit = false;
    let mut shader_workgroup: Option<[u32; 3]> = None;
    let mut shader_workgroup_explicit = false;
    let mut shader_bindings: Vec<proc_macro2::TokenStream> = Vec::new();
    let mut shader_specs: Vec<(proc_macro2::TokenStream, Option<LitStr>)> = Vec::new();
    let mut state_ty_attr: Option<syn::Type> = None;
    let mut compute_attr: Option<proc_macro2::TokenStream> = None;
    let mut sync_groups_attr: Option<proc_macro2::TokenStream> = None;
    let mut capability_attr: Option<LitStr> = None;

    // Prefer using the public `daedalus` crate if present; otherwise fall back to
    // the internal sub-crates. This keeps external plugin crates depending only
    // on `daedalus` while still working inside the workspace.
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
    let core_crate = crate_path("daedalus-core", "daedalus_core", Some("core"));
    let gpu_crate = crate_path("daedalus", "daedalus", Some("gpu"));

    for arg in args {
        match arg {
            NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. })) => {
                let Some(value) = lit_from_expr(&value) else {
                    return TokenStream::from(compile_error(
                        "name/value arguments must be literal values".into(),
                    ));
                };
                if path.is_ident("id") {
                    match value {
                        Lit::Str(s) => id = Some(s),
                        _ => {
                            return TokenStream::from(compile_error(
                                "id must be a string literal".into(),
                            ));
                        }
                    }
                } else if path.is_ident("summary") {
                    match value {
                        Lit::Str(s) => summary_attr = Some(s),
                        _ => {
                            return TokenStream::from(compile_error(
                                "summary must be a string literal".into(),
                            ));
                        }
                    }
                } else if path.is_ident("description") {
                    match value {
                        Lit::Str(s) => description_attr = Some(s),
                        _ => {
                            return TokenStream::from(compile_error(
                                "description must be a string literal".into(),
                            ));
                        }
                    }
                } else if path.is_ident("bundle") {
                    match value {
                        Lit::Str(s) => _bundle = Some(s),
                        _ => {
                            return TokenStream::from(compile_error(
                                "bundle must be a string literal".into(),
                            ));
                        }
                    }
                } else if path.is_ident("inputs") {
                    match value {
                        Lit::Str(s) => inputs.push(PortMeta::name_only(s)),
                        _ => {
                            return TokenStream::from(compile_error(
                                "inputs must be string literals".into(),
                            ));
                        }
                    }
                } else if path.is_ident("outputs") {
                    match value {
                        Lit::Str(s) => outputs.push(OutputPortMeta::name_only(s)),
                        _ => {
                            return TokenStream::from(compile_error(
                                "outputs must be string literals".into(),
                            ));
                        }
                    }
                } else if path.is_ident("capability") {
                    match value {
                        Lit::Str(s) => capability_attr = Some(s),
                        _ => {
                            return TokenStream::from(compile_error(
                                "capability must be a string literal".into(),
                            ));
                        }
                    }
                } else if path.is_ident("shader") {
                    match value {
                        Lit::Str(s) => shader_path = Some(s),
                        _ => {
                            return TokenStream::from(compile_error(
                                "shader must be a string literal".into(),
                            ));
                        }
                    }
                } else if path.is_ident("entry") {
                    match value {
                        Lit::Str(s) => {
                            shader_entry = s;
                            shader_entry_explicit = true;
                        }
                        _ => {
                            return TokenStream::from(compile_error(
                                "entry must be a string literal".into(),
                            ));
                        }
                    }
                } else if path.is_ident("workgroup_size") {
                    match value {
                        Lit::Int(i) => {
                            shader_workgroup = i.base10_parse::<u32>().ok().map(|v| [v, 1, 1]);
                            shader_workgroup_explicit = true;
                        }
                        _ => {
                            return TokenStream::from(compile_error(
                                "workgroup_size must be an integer literal".into(),
                            ));
                        }
                    }
                } else if path.is_ident("sync_groups") {
                    sync_groups_attr = Some(value.to_token_stream());
                } else if path.is_ident("compute") {
                    compute_attr = Some(value.to_token_stream());
                } else {
                    return TokenStream::from(compile_error(format!(
                        "unsupported name/value argument: {:?}",
                        path.to_token_stream()
                    )));
                }
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("sync_groups") => {
                sync_groups_attr = Some(list.tokens.clone());
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("inputs") => {
                let Ok(nested_items) = parse_nested(&list) else {
                    return TokenStream::from(compile_error(
                        "inputs(...) must use a comma-separated list".into(),
                    ));
                };
                for nested in nested_items {
                    // Support either bare names or port(name=..., source=...)
                    if let NestedMeta::Lit(Lit::Str(s)) = nested {
                        inputs.push(PortMeta::name_only(s));
                        continue;
                    }
                    if let NestedMeta::Meta(Meta::NameValue(nv)) = &nested {
                        if nv.path.is_ident("config") {
                            match syn::parse2::<syn::Type>(nv.value.to_token_stream()) {
                                Ok(ty) => {
                                    config_types.push(ty);
                                    continue;
                                }
                                Err(_) => {
                                    return TokenStream::from(compile_error(
                                        "config must be a type path, e.g. config = MyConfig".into(),
                                    ));
                                }
                            }
                        }
                    }
                    if let NestedMeta::Meta(Meta::List(inner)) = nested
                        && inner.path.is_ident("port")
                    {
                        let Ok(inner_items) = parse_nested(&inner) else {
                            return TokenStream::from(compile_error(
                                "port(...) expects comma-separated arguments".into(),
                            ));
                        };
                        let mut name: Option<LitStr> = None;
                        let mut source: Option<LitStr> = None;
                        let mut default_value: Option<proc_macro2::TokenStream> = None;
                        let mut ty_override: Option<proc_macro2::TokenStream> = None;
                        let mut description: Option<LitStr> = None;
                        let mut meta_entries: Vec<(LitStr, Lit)> = Vec::new();
                        for nm in inner_items {
                            let NestedMeta::Meta(Meta::NameValue(nv)) = nm else {
                                if let NestedMeta::Meta(Meta::List(list)) = nm {
                                    if list.path.is_ident("meta") || list.path.is_ident("metadata") {
                        let Ok(meta_items) = parse_nested(&list) else {
                                            return TokenStream::from(compile_error(
                                                "meta(...) expects comma-separated arguments".into(),
                                            ));
                                        };
                                        for meta in meta_items {
                                            let NestedMeta::Meta(Meta::NameValue(nv)) = meta else {
                                                return TokenStream::from(compile_error(
                                                    "meta(...) entries must be name/value pairs".into(),
                                                ));
                                            };
                                            let Some(key_ident) = nv.path.get_ident() else {
                                                return TokenStream::from(compile_error(
                                                    "meta keys must be simple identifiers".into(),
                                                ));
                                            };
                                            let Some(value) = lit_from_expr(&nv.value) else {
                                                return TokenStream::from(compile_error(
                                                    "meta values must be literal values".into(),
                                                ));
                                            };
                                            meta_entries.push((
                                                LitStr::new(&key_ident.to_string(), Span::call_site()),
                                                value,
                                            ));
                                        }
                                        continue;
                                    }
                                }
                                continue;
                            };
                            if nv.path.is_ident("name") {
                                if let Some(Lit::Str(s)) = lit_from_expr(&nv.value) {
                                    name = Some(s);
                                    continue;
                                }
                            }
                            if nv.path.is_ident("source") {
                                if let Some(Lit::Str(s)) = lit_from_expr(&nv.value) {
                                    source = Some(s);
                                    continue;
                                }
                            }
                            if nv.path.is_ident("ty") {
                                ty_override = Some(nv.value.to_token_stream());
                                continue;
                            }
                            if nv.path.is_ident("description") {
                                if let Some(Lit::Str(s)) = lit_from_expr(&nv.value) {
                                    description = Some(s);
                                    continue;
                                }
                                return TokenStream::from(compile_error(
                                    "port description must be a string literal".into(),
                                ));
                            }
                            if nv.path.is_ident("default") {
                                let ts = match lit_from_expr(&nv.value) {
                                    Some(Lit::Str(s)) => {
                                        quote! { #data_crate::model::Value::String(::std::borrow::Cow::from(#s)) }
                                    }
                                    Some(Lit::Int(i)) => {
                                        let v: i64 = i.base10_parse().unwrap_or(0);
                                        quote! { #data_crate::model::Value::Int(#v) }
                                    }
                                    Some(Lit::Float(f)) => {
                                        let v: f64 = f.base10_parse().unwrap_or(0.0);
                                        quote! { #data_crate::model::Value::Float(#v) }
                                    }
                                    Some(Lit::Bool(b)) => {
                                        let v = b.value;
                                        quote! { #data_crate::model::Value::Bool(#v) }
                                    }
                                    _ => {
                                        return TokenStream::from(compile_error(
                                            "default must be string/int/float/bool literal".into(),
                                        ));
                                    }
                                };
                                default_value = Some(ts);
                                continue;
                            }
                        }
                        let name = match name {
                            Some(n) => n,
                            None => {
                                return TokenStream::from(compile_error(
                                    "port(...) inside inputs requires name = \"...\"".into(),
                                ));
                            }
                        };
                        inputs.push(PortMeta {
                            name,
                            source,
                            default_value,
                            ty_override,
                            description,
                            meta: meta_entries,
                        });
                        continue;
                    }
                    return TokenStream::from(compile_error(
                        "inputs list supports \"name\", config = Type, or port(name = \"...\", source = \"...\")"
                            .into(),
                    ));
                }
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("outputs") => {
                let Ok(nested_items) = parse_nested(&list) else {
                    return TokenStream::from(compile_error(
                        "outputs(...) must use a comma-separated list".into(),
                    ));
                };
                for nested in nested_items {
                    if let NestedMeta::Lit(Lit::Str(s)) = nested {
                        outputs.push(OutputPortMeta::name_only(s));
                        continue;
                    }
                    if let NestedMeta::Meta(Meta::List(inner)) = nested {
                        if inner.path.is_ident("port") {
                            let Ok(inner_items) = parse_nested(&inner) else {
                                return TokenStream::from(compile_error(
                                    "port(...) expects comma-separated arguments".into(),
                                ));
                            };
                            let mut name: Option<LitStr> = None;
                            let mut source: Option<LitStr> = None;
                            let mut ty_override: Option<proc_macro2::TokenStream> = None;
                            let mut description: Option<LitStr> = None;
                            let mut meta_entries: Vec<(LitStr, Lit)> = Vec::new();
                            for nm in inner_items {
                                if let NestedMeta::Meta(Meta::NameValue(nv)) = &nm {
                                    if nv.path.is_ident("name") {
                                        if let Some(Lit::Str(s)) = lit_from_expr(&nv.value) {
                                            name = Some(s);
                                            continue;
                                        }
                                    }
                                    if nv.path.is_ident("source") {
                                        if let Some(Lit::Str(s)) = lit_from_expr(&nv.value) {
                                            source = Some(s);
                                            continue;
                                        }
                                    }
                                    if nv.path.is_ident("ty") {
                                        ty_override = Some(nv.value.to_token_stream());
                                        continue;
                                    }
                                    if nv.path.is_ident("description") {
                                        if let Some(Lit::Str(s)) = lit_from_expr(&nv.value) {
                                            description = Some(s);
                                            continue;
                                        }
                                        return TokenStream::from(compile_error(
                                            "port description must be a string literal".into(),
                                        ));
                                    }
                                }
                                if let NestedMeta::Meta(Meta::List(list)) = &nm {
                                    if list.path.is_ident("meta") || list.path.is_ident("metadata") {
                                        let Ok(meta_items) = parse_nested(list) else {
                                            return TokenStream::from(compile_error(
                                                "meta(...) expects comma-separated arguments".into(),
                                            ));
                                        };
                                        for meta in meta_items {
                                            let NestedMeta::Meta(Meta::NameValue(nv)) = meta else {
                                                return TokenStream::from(compile_error(
                                                    "meta(...) entries must be name/value pairs".into(),
                                                ));
                                            };
                                            let Some(key_ident) = nv.path.get_ident() else {
                                                return TokenStream::from(compile_error(
                                                    "meta keys must be simple identifiers".into(),
                                                ));
                                            };
                                            let Some(value) = lit_from_expr(&nv.value) else {
                                                return TokenStream::from(compile_error(
                                                    "meta values must be literal values".into(),
                                                ));
                                            };
                                            meta_entries.push((
                                                LitStr::new(&key_ident.to_string(), Span::call_site()),
                                                value,
                                            ));
                                        }
                                    }
                                }
                            }
                            let name = match name {
                                Some(n) => n,
                                None => {
                                    return TokenStream::from(compile_error(
                                        "port(...) inside outputs requires name = \"...\"".into(),
                                    ));
                                }
                            };
                            outputs.push(OutputPortMeta {
                                name,
                                source,
                                ty_override,
                                description,
                                meta: meta_entries,
                            });
                            continue;
                        }
                    }
                    return TokenStream::from(compile_error(
                        "outputs list supports \"name\" or port(name = \"...\", source = \"...\")"
                            .into(),
                    ));
                }
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("compute") => {
                let Ok(nested) = parse_nested(&list) else {
                    return TokenStream::from(compile_error(
                        "compute(...) expects a single argument".into(),
                    ));
                };
                if let Some(first) = nested.first() {
                    compute_attr = Some(first.to_token_stream());
                } else {
                    return TokenStream::from(compile_error(
                        "compute(...) expects an affinity, e.g., compute(ComputeAffinity::GpuPreferred)"
                            .into(),
                    ));
                }
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("generics") => {
                generics_attr = Some(list.tokens.clone());
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("state") => {
                let Ok(nested) = parse_nested(&list) else {
                    return TokenStream::from(compile_error(
                        "state(...) expects a type, e.g., state(MyState)".into(),
                    ));
                };
                if let Some(first) = nested.first() {
                    match syn::parse2::<syn::Type>(first.to_token_stream()) {
                        Ok(ty) => state_ty_attr = Some(ty),
                        Err(_) => {
                            return TokenStream::from(compile_error(
                                "state(...) expects a type, e.g., state(MyState)".into(),
                            ));
                        }
                    }
                } else {
                    return TokenStream::from(compile_error(
                        "state(...) expects a type, e.g., state(MyState)".into(),
                    ));
                }
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("bindings") => {
                let parser =
                    syn::punctuated::Punctuated::<BindingEntry, syn::Token![,]>::parse_terminated;
                let parsed = match parser.parse2(list.tokens.clone()) {
                    Ok(p) => p,
                    Err(_) => {
                        return TokenStream::from(compile_error(
                            "bindings(...) expects a comma-separated list".into(),
                        ));
                    }
                };
                for (idx, entry) in parsed.into_iter().enumerate() {
                    match entry {
                        BindingEntry::Other(ts) => shader_bindings.push(ts),
                        BindingEntry::Shorthand(sh) => {
                            let kind_str = sh.kind.to_string().to_lowercase();
                            let binding_idx = idx as u32;
                            let spec_tokens = match kind_str.as_str() {
                                "storage" | "storage_read" => quote! {
                                    #gpu_crate::shader::BindingSpec::storage_read(#binding_idx, None)
                                },
                                "storage_write" => quote! {
                                    #gpu_crate::shader::BindingSpec::storage_write(#binding_idx, None)
                                },
                                "storage_rw" | "storage_readwrite" => quote! {
                                    #gpu_crate::shader::BindingSpec::storage_read_write(#binding_idx, None)
                                },
                                "uniform" => quote! {
                                    #gpu_crate::shader::BindingSpec::uniform(#binding_idx)
                                },
                                other => {
                                    return TokenStream::from(compile_error(format!(
                                        "unsupported binding kind `{}` (expected storage, storage_rw, storage_write, or uniform)",
                                        other
                                    )));
                                }
                            };
                            shader_bindings.push(quote! {
                                #spec_tokens
                            });
                        }
                    }
                }
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("shaders") => {
                let Ok(nested) = parse_nested(&list) else {
                    return TokenStream::from(compile_error(
                        "shaders(...) expects a comma-separated list".into(),
                    ));
                };
                for nested in nested {
                    // Accept either plain string literals (paths) or full ShaderSpec structs.
                    match nested {
                        NestedMeta::Lit(Lit::Str(s)) => shader_paths.push(s),
                        NestedMeta::Meta(meta) => {
                            if let Meta::Path(p) = &meta {
                                // Allow shorthand: shaders(MyBindings) -> MyBindings::SPEC
                                let ident_str = p
                                    .segments
                                    .last()
                                    .map(|s| s.ident.to_string())
                                    .unwrap_or_else(|| "shader".into());
                                let lit = LitStr::new(
                                    &ident_str,
                                    p.segments
                                        .last()
                                        .map(|s| s.ident.span())
                                        .unwrap_or_else(Span::call_site),
                                );
                                shader_specs.push((quote! { #p :: SPEC }, Some(lit)));
                            } else {
                                let ts = meta.to_token_stream();
                                // Try to extract asset if present.
                                let asset =
                                    if let Ok(expr) = syn::parse2::<syn::ExprStruct>(ts.clone()) {
                                        expr.fields.iter().find_map(|f| {
                                            if let Member::Named(ident) = &f.member {
                                                if ident == "asset" || ident == "name" {
                                                    if let syn::Expr::Lit(syn::ExprLit {
                                                        lit: Lit::Str(ls),
                                                        ..
                                                    }) = &f.expr
                                                    {
                                                        return Some(ls.clone());
                                                    }
                                                }
                                            }
                                            None
                                        })
                                    } else {
                                        None
                                    };
                                shader_specs.push((meta.to_token_stream(), asset));
                            }
                        }
                        _ => {}
                    }
                }
            }
            other => {
                return TokenStream::from(compile_error(format!(
                    "unsupported argument: {:?}",
                    other.to_token_stream()
                )));
            }
        }
    }

    // Disallow mixing top-level entry/workgroup_size with shader path lists;
    // GPU metadata must live inside shaders(...) ShaderSpec entries.
    if (shader_entry_explicit || shader_workgroup_explicit)
        && (shader_path.is_some() || !shader_paths.is_empty() || !shader_specs.is_empty())
    {
        return TokenStream::from(compile_error(
            "entry/workgroup_size must be specified inside shaders(...) via ShaderSpec { entry: \"...\", workgroup_size: ... }".to_string(),
        ));
    }

    let id = match id {
        Some(v) => v,
        None => return TokenStream::from(compile_error("missing required argument `id`".into())),
    };
    let fn_ident = input.sig.ident.clone();
    let struct_ident = fn_ident.clone();
    let handle_ident = syn::Ident::new(&format!("{}Handle", struct_ident), Span::call_site());
    let inputs_ident = syn::Ident::new(&format!("{}Inputs", struct_ident), Span::call_site());
    let outputs_ident = syn::Ident::new(&format!("{}Outputs", struct_ident), Span::call_site());
    let inner_fn_ident = syn::Ident::new(&format!("{}_impl", fn_ident), Span::call_site());

    // Move the user function body into an inner helper to keep the external API clean.
    input.sig.ident = inner_fn_ident.clone();
    let sig_for_ports = input.sig.clone();

    // Detect low-level vs typed signature (same heuristic as node_handler).
    // Low-level is only the legacy (node, ctx, io) triad; otherwise we treat as typed
    // and allow RuntimeNode/ExecutionContext/NodeIo to appear anywhere.
    let is_low_level = {
        let inputs_sig = &input.sig.inputs;
        inputs_sig.len() == 3
            && inputs_sig.iter().all(|arg| {
                if let syn::FnArg::Typed(pat) = arg {
                    matches!(&*pat.ty, syn::Type::Reference(_))
                } else {
                    false
                }
            })
    };

    let has_shaders = shader_path.is_some() || !shader_specs.is_empty() || !shader_paths.is_empty();
    let compute_expr: proc_macro2::TokenStream = if let Some(ts) = compute_attr.clone() {
        quote! { #ts }
    } else if has_shaders {
        quote! { #core_crate::compute::ComputeAffinity::GpuRequired }
    } else {
        quote! { #core_crate::compute::ComputeAffinity::CpuOnly }
    };
    // Common descriptor payload.
    let inputs_vec = inputs.clone();
    let outputs_vec = outputs.clone();
    let input_names: Vec<LitStr> = inputs_vec.iter().map(|p| p.name.clone()).collect();
    let output_names: Vec<LitStr> = outputs_vec.iter().map(|p| p.name.clone()).collect();
    let input_idents: Vec<syn::Ident> = inputs_vec
        .iter()
        .map(|p| port_ident(&p.name.value()))
        .collect();
    let output_sources: Vec<proc_macro2::TokenStream> = outputs_vec
        .iter()
        .map(|p| {
            if let Some(s) = &p.source {
                quote! { Some(#s.into()) }
            } else {
                quote! { None }
            }
        })
        .collect();
    let output_idents: Vec<syn::Ident> = outputs_vec
        .iter()
        .map(|p| port_ident(&p.name.value()))
        .collect();

    // Captured so we can reuse when building descriptors.
    let mut arg_types: Vec<syn::Type> = Vec::new();
    let mut arg_idents: Vec<syn::Ident> = Vec::new();
    let mut arg_names: Vec<LitStr> = Vec::new();
    let mut arg_mut_bindings: Vec<bool> = Vec::new();
    let mut effective_inputs_for_args: Vec<PortMeta> = Vec::new();

    // Typed handler generation
    struct GraphCtxArg {
        ident: syn::Ident,
        is_mut_ref: bool,
    }
    let mut graph_ctx_arg: Option<GraphCtxArg> = None;
    let mut runtime_node_present = false;
    let mut exec_ctx_present = false;
    let mut node_io_present = false;
    let mut shader_ctx_present = false;

    let handler_body = if is_low_level {
        quote! { #inner_fn_ident(node, ctx, io) }
    } else {
        struct ConfigArg {
            ident: syn::Ident,
            ty: syn::Type,
            is_ref: bool,
            is_mut: bool,
        }
        let mut config_args: Vec<ConfigArg> = Vec::new();
        let config_type_keys: Vec<String> = config_types
            .iter()
            .map(|ty| {
                let mut raw = quote! { #ty }.to_string();
                raw.retain(|c| !c.is_whitespace());
                raw
            })
            .collect();
        let has_shader_metadata =
            shader_path.is_some() || !shader_specs.is_empty() || !shader_paths.is_empty();
        let mut shader_ctx_ident: Option<syn::Ident> = None;
        let mut runtime_node_ident: Option<syn::Ident> = None;
        let mut exec_ctx_ident: Option<syn::Ident> = None;
        let mut node_io_ident: Option<syn::Ident> = None;
        let state_ty = state_ty_attr.clone();
        let mut state_param: Option<syn::Ident> = None;
        for arg in &input.sig.inputs {
            if let syn::FnArg::Typed(pat) = arg {
                if let syn::Pat::Ident(id) = &*pat.pat {
                    let last_ident = match &*pat.ty {
                        syn::Type::Path(tp) => tp.path.segments.last().map(|s| s.ident.to_string()),
                        syn::Type::Reference(r) => {
                            if let syn::Type::Path(tp) = &*r.elem {
                                tp.path.segments.last().map(|s| s.ident.to_string())
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };
                    match last_ident.as_deref() {
                        Some("GraphCtx") => {
                            let is_mut_ref = matches!(
                                &*pat.ty,
                                syn::Type::Reference(r) if r.mutability.is_some()
                            );
                            graph_ctx_arg = Some(GraphCtxArg {
                                ident: id.ident.clone(),
                                is_mut_ref,
                            });
                            continue;
                        }
                        Some("ShaderContext") => {
                            // Many node crates gate ShaderContext behind `#[cfg(feature = "gpu")]`.
                            // In non-GPU builds, the cfg removes the parameter after macro
                            // expansion; avoid treating it as required unless shader metadata is
                            // present on this node.
                            let cfg_gated = pat.attrs.iter().any(|a| a.path().is_ident("cfg"));
                            if has_shader_metadata || !cfg_gated {
                                shader_ctx_present = true;
                                shader_ctx_ident = Some(id.ident.clone());
                            }
                            continue;
                        }
                        Some("RuntimeNode") => {
                            runtime_node_present = true;
                            runtime_node_ident = Some(id.ident.clone());
                            continue;
                        }
                        Some("ExecutionContext") => {
                            exec_ctx_present = true;
                            exec_ctx_ident = Some(id.ident.clone());
                            continue;
                        }
                        Some("NodeIo") => {
                            node_io_present = true;
                            node_io_ident = Some(id.ident.clone());
                            continue;
                        }
                        _ => {}
                    }
                    if matches!(
                        last_ident.as_deref(),
                        Some("NodeIo" | "RuntimeNode" | "ExecutionContext" | "ShaderContext" | "GraphCtx")
                    ) {
                        continue;
                    }
                    // State parameter detection: match type or &/&mut of type.
                    let is_state = if let Some(sty) = &state_ty {
                        let ty_str = quote! { #sty }.to_string();
                        let match_ty = match &*pat.ty {
                            syn::Type::Path(tp2) => quote! { #tp2 }.to_string() == ty_str,
                            syn::Type::Reference(r) => quote! { #r.elem }.to_string() == ty_str,
                            _ => false,
                        };
                        match_ty || id.ident == "state"
                    } else {
                        false
                    };
                    if is_state {
                        state_param = Some(id.ident.clone());
                        continue;
                    }
                    if !config_type_keys.is_empty() {
                        let mut matched_config = None;
                        let mut is_ref = false;
                        let mut is_mut = false;
                        match &*pat.ty {
                            syn::Type::Path(tp) => {
                                let mut raw = quote! { #tp }.to_string();
                                raw.retain(|c| !c.is_whitespace());
                                if config_type_keys.iter().any(|k| k == &raw) {
                                    matched_config = Some((*pat.ty).clone());
                                }
                            }
                            syn::Type::Reference(r) => {
                                if let syn::Type::Path(tp) = &*r.elem {
                                    let mut raw = quote! { #tp }.to_string();
                                    raw.retain(|c| !c.is_whitespace());
                                    if config_type_keys.iter().any(|k| k == &raw) {
                                        matched_config = Some((*r.elem).clone());
                                        is_ref = true;
                                        is_mut = r.mutability.is_some();
                                    }
                                }
                            }
                            _ => {}
                        }
                        if let Some(cfg_ty) = matched_config {
                            config_args.push(ConfigArg {
                                ident: id.ident.clone(),
                                ty: cfg_ty,
                                is_ref,
                                is_mut,
                            });
                            continue;
                        }
                    }
                    arg_idents.push(id.ident.clone());
                    arg_names.push(LitStr::new(&id.ident.to_string(), Span::call_site()));
                    arg_types.push((*pat.ty).clone());
                    arg_mut_bindings.push(id.mutability.is_some());
                }
            }
        }
        if state_ty.is_some() && state_param.is_none() {
            return TokenStream::from(compile_error(
                "state(...) specified but no matching state parameter found in signature".into(),
            ));
        }

        if shader_ctx_ident.is_some() && !has_shader_metadata {
            return TokenStream::from(compile_error(
                "ShaderContext parameter requires shader metadata (missing shaders(...))".into(),
            ));
        }

        let is_fanin_ty = |ty: &syn::Type| -> bool {
            let ty = if let syn::Type::Reference(r) = ty {
                &*r.elem
            } else {
                ty
            };
            if let syn::Type::Path(tp) = ty
                && tp.qself.is_none()
                && let Some(seg) = tp.path.segments.last()
                && seg.ident == "FanIn"
            {
                return true;
            }
            false
        };

        // Determine the effective port metadata for each typed argument.
        //
        // Rules:
        // - If `inputs(...)` matches all typed args, use it (including FanIn prefixes).
        // - If the node has any `FanIn` params and `inputs(...)` is provided, it must match all
        //   typed args (to avoid confusing mixed naming).
        // - Otherwise, ignore `inputs(...)` for port naming and use parameter names.
        let fanin_mask: Vec<bool> = arg_types.iter().map(is_fanin_ty).collect();
        let has_fanin = fanin_mask.iter().any(|b| *b);
        effective_inputs_for_args = if inputs_vec.is_empty() {
            arg_names.iter().cloned().map(PortMeta::name_only).collect()
        } else if inputs_vec.len() == arg_types.len() {
            inputs_vec.clone()
        } else if has_fanin {
            return TokenStream::from(compile_error(
                "FanIn params require inputs(...) entries for all typed args (include the FanIn prefix).".into(),
            ));
        } else {
            arg_names.iter().cloned().map(PortMeta::name_only).collect()
        };

        let mut call_args: Vec<proc_macro2::TokenStream> = Vec::new();
        for arg in &input.sig.inputs {
            if let syn::FnArg::Typed(pat) = arg {
                let is_shader_ctx_param = match &*pat.ty {
                    syn::Type::Path(tp) => tp.path.segments.last().map(|s| s.ident == "ShaderContext").unwrap_or(false),
                    syn::Type::Reference(r) => match &*r.elem {
                        syn::Type::Path(tp) => tp.path.segments.last().map(|s| s.ident == "ShaderContext").unwrap_or(false),
                        _ => false,
                    },
                    _ => false,
                };
                if is_shader_ctx_param && shader_ctx_ident.is_none() {
                    continue;
                }
                if let syn::Pat::Ident(id) = &*pat.pat {
                    let ident = &id.ident;
                    if let Some(n) = &runtime_node_ident {
                        if ident == n {
                            call_args.push(quote! { node });
                            continue;
                        }
                    }
                    if let Some(c) = &exec_ctx_ident {
                        if ident == c {
                            call_args.push(quote! { ctx });
                            continue;
                        }
                    }
                    if let Some(ioid) = &node_io_ident {
                        if ident == ioid {
                            call_args.push(quote! { io });
                            continue;
                        }
                    }
                    if let Some(ctx) = &shader_ctx_ident {
                        if ident == ctx {
                            call_args.push(quote! { __shader_ctx });
                            continue;
                        }
                    }
                    if let Some(st) = &state_param {
                        if ident == st {
                            call_args.push(quote! { #ident });
                            continue;
                        }
                    }
                    call_args.push(quote! { #ident });
                }
            }
        }

        let shader_tokens = if !shader_specs.is_empty() {
            let specs_only: Vec<proc_macro2::TokenStream> =
                shader_specs.iter().map(|(ts, _)| ts.clone()).collect();
            let names: Vec<proc_macro2::TokenStream> = shader_specs
                .iter()
                .enumerate()
                .map(|(i, (_, asset))| {
                    if let Some(a) = asset {
                        quote! { #a }
                    } else {
                        let n = format!("shader{}", i);
                        let lit = LitStr::new(&n, Span::call_site());
                        quote! { #lit }
                    }
                })
                .collect();
            let count = specs_only.len();
            let indices: Vec<syn::Index> = (0..count).map(syn::Index::from).collect();
            Some(quote! {
                use #gpu_crate::shader::{ShaderContext, ShaderInstance, ShaderSpec};
                const __SHADER_SPECS: [ShaderSpec; #count] = [ #(#specs_only),* ];
                const __SHADER_INSTANCES: [ShaderInstance; #count] = [
                    #(ShaderInstance {
                        name: #names,
                        spec: &__SHADER_SPECS[#indices],
                    }),*
                ];
                let __shader_ctx = ShaderContext { shaders: &__SHADER_INSTANCES, gpu: __ctx_gpu.clone() };
            })
        } else if let Some(shader_literal) = shader_path.clone() {
            let entry = shader_entry.clone();
            let wg_size = shader_workgroup
                .map(|[x, y, z]| quote! { Some([#x, #y, #z]) })
                .unwrap_or(quote! { None });
            let binding_tokens = shader_bindings.clone();
            let binding_len = binding_tokens.len();
            let binding_array = quote! { [ #(#binding_tokens),* ] };
            Some(quote! {
                use #gpu_crate::shader::{BindingSpec, ShaderContext, ShaderInstance, ShaderSpec};
                const __SHADER_SRC: &str = include_str!(#shader_literal);
                const __SHADER_BINDINGS: [BindingSpec; #binding_len] = #binding_array;
                const __SHADER_SPEC: ShaderSpec = ShaderSpec {
                    name: "default",
                    src: __SHADER_SRC,
                    entry: #entry,
                    workgroup_size: #wg_size,
                    bindings: &__SHADER_BINDINGS,
                };
                const __SHADER_INSTANCES: [ShaderInstance; 1] = [ShaderInstance {
                    name: "default",
                    spec: &__SHADER_SPEC,
                }];
                let __shader_ctx = ShaderContext { shaders: &__SHADER_INSTANCES, gpu: __ctx_gpu.clone() };
            })
        } else if !shader_paths.is_empty() {
            let entry = shader_entry.clone();
            let wg_size = shader_workgroup
                .map(|[x, y, z]| quote! { Some([#x, #y, #z]) })
                .unwrap_or(quote! { None });
            let binding_tokens = shader_bindings.clone();
            let binding_len = binding_tokens.len();
            let binding_array = quote! { [ #(#binding_tokens),* ] };
            let shader_insts: Vec<proc_macro2::TokenStream> = shader_paths
                .iter()
                .map(|p| {
                    let name = std::path::Path::new(&p.value())
                        .file_stem()
                        .map(|s| s.to_string_lossy().to_string())
                        .unwrap_or_else(|| "shader".into());
                    let lit_name = LitStr::new(&name, p.span());
                    quote! {
                        #gpu_crate::shader::ShaderInstance {
                            name: #lit_name,
                            spec: &#gpu_crate::shader::ShaderSpec {
                                name: #lit_name,
                                src: include_str!(#p),
                                entry: #entry,
                                workgroup_size: #wg_size,
                                bindings: &__SHADER_BINDINGS,
                            },
                        }
                    }
                })
                .collect();
            let count = shader_insts.len();
            Some(quote! {
                use #gpu_crate::shader::{Access, BindingKind, BindingSpec, ShaderContext, ShaderInstance};
                const __SHADER_BINDINGS: [#gpu_crate::shader::BindingSpec; #binding_len] = #binding_array;
                const __SHADER_INSTANCES: [ShaderInstance; #count] = [ #(#shader_insts),* ];
                let __shader_ctx = ShaderContext { shaders: &__SHADER_INSTANCES, gpu: __ctx_gpu.clone() };
            })
        } else {
            None
        };

        let call = quote! { #inner_fn_ident(#(#call_args),*) };
        let out_port = output_names
            .first()
            .cloned()
            .unwrap_or_else(|| LitStr::new("out", Span::call_site()));

        let port_names: Vec<LitStr> = effective_inputs_for_args
            .iter()
            .map(|p| p.name.clone())
            .collect();

        fn ok_type_from_return(ret: &syn::ReturnType) -> Option<&syn::Type> {
            let ty = match ret {
                syn::ReturnType::Default => return None,
                syn::ReturnType::Type(_, ty) => ty.as_ref(),
            };

            match ty {
                syn::Type::Path(p) if p.qself.is_none() => {
                    let last = p.path.segments.last()?;
                    if last.ident != "Result" {
                        return Some(ty);
                    }
                    match &last.arguments {
                        syn::PathArguments::AngleBracketed(ab) => ab.args.first().and_then(|arg| {
                            if let syn::GenericArgument::Type(inner) = arg {
                                Some(inner)
                            } else {
                                None
                            }
                        }),
                        _ => None,
                    }
                }
                _ => Some(ty),
            }
        }

        fn payload_inner_type(ty: &syn::Type) -> Option<&syn::Type> {
            let tp = match ty {
                syn::Type::Path(tp) if tp.qself.is_none() => tp,
                _ => return None,
            };
            let last = tp.path.segments.last()?;
            if last.ident != "Payload" {
                return None;
            }
            match &last.arguments {
                syn::PathArguments::AngleBracketed(ab) => ab.args.first().and_then(|arg| {
                    if let syn::GenericArgument::Type(inner) = arg {
                        Some(inner)
                    } else {
                        None
                    }
                }),
                _ => None,
            }
        }

        fn is_unit_type(ty: &syn::Type) -> bool {
            matches!(ty, syn::Type::Tuple(t) if t.elems.is_empty())
        }

        let ret_handling = if node_io_ident.is_some() {
            let ok_ty = ok_type_from_return(&input.sig.output);
            if !outputs.is_empty() && ok_ty.is_some_and(|t| !is_unit_type(t)) {
                quote! {
                    compile_error!("nodes that take `NodeIo` must return `()` (push outputs via `io.push_*`) when outputs(...) are declared");
                    Ok(())
                }
            } else if matches!(input.sig.output, syn::ReturnType::Default) {
                quote! { #call; Ok(()) }
            } else {
                quote! {
                    match #call {
                        Ok(_) => Ok(()),
                        Err(e) => Err(e),
                    }
                }
            }
        } else if !matches!(input.sig.output, syn::ReturnType::Default) {
            let ok_ty = ok_type_from_return(&input.sig.output);
            if outputs.len() > 1 {
                let out_ports = output_names.clone();
                let out_idents = output_idents.clone();
                let out_push_stmts: Vec<proc_macro2::TokenStream> = match ok_ty {
                    Some(syn::Type::Tuple(tuple)) => tuple
                        .elems
                        .iter()
                        .zip(out_ports.iter())
                        .zip(out_idents.iter())
                        .map(|((elem_ty, port), ident)| {
                            if let Some(inner) = payload_inner_type(elem_ty) {
                                quote! { io.push_payload::<#inner>(Some(#port), #ident); }
                            } else {
                                quote! { io.push_typed(Some(#port), #ident); }
                            }
                        })
                        .collect(),
                    _ => out_ports
                        .iter()
                        .zip(out_idents.iter())
                        .map(|(port, ident)| quote! { io.push_typed(Some(#port), #ident); })
                        .collect(),
                };
                quote! {
                    match #call {
                        Ok(val) => {
                            let (#(#out_idents),*) = val;
                            #(#out_push_stmts)*
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                }
            } else {
                let push_stmt = ok_ty
                    .and_then(payload_inner_type)
                    .map(|inner| {
                        quote! { io.push_payload::<#inner>(Some(#out_port), val); }
                    })
                    .unwrap_or_else(|| quote! { io.push_typed(Some(#out_port), val); });
                quote! {
                    match #call {
                        Ok(val) => {
                            #push_stmt
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                }
            }
        } else {
            quote! { #call; Ok(()) }
        };

        let state_support = state_ty.clone().map(|sty| {
            quote! {
                static STATE: ::std::sync::OnceLock<
                    ::std::sync::Mutex<::std::collections::HashMap<String, #sty>>
                > = ::std::sync::OnceLock::new();
            }
        });
        let state_binding = if let (Some(sty), Some(id)) = (state_ty.clone(), state_param.clone()) {
            Some(quote! {
                let state_map = STATE.get_or_init(|| ::std::sync::Mutex::new(::std::collections::HashMap::new()));
                let mut state_guard = state_map.lock()
                    .map_err(|_| #runtime_crate::NodeError::Handler("state lock poisoned".into()))?;
                let state_key = node.label.clone().unwrap_or_else(|| node.id.clone());
                let #id: &mut #sty = state_guard.entry(state_key).or_insert_with(|| #sty::default());
            })
        } else {
            None
        };

        if let Some(cap_str) = capability_attr.clone() {
            let cap_lit = cap_str;
            let port_idents: Vec<LitStr> = port_names.clone();
            quote! {
                let mut args_any: Vec<&dyn ::std::any::Any> = Vec::new();
                #(args_any.push(
                    io.get_any_raw(#port_idents)
                        .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port_idents)))?
                );)*
                {
                    let cap_read = #runtime_crate::capabilities::global()
                        .read()
                        .map_err(|_| #runtime_crate::NodeError::Handler("capability lock poisoned".into()))?;
                    let entries = cap_read
                        .get(#cap_lit)
                        .ok_or_else(|| #runtime_crate::NodeError::InvalidInput("missing capability entries".into()))?;
                    let mut dispatched = false;
                    for entry in entries {
                        if args_any.len() == entry.type_ids.len()
                            && args_any
                                .iter()
                                .zip(entry.type_ids.iter())
                                .all(|(a, tid)| a.type_id() == *tid)
                        {
                            let out = (entry.func)(&args_any)?;
                            io.push_output(Some(#out_port), out);
                            dispatched = true;
                            break;
                        }
                    }
                    if !dispatched {
                        return Err(#runtime_crate::NodeError::InvalidInput("unsupported capability type".into()));
                    }
                    Ok(())
                }
            }
        } else {
            let mut arg_fetch_mut_stmts: Vec<proc_macro2::TokenStream> = Vec::new();
            let mut arg_fetch_ref_stmts: Vec<proc_macro2::TokenStream> = Vec::new();
            let mut config_fetch_stmts: Vec<proc_macro2::TokenStream> = Vec::new();
            for (idx, cfg) in config_args.iter().enumerate() {
                let ident = &cfg.ident;
                let ty = &cfg.ty;
                let owned_ident = syn::Ident::new(&format!("__cfg_owned_{idx}"), Span::call_site());
                let sanitized_ident =
                    syn::Ident::new(&format!("__cfg_sanitized_{idx}"), Span::call_site());
                let value_ident = syn::Ident::new(&format!("__cfg_value_{idx}"), Span::call_site());
                let assign = if cfg.is_ref {
                    if cfg.is_mut {
                        quote! { let #ident = &mut #value_ident; }
                    } else {
                        quote! { let #ident = &#value_ident; }
                    }
                } else {
                    quote! { let #ident = #value_ident; }
                };
                config_fetch_stmts.push(quote! {
                    let #owned_ident = <#ty as #runtime_crate::config::NodeConfig>::from_io(io)?;
                    let #sanitized_ident = <#ty as #runtime_crate::config::NodeConfig>::sanitize(#owned_ident)
                        .map_err(|e| #runtime_crate::NodeError::InvalidInput(e.to_string()))?;
                    if !#sanitized_ident.changes.is_empty() {
                        #runtime_crate::config::log_config_changes(&node.id, &#sanitized_ident.changes);
                    }
                    let mut #value_ident = #sanitized_ident.value;
                    <#ty as #runtime_crate::config::NodeConfig>::validate(&#value_ident)
                        .map_err(|e| #runtime_crate::NodeError::InvalidInput(e.to_string()))?;
                    #assign
                });
            }
            for idx in 0..arg_idents.len() {
                let ident = &arg_idents[idx];
                let ty = &arg_types[idx];
                let port = &port_names[idx];
                let (ty_core, is_ref, is_ref_mut) = if let syn::Type::Reference(r) = ty {
                    (&*r.elem, true, r.mutability.is_some())
                } else {
                    (ty, false, false)
                };
                let is_binding_mut = arg_mut_bindings.get(idx).copied().unwrap_or(false);
                let mode = if is_ref {
                    if is_ref_mut { "borrowed_mut" } else { "borrowed" }
                } else if is_binding_mut {
                    "owned_mut"
                } else {
                    "owned"
                };
                let is_payload = if let syn::Type::Path(tp) = ty_core
                    && tp.qself.is_none()
                    && let Some(seg) = tp.path.segments.last()
                {
                    seg.ident == "Payload"
                } else {
                    false
                };

                let fanin_inner = if let syn::Type::Path(tp) = ty_core
                    && tp.qself.is_none()
                    && let Some(seg) = tp.path.segments.last()
                    && seg.ident == "FanIn"
                    && let syn::PathArguments::AngleBracketed(ab) = &seg.arguments
                    && let Some(syn::GenericArgument::Type(inner_ty)) = ab.args.first()
                {
                    Some(inner_ty)
                } else {
                    None
                };
                let option_inner = if let syn::Type::Path(tp) = ty_core
                    && tp.qself.is_none()
                    && let Some(seg) = tp.path.segments.last()
                    && seg.ident == "Option"
                    && let syn::PathArguments::AngleBracketed(ab) = &seg.arguments
                    && let Some(syn::GenericArgument::Type(inner_ty)) = ab.args.first()
                {
                    Some(inner_ty)
                } else {
                    None
                };

                let fetch = if let Some(inner_ty) = fanin_inner {
                    let tmp_ident =
                        syn::Ident::new(&format!("__fanin_indexed_{idx}"), Span::call_site());
                    quote! {
                        let #tmp_ident = io.get_any_all_fanin_indexed::<#inner_ty>(#port);
                        let #ident = #runtime_crate::FanIn::<#inner_ty>::from_indexed(#tmp_ident);
                    }
                } else if option_inner.is_some() && mode == "owned" {
                    let inner_ty = option_inner.unwrap();
                    quote! {
                        let #ident = io.get_typed::<#inner_ty>(#port);
                    }
                } else if let syn::Type::Path(tp) = ty_core {
                    if let Some(seg) = tp.path.segments.last() {
                        let ident_seg = &seg.ident;
                        if ident_seg == "Payload" {
                            if let syn::PathArguments::AngleBracketed(ab) = &seg.arguments {
                                if let Some(syn::GenericArgument::Type(inner_ty)) = ab.args.first()
                                {
                                    match mode {
                                        "borrowed" => {
                                            let tmp_ident = syn::Ident::new(&format!("__payload_ref_{idx}"), Span::call_site());
                                            quote! {
                                                let #tmp_ident = io
                                                    .get_payload::<#inner_ty>(#port)
                                                    .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                                let #ident = &#tmp_ident;
                                            }
                                        }
                                        "borrowed_mut" => {
                                            let tmp_ident = syn::Ident::new(&format!("__payload_mut_{idx}"), Span::call_site());
                                            quote! {
                                                let mut #tmp_ident = io
                                                    .get_payload_mut::<#inner_ty>(#port)
                                                    .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                                let #ident = &mut #tmp_ident;
                                            }
                                        }
                                        "owned_mut" => quote! {
                                            let #ident = io
                                                .get_payload_mut::<#inner_ty>(#port)
                                                .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                        },
                                        _ => quote! {
                                            let #ident = io
                                                .get_payload_mut::<#inner_ty>(#port)
                                                .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                        },
                                    }
                                } else {
                                    quote! {
                                        let #ident = io
                                            .get_any::<#ty_core>(#port)
                                            .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                    }
                                }
                            } else {
                                quote! {
                                    let #ident = io
                                        .get_typed::<#ty_core>(#port)
                                        .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                }
                            }
                        } else {
                            match mode {
                                "borrowed" => quote! {
                                    let #ident = io
                                        .get_typed_ref::<#ty_core>(#port)
                                        .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                },
                                "borrowed_mut" => {
                                    let tmp_ident = syn::Ident::new(&format!("__borrowed_mut_{idx}"), Span::call_site());
                                    quote! {
                                        let mut #tmp_ident = io
                                            .get_typed_mut::<#ty_core>(#port)
                                            .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                        let #ident = &mut #tmp_ident;
                                    }
                                }
                                "owned_mut" => quote! {
                                    let #ident = io
                                        .get_typed_mut::<#ty_core>(#port)
                                        .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                },
                                _ => quote! {
                                    let #ident = io
                                        .get_typed_mut::<#ty_core>(#port)
                                        .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                },
                            }
                        }
                    } else {
                        match mode {
                            "borrowed" => quote! {
                                let #ident = io
                                    .get_any_ref::<#ty_core>(#port)
                                    .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                            },
                            "borrowed_mut" => {
                                let tmp_ident = syn::Ident::new(&format!("__borrowed_mut_{idx}"), Span::call_site());
                                quote! {
                                    let mut #tmp_ident = io
                                        .get_any_mut::<#ty_core>(#port)
                                        .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                    let #ident = &mut #tmp_ident;
                                }
                            }
                            "owned_mut" => quote! {
                                let #ident = io
                                    .get_any_mut::<#ty_core>(#port)
                                    .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                            },
                            _ => quote! {
                                let #ident = io
                                    .get_any_mut::<#ty_core>(#port)
                                    .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                            },
                        }
                    }
                } else {
                    match mode {
                        "borrowed" => quote! {
                            let #ident = io
                                .get_typed_ref::<#ty_core>(#port)
                                .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                        },
                        "borrowed_mut" => {
                            let tmp_ident = syn::Ident::new(&format!("__borrowed_mut_{idx}"), Span::call_site());
                            quote! {
                                let mut #tmp_ident = io
                                    .get_typed_mut::<#ty_core>(#port)
                                    .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                                let #ident = &mut #tmp_ident;
                            }
                        }
                        "owned_mut" => quote! {
                            let #ident = io
                                .get_typed_mut::<#ty_core>(#port)
                                .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                        },
                        _ => quote! {
                            let #ident = io
                                .get_typed_mut::<#ty_core>(#port)
                                .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port)))?;
                        },
                    }
                };
                let needs_immut_borrow = mode == "borrowed" && !is_payload;
                if needs_immut_borrow {
                    arg_fetch_ref_stmts.push(fetch);
                } else {
                    arg_fetch_mut_stmts.push(fetch);
                }
            }

            let shader_gpu_init = if shader_tokens.is_some() {
                quote! { let __ctx_gpu: Option<#gpu_crate::GpuContextHandle> = ctx.gpu.clone(); }
            } else {
                quote! {}
            };

            quote! {
                #(#config_fetch_stmts)*
                #(#arg_fetch_mut_stmts)*
                #(#arg_fetch_ref_stmts)*
                #shader_gpu_init
                #state_support
                #state_binding
                #shader_tokens
                #ret_handling
            }
        }
    };

    if let Some(arg) = &graph_ctx_arg {
        if !arg.is_mut_ref {
            return TokenStream::from(compile_error(
                "GraphCtx parameter must be passed as &mut GraphCtx".into(),
            ));
        }
    }

    let is_graph_node = graph_ctx_arg.is_some();
    if is_graph_node {
        if state_ty_attr.is_some() {
            return TokenStream::from(compile_error(
                "graph-backed nodes cannot use state(...)".into(),
            ));
        }
        if !config_types.is_empty() {
            return TokenStream::from(compile_error(
                "graph-backed nodes cannot use config(...) types".into(),
            ));
        }
        if capability_attr.is_some() {
            return TokenStream::from(compile_error(
                "graph-backed nodes cannot use capability(...)".into(),
            ));
        }
        if runtime_node_present || exec_ctx_present || node_io_present || shader_ctx_present {
            return TokenStream::from(compile_error(
                "graph-backed nodes cannot use RuntimeNode/ExecutionContext/NodeIo/ShaderContext parameters".into(),
            ));
        }
    }

    let has_fanin_inputs = !is_low_level
        && arg_types.iter().any(|ty| {
            if let syn::Type::Path(tp) = ty
                && tp.qself.is_none()
                && let Some(seg) = tp.path.segments.last()
            {
                return seg.ident == "FanIn";
            }
            false
        });

    let sync_groups_tokens: proc_macro2::TokenStream = if let Some(ts) = sync_groups_attr {
        ts
    } else if has_fanin_inputs {
        // Disable implicit AllReady sync across indexed fan-in ports. FanIn ports are dynamic and
        // do not share correlation ids, so the default alignment would otherwise suppress firing.
        quote! {
            vec![#core_crate::sync::SyncGroup {
                name: "__fanin".into(),
                policy: #core_crate::sync::SyncPolicy::AllReady,
                backpressure: None,
                capacity: None,
                ports: Vec::new(),
            }]
        }
    } else {
        quote! { Vec::<#core_crate::sync::SyncGroup>::new() }
    };

    let has_generics = generics_attr.is_some();
    let fn_generics = input.sig.generics.clone();
    if has_generics && fn_generics.params.is_empty() {
        return TokenStream::from(compile_error(
            "generics(...) specified but function has no generic parameters".into(),
        ));
    }
    let (fn_impl_generics, fn_ty_generics, fn_where_clause) = fn_generics.split_for_impl();
    let fn_turbofish_generics: proc_macro2::TokenStream = quote! { ::#fn_ty_generics };

    let generic_type_params: ::std::collections::HashSet<::std::string::String> = input
        .sig
        .generics
        .type_params()
        .map(|tp| tp.ident.to_string())
        .collect();

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
                        let lit = LitStr::new("generic", Span::call_site());
                        return Some(quote! {
                            #data_crate::model::TypeExpr::Opaque(::std::string::String::from(#lit))
                        });
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

    let is_fanin_ty = |ty: &syn::Type| -> bool {
        let ty = if let syn::Type::Reference(r) = ty {
            &*r.elem
        } else {
            ty
        };
        if let syn::Type::Path(tp) = ty
            && tp.qself.is_none()
            && let Some(seg) = tp.path.segments.last()
            && seg.ident == "FanIn"
        {
            return true;
        }
        false
    };

    let descriptor_input_ports_tokens: Vec<proc_macro2::TokenStream> = if is_low_level {
        inputs_vec
            .iter()
            .map(|port| {
                let name = &port.name;
                let source = if let Some(s) = &port.source {
                    quote! { Some(#s.into()) }
                } else {
                    quote! { None }
                };
                let default = if let Some(ts) = &port.default_value {
                    quote! { Some(#ts) }
                } else {
                    quote! { None }
                };
                let ty_expr = if let Some(ty) = port.ty_override.as_ref() {
                    quote! { (#ty) }
                } else {
                    let lit = LitStr::new("rust:unknown", Span::call_site());
                    quote! { #data_crate::model::TypeExpr::Opaque(::std::string::String::from(#lit)) }
                };
                quote! {
                    #registry_crate::store::Port {
                        name: #name.into(),
                        ty: #ty_expr,
                        access: ::core::default::Default::default(),
                        source: #source,
                        const_value: #default,
                    }
                }
            })
            .collect()
    } else {
        effective_inputs_for_args
            .iter()
            .enumerate()
            .filter_map(|(idx, port)| {
                let aty = arg_types.get(idx)?;
                let aty = if let syn::Type::Reference(r) = aty {
                    &*r.elem
                } else {
                    aty
                };
                if is_fanin_ty(aty) {
                    return None;
                }

                let name = &port.name;
                let source = if let Some(s) = &port.source {
                    quote! { Some(#s.into()) }
                } else {
                    quote! { None }
                };
                let default = if let Some(ts) = &port.default_value {
                    quote! { Some(#ts) }
                } else {
                    quote! { None }
                };
                let ty_expr = if let Some(ty) = port.ty_override.as_ref() {
                    quote! { (#ty) }
                } else if let Some(ts) = type_expr_for(aty, &generic_type_params, &data_crate) {
                    ts
                } else {
                    opaque_fallback_type_expr_for(aty, &data_crate)
                };

                Some(quote! {
                    #registry_crate::store::Port {
                        name: #name.into(),
                        ty: #ty_expr,
                        access: ::core::default::Default::default(),
                        source: #source,
                        const_value: #default,
                    }
                })
            })
            .collect()
    };

    let output_type_exprs: Vec<proc_macro2::TokenStream> = {
        // Attempt to map return type to outputs; fallback to opaque.
        fn peel_wrapped(ty: &syn::Type) -> &syn::Type {
            if let syn::Type::Path(p) = ty {
                if let Some(seg) = p.path.segments.last() {
                    let ident = seg.ident.to_string();
                    if ident == "Result" || ident == "Option" {
                        if let syn::PathArguments::AngleBracketed(ab) = &seg.arguments {
                            if let Some(syn::GenericArgument::Type(inner)) = ab.args.first() {
                                return inner;
                            }
                        }
                    }
                }
            }
            ty
        }

        let explicit: Vec<Option<proc_macro2::TokenStream>> = outputs_vec
            .iter()
            .map(|p| p.ty_override.as_ref().map(|ts| quote! { (#ts) }))
            .collect();

        let mut out: Vec<proc_macro2::TokenStream> = Vec::new();
        if let syn::ReturnType::Type(_, ty) = &input.sig.output {
            let mut base_ty: &syn::Type = ty.as_ref();
            loop {
                let next = peel_wrapped(base_ty);
                if std::ptr::eq(next, base_ty) {
                    break;
                }
                base_ty = next;
            }

            if let syn::Type::Tuple(t) = base_ty {
                if t.elems.len() == outputs_vec.len() {
                    for (idx, elem) in t.elems.iter().enumerate() {
                        if let Some(ts) = explicit.get(idx).and_then(|v| v.clone()) {
                            out.push(ts);
                            continue;
                        }
                        out.push(
                            type_expr_for(elem, &generic_type_params, &data_crate).unwrap_or_else(
                                || opaque_fallback_type_expr_for(elem, &data_crate),
                            ),
                        );
                    }
                }
            } else if outputs_vec.len() == 1 {
                if let Some(ts) = explicit.first().and_then(|v| v.clone()) {
                    out.push(ts);
                } else if let Some(ts) = type_expr_for(base_ty, &generic_type_params, &data_crate) {
                    out.push(ts);
                } else {
                    out.push(opaque_fallback_type_expr_for(base_ty, &data_crate));
                }
            }
        }
        while out.len() < outputs_vec.len() {
            let idx = out.len();
            if let Some(ts) = explicit.get(idx).and_then(|v| v.clone()) {
                out.push(ts);
            } else {
                let lit = LitStr::new("rust:unknown", Span::call_site());
                out.push(quote! { #data_crate::model::TypeExpr::Opaque(::std::string::String::from(#lit)) });
            }
        }
        out
    };

    let graph_port_names: Vec<LitStr> = effective_inputs_for_args
        .iter()
        .map(|p| p.name.clone())
        .collect();

    let graph_call_args: Vec<proc_macro2::TokenStream> = if is_graph_node {
        let mut args = Vec::new();
        for arg in &sig_for_ports.inputs {
            if let syn::FnArg::Typed(pat) = arg {
                if let syn::Pat::Ident(id) = &*pat.pat {
                    if let Some(ctx) = &graph_ctx_arg {
                        if id.ident == ctx.ident {
                            args.push(quote! { &mut __graph_ctx });
                            continue;
                        }
                    }
                    let ident = &id.ident;
                    args.push(quote! { #ident });
                }
            }
        }
        args
    } else {
        Vec::new()
    };

    let graph_input_bindings: Vec<proc_macro2::TokenStream> = if is_graph_node {
        arg_idents
            .iter()
            .zip(graph_port_names.iter())
            .map(|(ident, port)| {
                quote! {
                    let #ident = __graph_ctx.input(#port);
                }
            })
            .collect()
    } else {
        Vec::new()
    };

    let graph_output_bindings: proc_macro2::TokenStream = if is_graph_node {
        if output_names.is_empty() {
            quote! {}
        } else if output_names.len() == 1 {
            let name = &output_names[0];
            quote! {
                __graph_ctx.bind_output(#name, &__graph_ret);
            }
        } else {
            let out_idents: Vec<syn::Ident> = (0..output_names.len())
                .map(|i| syn::Ident::new(&format!("__graph_out_{i}"), Span::call_site()))
                .collect();
            let bind_calls: Vec<proc_macro2::TokenStream> = output_names
                .iter()
                .zip(out_idents.iter())
                .map(|(name, ident)| {
                    quote! { __graph_ctx.bind_output(#name, &#ident); }
                })
                .collect();
            quote! {
                let (#(#out_idents),*) = __graph_ret;
                #(#bind_calls)*
            }
        }
    } else {
        quote! {}
    };

    let fanin_inputs_tokens: Vec<proc_macro2::TokenStream> = if is_low_level {
        Vec::new()
    } else {
        effective_inputs_for_args
            .iter()
            .enumerate()
            .filter_map(|(idx, port)| {
                let aty = arg_types.get(idx)?;
                let syn::Type::Path(tp) = aty else {
                    return None;
                };
                let seg = tp.path.segments.last()?;
                if seg.ident != "FanIn" {
                    return None;
                }
                let syn::PathArguments::AngleBracketed(ab) = &seg.arguments else {
                    return None;
                };
                let Some(syn::GenericArgument::Type(inner_ty)) = ab.args.first() else {
                    return None;
                };

                let prefix = &port.name;
                let ty_expr = if let Some(ty) = port.ty_override.as_ref() {
                    quote! { (#ty) }
                } else {
                    type_expr_for(inner_ty, &generic_type_params, &data_crate)
                        .unwrap_or_else(|| opaque_fallback_type_expr_for(inner_ty, &data_crate))
                };
                Some(quote! {
                    #registry_crate::store::FanInPort {
                        prefix: #prefix.into(),
                        start: 0,
                        ty: #ty_expr,
                    }
                })
            })
            .collect()
    };

    let (handle_input_idents, handle_input_names): (Vec<syn::Ident>, Vec<LitStr>) = if is_low_level
    {
        (input_idents.clone(), input_names.clone())
    } else {
        let mut idents = Vec::new();
        let mut names = Vec::new();
        for (idx, port) in effective_inputs_for_args.iter().enumerate() {
            let Some(ty) = arg_types.get(idx) else {
                continue;
            };
            if is_fanin_ty(ty) {
                continue;
            }
            idents.push(port_ident(&port.name.value()));
            names.push(port.name.clone());
        }
        (idents, names)
    };

    let input_access: Vec<(LitStr, LitStr)> = if is_low_level {
        Vec::new()
    } else {
        let mut out = Vec::new();
        for (idx, port) in effective_inputs_for_args.iter().enumerate() {
            let Some(ty) = arg_types.get(idx) else {
                continue;
            };
            if is_fanin_ty(ty) {
                continue;
            }
            let (is_ref, is_ref_mut) = if let syn::Type::Reference(r) = ty {
                (true, r.mutability.is_some())
            } else {
                (false, false)
            };
            let is_binding_mut = arg_mut_bindings.get(idx).copied().unwrap_or(false);
            let access = if is_ref {
                if is_ref_mut { "mutable" } else { "borrowed" }
            } else if is_binding_mut {
                "mutable"
            } else {
                "owned"
            };
            out.push((port.name.clone(), LitStr::new(access, Span::call_site())));
        }
        out
    };

    let metadata_tokens: proc_macro2::TokenStream = {
        let mut inserts: Vec<proc_macro2::TokenStream> = Vec::new();
        let lit_to_value = |lit: &Lit| {
            match lit {
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
            }
        };
        if let Some(summary) = &summary_attr {
            inserts.push(quote! {
                __meta.insert(
                    ::std::string::String::from("summary"),
                    #data_crate::model::Value::String(::std::borrow::Cow::from(#summary)),
                );
            });
        }
        if let Some(description) = &description_attr {
            inserts.push(quote! {
                __meta.insert(
                    ::std::string::String::from("description"),
                    #data_crate::model::Value::String(::std::borrow::Cow::from(#description)),
                );
            });
        }
        for port in &inputs_vec {
            if let Some(desc) = &port.description {
                let key = LitStr::new(
                    &format!("inputs.{}.description", port.name.value()),
                    Span::call_site(),
                );
                inserts.push(quote! {
                    __meta.insert(
                        ::std::string::String::from(#key),
                        #data_crate::model::Value::String(::std::borrow::Cow::from(#desc)),
                    );
                });
            }
            for (meta_key, meta_value) in &port.meta {
                let key = LitStr::new(
                    &format!("inputs.{}.{}", port.name.value(), meta_key.value()),
                    Span::call_site(),
                );
                let value = lit_to_value(meta_value);
                inserts.push(quote! {
                    __meta.insert(
                        ::std::string::String::from(#key),
                        #value,
                    );
                });
            }
        }
        for (name, access) in &input_access {
            let key = LitStr::new(
                &format!("inputs.{}.access", name.value()),
                Span::call_site(),
            );
            inserts.push(quote! {
                __meta.insert(
                    ::std::string::String::from(#key),
                    #data_crate::model::Value::String(::std::borrow::Cow::from(#access)),
                );
            });
        }
        for port in &outputs_vec {
            if let Some(desc) = &port.description {
                let key = LitStr::new(
                    &format!("outputs.{}.description", port.name.value()),
                    Span::call_site(),
                );
                inserts.push(quote! {
                    __meta.insert(
                        ::std::string::String::from(#key),
                        #data_crate::model::Value::String(::std::borrow::Cow::from(#desc)),
                    );
                });
            }
            for (meta_key, meta_value) in &port.meta {
                let key = LitStr::new(
                    &format!("outputs.{}.{}", port.name.value(), meta_key.value()),
                    Span::call_site(),
                );
                let value = lit_to_value(meta_value);
                inserts.push(quote! {
                    __meta.insert(
                        ::std::string::String::from(#key),
                        #value,
                    );
                });
            }
        }

        quote! {{
            let mut __meta: ::std::collections::BTreeMap<
                ::std::string::String,
                #data_crate::model::Value,
            > = ::std::collections::BTreeMap::new();
            #(#inserts)*
            #(
                __meta.extend(<#config_types as #runtime_crate::config::NodeConfig>::metadata());
            )*
            __meta
        }}
    };

    let config_inputs_extend: Vec<proc_macro2::TokenStream> = config_types
        .iter()
        .map(|ty| {
            quote! {
                __inputs.extend(<#ty as #runtime_crate::config::NodeConfig>::ports());
            }
        })
        .collect();

    let descriptor_fn = if has_generics {
        quote! {
            pub fn descriptor_for #fn_impl_generics (id: impl Into<String>) -> #registry_crate::store::NodeDescriptor #fn_where_clause {
                let id_str = id.into();
                #registry_crate::store::NodeDescriptor {
                    id: #registry_crate::ids::NodeId::new(&id_str),
                    feature_flags: vec![],
                    label: None,
                    group: None,
                    inputs: {
                        let mut __inputs = vec![#(#descriptor_input_ports_tokens),*];
                        #(#config_inputs_extend)*
                        __inputs
                    },
                    fanin_inputs: vec![#(#fanin_inputs_tokens),*],
                    outputs: vec![#(#registry_crate::store::Port {
                        name: #output_names.into(),
                        ty: #output_type_exprs,
                        access: ::core::default::Default::default(),
                        source: #output_sources,
                        const_value: None,
                    }),*],
                    default_compute: #compute_expr,
                    sync_groups: #sync_groups_tokens,
                    metadata: #metadata_tokens,
                }
            }
        }
    } else {
        quote! {
            pub fn descriptor() -> #registry_crate::store::NodeDescriptor {
                #registry_crate::store::NodeDescriptor {
                    id: #registry_crate::ids::NodeId::new(#id),
                    feature_flags: vec![],
                    label: None,
                    group: None,
                    inputs: {
                        let mut __inputs = vec![#(#descriptor_input_ports_tokens),*];
                        #(#config_inputs_extend)*
                        __inputs
                    },
                    fanin_inputs: vec![#(#fanin_inputs_tokens),*],
                    outputs: vec![#(#registry_crate::store::Port {
                        name: #output_names.into(),
                        ty: #output_type_exprs,
                        access: ::core::default::Default::default(),
                        source: #output_sources,
                        const_value: None,
                    }),*],
                    default_compute: #compute_expr,
                    sync_groups: #sync_groups_tokens,
                    metadata: #metadata_tokens,
                }
            }
        }
    };

    let handler_registry_fn = if is_graph_node {
        if has_generics {
            quote! {
                pub fn handler_registry_for #fn_impl_generics (id: impl Into<String>) -> #runtime_crate::handler_registry::HandlerRegistry #fn_where_clause {
                    let _ = id;
                    #runtime_crate::handler_registry::HandlerRegistry::new()
                }
            }
        } else {
            quote! {
                pub fn handler_registry() -> #runtime_crate::handler_registry::HandlerRegistry {
                    #runtime_crate::handler_registry::HandlerRegistry::new()
                }
            }
        }
    } else if has_generics {
        quote! {
            pub fn handler_registry_for #fn_impl_generics (id: impl Into<String>) -> #runtime_crate::handler_registry::HandlerRegistry #fn_where_clause {
                let id_str = id.into();
                let mut reg = #runtime_crate::handler_registry::HandlerRegistry::new();
                reg.on(&id_str, |node, ctx, io| {
                    #handler_body
                });
                reg
            }
        }
    } else {
        quote! {
            pub fn handler_registry() -> #runtime_crate::handler_registry::HandlerRegistry {
                let mut reg = #runtime_crate::handler_registry::HandlerRegistry::new();
                reg.on(Self::ID, |node, ctx, io| {
                    #handler_body
                });
                reg
            }
        }
    };

    let graph_register_tokens = if is_graph_node {
        let input_names = graph_port_names.clone();
        let output_names = output_names.clone();
        quote! {
            let __graph_inputs = [#(#input_names),*];
            let __graph_outputs = [#(#output_names),*];
            let mut __graph_ctx = #runtime_crate::graph_builder::GraphCtx::new(
                &into.registry,
                &__graph_inputs,
                &__graph_outputs,
            );
            #(#graph_input_bindings)*
            let __graph_ret = #inner_fn_ident(#(#graph_call_args),*);
            #graph_output_bindings
            let __graph = __graph_ctx.build();
            let __graph_json = #runtime_crate::graph_builder::graph_to_json(&__graph)
                .map_err(|_| "graph serialization failed")?;
            let __group_id = ::std::format!("{}::group", desc.id.0);
            let mut __group_builder =
                #registry_crate::store::GroupDescriptorBuilder::new(__group_id.clone(), __graph_json);
            let __group_label = desc
                .label
                .clone()
                .unwrap_or_else(|| desc.id.0.clone());
            __group_builder = __group_builder.label(__group_label);
            for __flag in desc.feature_flags.iter() {
                __group_builder = __group_builder.feature_flag(__flag.clone());
            }
            __group_builder = __group_builder.metadata(
                ::std::string::String::from("daedalus.embedded_host"),
                #data_crate::model::Value::String(::std::borrow::Cow::from("host")),
            );
            let __group_desc = __group_builder.build().map_err(|_| "group descriptor invalid")?;
            match into.registry.register_group(__group_desc) {
                Ok(_) => {}
                Err(e) if e.code() == #registry_crate::diagnostics::RegistryErrorCode::Conflict => {}
                Err(_) => return Err("registry conflict"),
            }
            desc.group = Some(#registry_crate::ids::GroupId::new(__group_id));
        }
    } else {
        quote! {}
    };

    let register_fn = if has_generics {
        if is_graph_node {
            quote! {
                pub fn register_for #fn_impl_generics (
                    into: &mut #runtime_crate::plugins::PluginRegistry,
                    id: impl Into<String>,
                ) -> Result<#handle_ident, &'static str> #fn_where_clause {
                    let local_id: String = id.into();
                    let full_id = if let Some(prefix) = &into.current_prefix {
                        #runtime_crate::apply_node_prefix(prefix, &local_id)
                    } else {
                        local_id.clone()
                    };
                    let mut desc = #struct_ident::descriptor_for #fn_turbofish_generics (full_id.clone());
                    #graph_register_tokens
                    match into.registry.register_node(desc) {
                        Ok(_) => {}
                        Err(e) if e.code() == #registry_crate::diagnostics::RegistryErrorCode::Conflict => {}
                        Err(_) => return Err("registry conflict"),
                    }
                    Ok(#handle_ident::new_with_id(full_id))
                }
            }
        } else {
            quote! {
                pub fn register_for #fn_impl_generics (
                    into: &mut #runtime_crate::plugins::PluginRegistry,
                    id: impl Into<String>,
                ) -> Result<#handle_ident, &'static str> #fn_where_clause {
                    let local_id: String = id.into();
                    let full_id = if let Some(prefix) = &into.current_prefix {
                        #runtime_crate::apply_node_prefix(prefix, &local_id)
                    } else {
                        local_id.clone()
                    };
                    let mut desc = #struct_ident::descriptor_for #fn_turbofish_generics (full_id.clone());
                    match into.registry.register_node(desc) {
                        Ok(_) => {}
                        Err(e) if e.code() == #registry_crate::diagnostics::RegistryErrorCode::Conflict => {}
                        Err(_) => return Err("registry conflict"),
                    }
                    let handlers = #struct_ident::handler_registry_for #fn_turbofish_generics (full_id.clone());
                    into.handlers.merge(handlers);
                    Ok(#handle_ident::new_with_id(full_id))
                }
            }
        }
    } else {
        quote! {}
    };

    let fn_generics = input.sig.generics.clone();
    let (cap_impl_generics, _cap_ty_generics, cap_where_clause) = fn_generics.split_for_impl();
    let cap_type_param = fn_generics
        .params
        .iter()
        .find_map(|p| match p {
            syn::GenericParam::Type(ty) => Some(ty.ident.clone()),
            _ => None,
        })
        .unwrap_or_else(|| syn::Ident::new("T", Span::call_site()));

    let capability_helper = if let Some(cap) = capability_attr.clone() {
        match inputs_vec.len() {
            2 => {
                quote! {
                    pub fn register_capability #cap_impl_generics (
                        into: &mut #runtime_crate::plugins::PluginRegistry,
                    ) #cap_where_clause {
                        into.register_capability_typed::<#cap_type_param, _>(#cap, |a, b| #inner_fn_ident(a.clone(), b.clone()));
                    }
                }
            }
            3 => {
                quote! {
                    pub fn register_capability #cap_impl_generics (
                        into: &mut #runtime_crate::plugins::PluginRegistry,
                    ) #cap_where_clause {
                        into.register_capability_typed3::<#cap_type_param, _>(#cap, |x, lo, hi| #inner_fn_ident(x.clone(), lo.clone(), hi.clone()));
                    }
                }
            }
            _ => {
                quote! { compile_error!("capability nodes currently support only 2 or 3 inputs"); }
            }
        }
    } else {
        quote! {}
    };

    let node_install_impl = if has_generics && capability_attr.is_none() {
        quote! {}
    } else if is_graph_node {
        quote! {
            impl #runtime_crate::plugins::NodeInstall for #struct_ident {
                fn register(into: &mut #runtime_crate::plugins::PluginRegistry) -> Result<(), &'static str> {
                    let mut desc = #struct_ident::descriptor();
                    if let Some(prefix) = &into.current_prefix {
                        let full_id = #runtime_crate::apply_node_prefix(prefix, #struct_ident::ID);
                        desc.id = #registry_crate::ids::NodeId::new(&full_id);
                    }
                    #graph_register_tokens
                    match into.registry.register_node(desc) {
                        Ok(_) => {}
                        Err(e) if e.code() == #registry_crate::diagnostics::RegistryErrorCode::Conflict => {}
                        Err(_) => return Err("registry conflict"),
                    }
                    Ok(())
                }
            }
        }
    } else {
        quote! {
            impl #runtime_crate::plugins::NodeInstall for #struct_ident {
                fn register(into: &mut #runtime_crate::plugins::PluginRegistry) -> Result<(), &'static str> {
                    let mut desc = #struct_ident::descriptor();
                    if let Some(prefix) = &into.current_prefix {
                        let full_id = #runtime_crate::apply_node_prefix(prefix, #struct_ident::ID);
                        desc.id = #registry_crate::ids::NodeId::new(&full_id);
                    }
                    match into.registry.register_node(desc) {
                        Ok(_) => {}
                        Err(e) if e.code() == #registry_crate::diagnostics::RegistryErrorCode::Conflict => {}
                        Err(_) => return Err("registry conflict"),
                    }
                    let handlers = if let Some(prefix) = &into.current_prefix {
                        #struct_ident::handler_registry().with_prefix(prefix)
                    } else {
                        #struct_ident::handler_registry()
                    };
                    into.handlers.merge(handlers);
                    Ok(())
                }
            }
        }
    };

    if is_graph_node {
        let port_ty: syn::Type = syn::parse2(quote! { #runtime_crate::handles::PortHandle })
            .expect("failed to build PortHandle type");
        for arg in input.sig.inputs.iter_mut() {
            if let syn::FnArg::Typed(pat) = arg {
                if let syn::Pat::Ident(id) = &*pat.pat {
                    if let Some(ctx) = &graph_ctx_arg {
                        if id.ident == ctx.ident {
                            continue;
                        }
                    }
                    *pat.ty = port_ty.clone();
                }
            }
        }
        let output_len = output_names.len();
        input.sig.output = if output_len == 0 {
            syn::ReturnType::Default
        } else if output_len == 1 {
            syn::ReturnType::Type(Default::default(), Box::new(port_ty.clone()))
        } else {
            let tuple_elems: Vec<syn::Type> = (0..output_len).map(|_| port_ty.clone()).collect();
            let tuple_ty: syn::Type =
                syn::parse2(quote! { (#(#tuple_elems),*) }).expect("failed to build tuple type");
            syn::ReturnType::Type(Default::default(), Box::new(tuple_ty))
        };
    }

    // Final expanded tokens
    TokenStream::from(quote! {
        #input

        #[allow(non_camel_case_types)]
        pub struct #struct_ident;

        impl #struct_ident {
            pub const ID: &'static str = #id;

            #descriptor_fn

            #handler_registry_fn

            #register_fn

            #capability_helper
        }

        #node_install_impl

        #[derive(Clone, Debug)]
        pub struct #inputs_ident {
            #(pub #handle_input_idents: #runtime_crate::handles::PortHandle),*
        }

        impl #inputs_ident {
            pub fn new(alias: &str) -> Self {
                Self {
                    #(#handle_input_idents: #runtime_crate::handles::PortHandle::new(alias, #handle_input_names)),*
                }
            }
        }

        #[derive(Clone, Debug)]
        pub struct #outputs_ident {
            #(pub #output_idents: #runtime_crate::handles::PortHandle),*
        }

        impl #outputs_ident {
            pub fn new(alias: &str) -> Self {
                Self {
                    #(#output_idents: #runtime_crate::handles::PortHandle::new(alias, #output_names)),*
                }
            }
        }

        #[derive(Clone, Debug)]
        pub struct #handle_ident {
            pub spec: #runtime_crate::handles::NodeHandle,
            pub inputs: #inputs_ident,
            pub outputs: #outputs_ident,
        }

        impl #handle_ident {
            pub fn new() -> Self {
                Self::new_with_id(#struct_ident::ID)
            }

            pub fn new_with_id(id: impl Into<String>) -> Self {
                let id_str = id.into();
                let spec = #runtime_crate::handles::NodeHandle::new(id_str);
                let alias = spec.alias.clone();
                Self {
                    spec,
                    inputs: #inputs_ident::new(&alias),
                    outputs: #outputs_ident::new(&alias),
                }
            }

            pub fn with_prefix(mut self, prefix: &str) -> Self {
                self.spec.id = #runtime_crate::apply_node_prefix(prefix, &self.spec.id);
                self.spec.alias = self.spec.id.clone();
                self.inputs = #inputs_ident::new(&self.spec.alias);
                self.outputs = #outputs_ident::new(&self.spec.alias);
                self
            }

            pub fn alias(mut self, alias: impl Into<String>) -> Self {
                let a = alias.into();
                self.spec.alias = a.clone();
                self.inputs = #inputs_ident::new(&a);
                self.outputs = #outputs_ident::new(&a);
                self
            }
        }

        impl #runtime_crate::handles::NodeHandleLike for #handle_ident {
            fn id(&self) -> &str { &self.spec.id }
            fn alias(&self) -> &str { &self.spec.alias }
        }

        impl #struct_ident {
            pub fn handle() -> #handle_ident {
                #handle_ident::new()
            }
        }

    })
}

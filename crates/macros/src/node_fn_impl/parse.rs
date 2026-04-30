use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, quote};
use syn::parse::Parser;
use syn::parse::discouraged::Speculative;
use syn::{Lit, LitStr, Member, Meta, MetaNameValue};

use crate::helpers::{AttributeArgs, NestedMeta, compile_error, lit_from_expr, parse_nested};

#[derive(Clone)]
pub(super) struct PortMeta {
    pub(super) name: LitStr,
    pub(super) source: Option<LitStr>,
    pub(super) default_value: Option<TokenStream>,
    pub(super) ty_override: Option<TokenStream>,
    pub(super) description: Option<LitStr>,
    pub(super) meta: Vec<(LitStr, Lit)>,
}

impl PortMeta {
    pub(super) fn name_only(name: LitStr) -> Self {
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

#[derive(Clone)]
pub(super) struct OutputPortMeta {
    pub(super) name: LitStr,
    pub(super) source: Option<LitStr>,
    pub(super) ty_override: Option<TokenStream>,
    pub(super) description: Option<LitStr>,
    pub(super) meta: Vec<(LitStr, Lit)>,
}

impl OutputPortMeta {
    pub(super) fn name_only(name: LitStr) -> Self {
        Self {
            name,
            source: None,
            ty_override: None,
            description: None,
            meta: Vec::new(),
        }
    }
}

pub(super) struct NodeArgs {
    pub(super) id: LitStr,
    pub(super) summary_attr: Option<LitStr>,
    pub(super) description_attr: Option<LitStr>,
    pub(super) generics_attr: Option<TokenStream>,
    pub(super) inputs: Vec<PortMeta>,
    pub(super) config_types: Vec<syn::Type>,
    pub(super) outputs: Vec<OutputPortMeta>,
    pub(super) shader_path: Option<LitStr>,
    pub(super) shader_paths: Vec<LitStr>,
    pub(super) shader_entry: LitStr,
    pub(super) shader_workgroup: Option<[u32; 3]>,
    pub(super) shader_bindings: Vec<TokenStream>,
    pub(super) shader_specs: Vec<(TokenStream, Option<LitStr>)>,
    pub(super) state_ty_attr: Option<syn::Type>,
    pub(super) compute_attr: Option<TokenStream>,
    pub(super) sync_groups_attr: Option<TokenStream>,
    pub(super) capability_attr: Option<LitStr>,
    pub(super) fallback_attr: Option<LitStr>,
    pub(super) same_payload_attr: bool,
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
    Other(TokenStream),
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

pub(super) fn parse_node_args(
    args: AttributeArgs,
    data_crate: &TokenStream,
    gpu_crate: &TokenStream,
) -> Result<NodeArgs, TokenStream> {
    let mut id: Option<LitStr> = None;
    let mut summary_attr: Option<LitStr> = None;
    let mut description_attr: Option<LitStr> = None;
    let mut generics_attr: Option<TokenStream> = None;
    let mut inputs: Vec<PortMeta> = Vec::new();
    let mut config_types: Vec<syn::Type> = Vec::new();
    let mut outputs: Vec<OutputPortMeta> = Vec::new();
    let mut shader_path: Option<LitStr> = None;
    let mut shader_paths: Vec<LitStr> = Vec::new();
    let mut shader_entry: LitStr = LitStr::new("main", Span::call_site());
    let mut shader_entry_explicit = false;
    let mut shader_workgroup: Option<[u32; 3]> = None;
    let mut shader_workgroup_explicit = false;
    let mut shader_bindings: Vec<TokenStream> = Vec::new();
    let mut shader_specs: Vec<(TokenStream, Option<LitStr>)> = Vec::new();
    let mut state_ty_attr: Option<syn::Type> = None;
    let mut compute_attr: Option<TokenStream> = None;
    let mut sync_groups_attr: Option<TokenStream> = None;
    let mut capability_attr: Option<LitStr> = None;
    let mut fallback_attr: Option<LitStr> = None;
    let mut same_payload_attr = false;

    for arg in args {
        match arg {
            NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. })) => {
                let Some(value) = lit_from_expr(&value) else {
                    return Err(compile_error(
                        "name/value arguments must be literal values".into(),
                    ));
                };
                if path.is_ident("id") {
                    match value {
                        Lit::Str(s) => id = Some(s),
                        _ => return Err(compile_error("id must be a string literal".into())),
                    }
                } else if path.is_ident("summary") {
                    match value {
                        Lit::Str(s) => summary_attr = Some(s),
                        _ => return Err(compile_error("summary must be a string literal".into())),
                    }
                } else if path.is_ident("description") {
                    match value {
                        Lit::Str(s) => description_attr = Some(s),
                        _ => {
                            return Err(compile_error(
                                "description must be a string literal".into(),
                            ));
                        }
                    }
                } else if path.is_ident("bundle") {
                    match value {
                        Lit::Str(_) => {}
                        _ => return Err(compile_error("bundle must be a string literal".into())),
                    }
                } else if path.is_ident("inputs") {
                    match value {
                        Lit::Str(s) => inputs.push(PortMeta::name_only(s)),
                        _ => return Err(compile_error("inputs must be string literals".into())),
                    }
                } else if path.is_ident("outputs") {
                    match value {
                        Lit::Str(s) => outputs.push(OutputPortMeta::name_only(s)),
                        _ => return Err(compile_error("outputs must be string literals".into())),
                    }
                } else if path.is_ident("capability") {
                    match value {
                        Lit::Str(s) => capability_attr = Some(s),
                        _ => {
                            return Err(compile_error(
                                "capability must be a string literal".into(),
                            ));
                        }
                    }
                } else if path.is_ident("shader") {
                    match value {
                        Lit::Str(s) => shader_path = Some(s),
                        _ => return Err(compile_error("shader must be a string literal".into())),
                    }
                } else if path.is_ident("entry") {
                    match value {
                        Lit::Str(s) => {
                            shader_entry = s;
                            shader_entry_explicit = true;
                        }
                        _ => return Err(compile_error("entry must be a string literal".into())),
                    }
                } else if path.is_ident("workgroup_size") {
                    match value {
                        Lit::Int(i) => {
                            shader_workgroup = i.base10_parse::<u32>().ok().map(|v| [v, 1, 1]);
                            shader_workgroup_explicit = true;
                        }
                        _ => {
                            return Err(compile_error(
                                "workgroup_size must be an integer literal".into(),
                            ));
                        }
                    }
                } else if path.is_ident("sync_groups") {
                    sync_groups_attr = Some(value.to_token_stream());
                } else if path.is_ident("compute") {
                    compute_attr = Some(value.to_token_stream());
                } else if path.is_ident("fallback") {
                    match value {
                        Lit::Str(s) => fallback_attr = Some(s),
                        _ => return Err(compile_error("fallback must be a string literal".into())),
                    }
                } else {
                    return Err(compile_error(format!(
                        "unsupported name/value argument: {:?}",
                        path.to_token_stream()
                    )));
                }
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("sync_groups") => {
                sync_groups_attr = Some(list.tokens.clone());
            }
            NestedMeta::Meta(Meta::Path(path)) if path.is_ident("same_payload") => {
                same_payload_attr = true;
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("inputs") => {
                parse_inputs_list(&list, &mut inputs, &mut config_types, data_crate)?;
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("outputs") => {
                parse_outputs_list(&list, &mut outputs)?;
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("compute") => {
                let nested = parse_nested(&list)
                    .map_err(|_| compile_error("compute(...) expects a single argument".into()))?;
                if let Some(first) = nested.first() {
                    compute_attr = Some(first.to_token_stream());
                } else {
                    return Err(compile_error(
                        "compute(...) expects an affinity, e.g., compute(ComputeAffinity::GpuPreferred)"
                            .into(),
                    ));
                }
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("generics") => {
                generics_attr = Some(list.tokens.clone());
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("state") => {
                let nested = parse_nested(&list).map_err(|_| {
                    compile_error("state(...) expects a type, e.g., state(MyState)".into())
                })?;
                if let Some(first) = nested.first() {
                    match syn::parse2::<syn::Type>(first.to_token_stream()) {
                        Ok(ty) => state_ty_attr = Some(ty),
                        Err(_) => {
                            return Err(compile_error(
                                "state(...) expects a type, e.g., state(MyState)".into(),
                            ));
                        }
                    }
                } else {
                    return Err(compile_error(
                        "state(...) expects a type, e.g., state(MyState)".into(),
                    ));
                }
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("bindings") => {
                parse_bindings_list(&list, &mut shader_bindings, gpu_crate)?;
            }
            NestedMeta::Meta(Meta::List(list)) if list.path.is_ident("shaders") => {
                parse_shaders_list(&list, &mut shader_paths, &mut shader_specs)?;
            }
            other => {
                return Err(compile_error(format!(
                    "unsupported argument: {:?}",
                    other.to_token_stream()
                )));
            }
        }
    }

    if (shader_entry_explicit || shader_workgroup_explicit)
        && (shader_path.is_some() || !shader_paths.is_empty() || !shader_specs.is_empty())
    {
        return Err(compile_error(
            "entry/workgroup_size must be specified inside shaders(...) via ShaderSpec { entry: \"...\", workgroup_size: ... }".to_string(),
        ));
    }

    let Some(id) = id else {
        return Err(compile_error("missing required argument `id`".into()));
    };

    Ok(NodeArgs {
        id,
        summary_attr,
        description_attr,
        generics_attr,
        inputs,
        config_types,
        outputs,
        shader_path,
        shader_paths,
        shader_entry,
        shader_workgroup,
        shader_bindings,
        shader_specs,
        state_ty_attr,
        compute_attr,
        sync_groups_attr,
        capability_attr,
        fallback_attr,
        same_payload_attr,
    })
}

fn parse_inputs_list(
    list: &syn::MetaList,
    inputs: &mut Vec<PortMeta>,
    config_types: &mut Vec<syn::Type>,
    data_crate: &TokenStream,
) -> Result<(), TokenStream> {
    let nested_items = parse_nested(list)
        .map_err(|_| compile_error("inputs(...) must use a comma-separated list".into()))?;
    for nested in nested_items {
        if let NestedMeta::Lit(Lit::Str(s)) = nested {
            inputs.push(PortMeta::name_only(s));
            continue;
        }
        if let NestedMeta::Meta(Meta::NameValue(nv)) = &nested
            && nv.path.is_ident("config")
        {
            match syn::parse2::<syn::Type>(nv.value.to_token_stream()) {
                Ok(ty) => {
                    config_types.push(ty);
                    continue;
                }
                Err(_) => {
                    return Err(compile_error(
                        "config must be a type path, e.g. config = MyConfig".into(),
                    ));
                }
            }
        }
        if let NestedMeta::Meta(Meta::List(inner)) = nested
            && inner.path.is_ident("port")
        {
            inputs.push(parse_input_port(&inner, data_crate)?);
            continue;
        }
        return Err(compile_error(
            "inputs list supports \"name\", config = Type, or port(name = \"...\", source = \"...\")"
                .into(),
        ));
    }
    Ok(())
}

fn parse_input_port(
    inner: &syn::MetaList,
    data_crate: &TokenStream,
) -> Result<PortMeta, TokenStream> {
    let inner_items = parse_nested(inner)
        .map_err(|_| compile_error("port(...) expects comma-separated arguments".into()))?;
    let mut name: Option<LitStr> = None;
    let mut source: Option<LitStr> = None;
    let mut default_value: Option<TokenStream> = None;
    let mut ty_override: Option<TokenStream> = None;
    let mut description: Option<LitStr> = None;
    let mut meta_entries: Vec<(LitStr, Lit)> = Vec::new();
    for nm in inner_items {
        let NestedMeta::Meta(Meta::NameValue(nv)) = nm else {
            if let NestedMeta::Meta(Meta::List(list)) = nm
                && (list.path.is_ident("meta") || list.path.is_ident("metadata"))
            {
                parse_meta_entries(&list, &mut meta_entries)?;
                continue;
            }
            continue;
        };
        if nv.path.is_ident("name")
            && let Some(Lit::Str(s)) = lit_from_expr(&nv.value)
        {
            name = Some(s);
            continue;
        }
        if nv.path.is_ident("source")
            && let Some(Lit::Str(s)) = lit_from_expr(&nv.value)
        {
            source = Some(s);
            continue;
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
            return Err(compile_error(
                "port description must be a string literal".into(),
            ));
        }
        if nv.path.is_ident("default") {
            default_value = Some(default_value_tokens(&nv.value, data_crate)?);
        }
    }
    let Some(name) = name else {
        return Err(compile_error(
            "port(...) inside inputs requires name = \"...\"".into(),
        ));
    };
    Ok(PortMeta {
        name,
        source,
        default_value,
        ty_override,
        description,
        meta: meta_entries,
    })
}

fn parse_outputs_list(
    list: &syn::MetaList,
    outputs: &mut Vec<OutputPortMeta>,
) -> Result<(), TokenStream> {
    let nested_items = parse_nested(list)
        .map_err(|_| compile_error("outputs(...) must use a comma-separated list".into()))?;
    for nested in nested_items {
        if let NestedMeta::Lit(Lit::Str(s)) = nested {
            outputs.push(OutputPortMeta::name_only(s));
            continue;
        }
        if let NestedMeta::Meta(Meta::List(inner)) = nested
            && inner.path.is_ident("port")
        {
            outputs.push(parse_output_port(&inner)?);
            continue;
        }
        return Err(compile_error(
            "outputs list supports \"name\" or port(name = \"...\", source = \"...\")".into(),
        ));
    }
    Ok(())
}

fn parse_output_port(inner: &syn::MetaList) -> Result<OutputPortMeta, TokenStream> {
    let inner_items = parse_nested(inner)
        .map_err(|_| compile_error("port(...) expects comma-separated arguments".into()))?;
    let mut name: Option<LitStr> = None;
    let mut source: Option<LitStr> = None;
    let mut ty_override: Option<TokenStream> = None;
    let mut description: Option<LitStr> = None;
    let mut meta_entries: Vec<(LitStr, Lit)> = Vec::new();
    for nm in inner_items {
        if let NestedMeta::Meta(Meta::NameValue(nv)) = &nm {
            if nv.path.is_ident("name")
                && let Some(Lit::Str(s)) = lit_from_expr(&nv.value)
            {
                name = Some(s);
                continue;
            }
            if nv.path.is_ident("source")
                && let Some(Lit::Str(s)) = lit_from_expr(&nv.value)
            {
                source = Some(s);
                continue;
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
                return Err(compile_error(
                    "port description must be a string literal".into(),
                ));
            }
        }
        if let NestedMeta::Meta(Meta::List(list)) = &nm
            && (list.path.is_ident("meta") || list.path.is_ident("metadata"))
        {
            parse_meta_entries(list, &mut meta_entries)?;
        }
    }
    let Some(name) = name else {
        return Err(compile_error(
            "port(...) inside outputs requires name = \"...\"".into(),
        ));
    };
    Ok(OutputPortMeta {
        name,
        source,
        ty_override,
        description,
        meta: meta_entries,
    })
}

fn parse_meta_entries(
    list: &syn::MetaList,
    entries: &mut Vec<(LitStr, Lit)>,
) -> Result<(), TokenStream> {
    let meta_items = parse_nested(list)
        .map_err(|_| compile_error("meta(...) expects comma-separated arguments".into()))?;
    for meta in meta_items {
        let NestedMeta::Meta(Meta::NameValue(nv)) = meta else {
            return Err(compile_error(
                "meta(...) entries must be name/value pairs".into(),
            ));
        };
        let Some(key_ident) = nv.path.get_ident() else {
            return Err(compile_error("meta keys must be simple identifiers".into()));
        };
        let Some(value) = lit_from_expr(&nv.value) else {
            return Err(compile_error("meta values must be literal values".into()));
        };
        entries.push((
            LitStr::new(&key_ident.to_string(), Span::call_site()),
            value,
        ));
    }
    Ok(())
}

fn default_value_tokens(
    value: &syn::Expr,
    data_crate: &TokenStream,
) -> Result<TokenStream, TokenStream> {
    match lit_from_expr(value) {
        Some(Lit::Str(s)) => {
            Ok(quote! { #data_crate::model::Value::String(::std::borrow::Cow::from(#s)) })
        }
        Some(Lit::Int(i)) => {
            let v: i64 = i.base10_parse().unwrap_or(0);
            Ok(quote! { #data_crate::model::Value::Int(#v) })
        }
        Some(Lit::Float(f)) => {
            let v: f64 = f.base10_parse().unwrap_or(0.0);
            Ok(quote! { #data_crate::model::Value::Float(#v) })
        }
        Some(Lit::Bool(b)) => {
            let v = b.value;
            Ok(quote! { #data_crate::model::Value::Bool(#v) })
        }
        _ => Err(compile_error(
            "default must be string/int/float/bool literal".into(),
        )),
    }
}

fn parse_bindings_list(
    list: &syn::MetaList,
    shader_bindings: &mut Vec<TokenStream>,
    gpu_crate: &TokenStream,
) -> Result<(), TokenStream> {
    let parser = syn::punctuated::Punctuated::<BindingEntry, syn::Token![,]>::parse_terminated;
    let parsed = parser
        .parse2(list.tokens.clone())
        .map_err(|_| compile_error("bindings(...) expects a comma-separated list".into()))?;
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
                        return Err(compile_error(format!(
                            "unsupported binding kind `{}` (expected storage, storage_rw, storage_write, or uniform)",
                            other
                        )));
                    }
                };
                shader_bindings.push(quote! { #spec_tokens });
            }
        }
    }
    Ok(())
}

fn parse_shaders_list(
    list: &syn::MetaList,
    shader_paths: &mut Vec<LitStr>,
    shader_specs: &mut Vec<(TokenStream, Option<LitStr>)>,
) -> Result<(), TokenStream> {
    let nested = parse_nested(list)
        .map_err(|_| compile_error("shaders(...) expects a comma-separated list".into()))?;
    for nested in nested {
        match nested {
            NestedMeta::Lit(Lit::Str(s)) => shader_paths.push(s),
            NestedMeta::Meta(meta) => {
                if let Meta::Path(p) = &meta {
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
                    let asset = if let Ok(expr) = syn::parse2::<syn::ExprStruct>(ts.clone()) {
                        expr.fields.iter().find_map(|f| {
                            if let Member::Named(ident) = &f.member
                                && (ident == "asset" || ident == "name")
                                && let syn::Expr::Lit(syn::ExprLit {
                                    lit: Lit::Str(ls), ..
                                }) = &f.expr
                            {
                                return Some(ls.clone());
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
    Ok(())
}

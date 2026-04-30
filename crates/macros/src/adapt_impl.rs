use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::Span;
use quote::quote;
use syn::{
    FnArg, ItemFn, Lit, LitInt, LitStr, Meta, MetaNameValue, ReturnType, Type, parse_macro_input,
};

use crate::helpers::{AttributeArgs, NestedMeta, compile_error, lit_from_expr};

fn crate_path(
    pkg: &str,
    fallback: &str,
    via_root: Option<&str>,
    daedalus_root: &Option<String>,
) -> proc_macro2::TokenStream {
    if let Some(root) = daedalus_root
        && let Some(via) = via_root
    {
        let root_ident = syn::Ident::new(root, Span::call_site());
        let via_ident = syn::Ident::new(via, Span::call_site());
        return quote! { ::#root_ident::#via_ident };
    }
    let name = crate_name(pkg)
        .map(|found| match found {
            FoundCrate::Itself => pkg.replace('-', "_"),
            FoundCrate::Name(name) => name,
        })
        .unwrap_or_else(|_| fallback.to_string());
    let ident = syn::Ident::new(&name, Span::call_site());
    quote! { ::#ident }
}

struct AdaptArgs {
    id: LitStr,
    from: Option<LitStr>,
    to: Option<LitStr>,
    access: Option<syn::Ident>,
    cost: LitInt,
    kind: syn::Ident,
    residency: Option<syn::Ident>,
    layout: Option<LitStr>,
    requires_gpu: bool,
    feature_flags: Vec<LitStr>,
}

fn parse_args(args: AttributeArgs) -> Result<AdaptArgs, proc_macro2::TokenStream> {
    let mut id = None;
    let mut from = None;
    let mut to = None;
    let mut access = None;
    let mut cost = None;
    let mut kind = None;
    let mut residency = None;
    let mut layout = None;
    let mut requires_gpu = false;
    let mut feature_flags = Vec::new();

    for arg in args {
        match arg {
            NestedMeta::Meta(Meta::NameValue(MetaNameValue { path, value, .. })) => {
                if path.is_ident("kind") {
                    kind = Some(adapt_kind_ident_from_expr(&value)?);
                    continue;
                }
                if path.is_ident("access") {
                    access = Some(access_ident_from_expr(&value)?);
                    continue;
                }
                let Some(lit) = lit_from_expr(&value) else {
                    return Err(compile_error(
                        "adapt arguments must be string literals".into(),
                    ));
                };
                if path.is_ident("id") {
                    let Lit::Str(value) = lit else {
                        return Err(compile_error("adapt id must be a string literal".into()));
                    };
                    id = Some(value);
                } else if path.is_ident("from") {
                    let Lit::Str(value) = lit else {
                        return Err(compile_error("adapt from must be a string literal".into()));
                    };
                    from = Some(value);
                } else if path.is_ident("to") {
                    let Lit::Str(value) = lit else {
                        return Err(compile_error("adapt to must be a string literal".into()));
                    };
                    to = Some(value);
                } else if path.is_ident("cost") {
                    let Lit::Int(value) = lit else {
                        return Err(compile_error(
                            "adapt cost must be an integer literal".into(),
                        ));
                    };
                    cost = Some(value);
                } else if path.is_ident("residency") {
                    let Lit::Str(value) = lit else {
                        return Err(compile_error(
                            "adapt residency must be a string literal".into(),
                        ));
                    };
                    residency = Some(residency_ident(&value)?);
                } else if path.is_ident("layout") {
                    let Lit::Str(value) = lit else {
                        return Err(compile_error(
                            "adapt layout must be a string literal".into(),
                        ));
                    };
                    layout = Some(value);
                } else if path.is_ident("requires_gpu") {
                    let Lit::Bool(value) = lit else {
                        return Err(compile_error(
                            "adapt requires_gpu must be a bool literal".into(),
                        ));
                    };
                    requires_gpu = value.value;
                } else if path.is_ident("feature") {
                    let Lit::Str(value) = lit else {
                        return Err(compile_error(
                            "adapt feature must be a string literal".into(),
                        ));
                    };
                    feature_flags.push(value);
                } else if path.is_ident("features") {
                    let Lit::Str(value) = lit else {
                        return Err(compile_error(
                            "adapt features must be a string literal".into(),
                        ));
                    };
                    for feature in value.value().split(',') {
                        let feature = feature.trim();
                        if !feature.is_empty() {
                            feature_flags.push(LitStr::new(feature, value.span()));
                        }
                    }
                } else {
                    return Err(compile_error("unknown adapt argument".into()));
                }
            }
            _ => {
                return Err(compile_error(
                    "adapt arguments must use `id = \"...\", from = \"...\", to = \"...\", cost = 1`".into(),
                ));
            }
        }
    }

    let cost = cost.unwrap_or_else(|| LitInt::new("1", Span::call_site()));
    if let Err(err) = cost.base10_parse::<u64>() {
        return Err(compile_error(format!("adapt cost must fit in u64: {err}")));
    }

    Ok(AdaptArgs {
        id: id.ok_or_else(|| compile_error("missing adapt id".into()))?,
        from,
        to,
        access,
        cost,
        kind: kind.unwrap_or_else(|| syn::Ident::new("Materialize", Span::call_site())),
        residency,
        layout,
        requires_gpu,
        feature_flags,
    })
}

fn access_ident_from_expr(expr: &syn::Expr) -> Result<syn::Ident, proc_macro2::TokenStream> {
    if let Some(Lit::Str(access)) = lit_from_expr(expr) {
        return access_ident_from_str(&access.value(), access.span());
    }
    let syn::Expr::Path(path) = expr else {
        return Err(compile_error(
            "adapt access must be a string literal or enum path like `AccessMode::Read`".into(),
        ));
    };
    let Some(segment) = path.path.segments.last() else {
        return Err(compile_error("adapt access path is empty".into()));
    };
    access_ident_from_str(&segment.ident.to_string(), segment.ident.span())
}

fn access_ident_from_str(raw: &str, span: Span) -> Result<syn::Ident, proc_macro2::TokenStream> {
    let ident = match raw {
        "read" | "Read" => "Read",
        "move" | "Move" => "Move",
        "modify" | "Modify" => "Modify",
        "view" | "View" => "View",
        other => {
            return Err(compile_error(format!(
                "unknown adapt access `{other}`; expected read, move, modify, or view"
            )));
        }
    };
    Ok(syn::Ident::new(ident, span))
}

fn adapt_kind_ident(kind: &LitStr) -> Result<syn::Ident, proc_macro2::TokenStream> {
    adapt_kind_ident_from_str(&kind.value(), kind.span())
}

fn adapt_kind_ident_from_expr(expr: &syn::Expr) -> Result<syn::Ident, proc_macro2::TokenStream> {
    if let Some(Lit::Str(kind)) = lit_from_expr(expr) {
        return adapt_kind_ident(&kind);
    }
    let syn::Expr::Path(path) = expr else {
        return Err(compile_error(
            "adapt kind must be a string literal or enum path like `AdapterKind::View`".into(),
        ));
    };
    let Some(segment) = path.path.segments.last() else {
        return Err(compile_error("adapt kind path is empty".into()));
    };
    adapt_kind_ident_from_str(&segment.ident.to_string(), segment.ident.span())
}

fn adapt_kind_ident_from_str(
    raw: &str,
    span: Span,
) -> Result<syn::Ident, proc_macro2::TokenStream> {
    let ident = match raw {
        "identity" => "Identity",
        "Identity" => "Identity",
        "reinterpret" => "Reinterpret",
        "Reinterpret" => "Reinterpret",
        "view" => "View",
        "View" => "View",
        "shared_view" => "SharedView",
        "SharedView" => "SharedView",
        "cow" => "Cow",
        "Cow" => "Cow",
        "cow_view" => "CowView",
        "CowView" => "CowView",
        "metadata_only" => "MetadataOnly",
        "MetadataOnly" => "MetadataOnly",
        "branch" => "Branch",
        "Branch" => "Branch",
        "mutate_in_place" => "MutateInPlace",
        "MutateInPlace" => "MutateInPlace",
        "materialize" => "Materialize",
        "Materialize" => "Materialize",
        "device_transfer" => "DeviceTransfer",
        "DeviceTransfer" => "DeviceTransfer",
        "device_upload" => "DeviceUpload",
        "DeviceUpload" => "DeviceUpload",
        "device_download" => "DeviceDownload",
        "DeviceDownload" => "DeviceDownload",
        "serialize" => "Serialize",
        "Serialize" => "Serialize",
        "deserialize" => "Deserialize",
        "Deserialize" => "Deserialize",
        "custom" => "Custom",
        "Custom" => "Custom",
        other => {
            return Err(compile_error(format!(
                "unknown adapt kind `{other}`; expected identity, reinterpret, view, shared_view, cow, cow_view, metadata_only, branch, mutate_in_place, materialize, device_transfer, device_upload, device_download, serialize, deserialize, or custom"
            )));
        }
    };
    Ok(syn::Ident::new(ident, span))
}

fn residency_ident(residency: &LitStr) -> Result<syn::Ident, proc_macro2::TokenStream> {
    let ident = match residency.value().as_str() {
        "cpu" => "Cpu",
        "gpu" => "Gpu",
        "cpu_and_gpu" => "CpuAndGpu",
        "external" => "External",
        other => {
            return Err(compile_error(format!(
                "unknown adapt residency `{other}`; expected cpu, gpu, cpu_and_gpu, or external"
            )));
        }
    };
    Ok(syn::Ident::new(ident, residency.span()))
}

fn result_ok_type(output: &ReturnType) -> Option<&Type> {
    let ReturnType::Type(_, ty) = output else {
        return None;
    };
    let Type::Path(path) = ty.as_ref() else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Result" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.first().and_then(|arg| match arg {
        syn::GenericArgument::Type(ty) => Some(ty),
        _ => None,
    })
}

fn is_unit_type(ty: &Type) -> bool {
    matches!(ty, Type::Tuple(tuple) if tuple.elems.is_empty())
}

enum AdaptInput<'a> {
    Owned(&'a Type),
    Ref(&'a Type),
    Mut(&'a Type),
    Arc(&'a Type),
}

#[derive(Clone, Copy)]
enum AdaptOutput<'a> {
    Owned,
    Arc(&'a Type),
    Unit,
}

fn arc_inner_type(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Arc" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.first().and_then(|arg| match arg {
        syn::GenericArgument::Type(ty) => Some(ty),
        _ => None,
    })
}

fn classify_input(ty: &Type) -> Option<AdaptInput<'_>> {
    if let Type::Reference(reference) = ty {
        if reference.mutability.is_some() {
            return Some(AdaptInput::Mut(reference.elem.as_ref()));
        }
        return Some(AdaptInput::Ref(reference.elem.as_ref()));
    }
    if let Some(inner) = arc_inner_type(ty) {
        return Some(AdaptInput::Arc(inner));
    }
    Some(AdaptInput::Owned(ty))
}

fn classify_output(ty: &Type) -> AdaptOutput<'_> {
    if is_unit_type(ty) {
        return AdaptOutput::Unit;
    }
    if let Some(inner) = arc_inner_type(ty) {
        return AdaptOutput::Arc(inner);
    }
    AdaptOutput::Owned
}

pub fn adapt(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args with AttributeArgs::parse_terminated);
    let input = parse_macro_input!(item as ItemFn);

    let parsed = match parse_args(args) {
        Ok(parsed) => parsed,
        Err(err) => return err.into(),
    };

    if !input.sig.generics.params.is_empty() {
        return compile_error("adapt functions cannot be generic yet".into()).into();
    }

    let daedalus_root: Option<String> = crate_name("daedalus-rs")
        .or_else(|_| crate_name("daedalus"))
        .ok()
        .map(|found| match found {
            FoundCrate::Itself => "daedalus".to_string(),
            FoundCrate::Name(name) => {
                if name == "daedalus_rs" {
                    "daedalus".to_string()
                } else {
                    name
                }
            }
        });
    let runtime_crate = crate_path(
        "daedalus-runtime",
        "daedalus_runtime",
        Some("runtime"),
        &daedalus_root,
    );
    let data_crate = crate_path(
        "daedalus-data",
        "daedalus_data",
        Some("data"),
        &daedalus_root,
    );
    let transport_crate = crate_path(
        "daedalus-transport",
        "daedalus_transport",
        Some("transport"),
        &daedalus_root,
    );

    let fn_ident = &input.sig.ident;
    let register_ident = syn::Ident::new(&format!("register_{fn_ident}_adapter"), fn_ident.span());
    let vis = &input.vis;
    let id = parsed.id;
    let cost = parsed.cost;
    let kind = parsed.kind;
    let access_override = parsed.access;
    let residency = parsed.residency;
    let layout = parsed.layout;
    let requires_gpu = parsed.requires_gpu;
    let feature_flags = parsed.feature_flags;

    let mut typed_args = input.sig.inputs.iter().filter_map(|arg| match arg {
        FnArg::Typed(pat) => Some(pat),
        FnArg::Receiver(_) => None,
    });
    let Some(arg) = typed_args.next() else {
        return compile_error("adapt functions must take exactly one typed argument".into()).into();
    };
    if typed_args.next().is_some() {
        return compile_error("adapt functions must take exactly one typed argument".into()).into();
    }

    let Some(input_kind) = classify_input(arg.ty.as_ref()) else {
        return compile_error(
            "adapt currently supports `T`, `&T`, `&mut T`, and `Arc<T>` inputs".into(),
        )
        .into();
    };
    let Some(output_ty) = result_ok_type(&input.sig.output) else {
        return compile_error(
            "adapt functions must return `Result<T, daedalus::transport::TransportError>`".into(),
        )
        .into();
    };
    let output_kind = classify_output(output_ty);
    let inferred_access = match (&input_kind, &output_kind) {
        (AdaptInput::Mut(_), _) => syn::Ident::new("Modify", Span::call_site()),
        (AdaptInput::Ref(_) | AdaptInput::Arc(_), _) if kind == "Branch" => {
            syn::Ident::new("Modify", Span::call_site())
        }
        (_, AdaptOutput::Owned) => syn::Ident::new("Move", Span::call_site()),
        (AdaptInput::Owned(_), _) => syn::Ident::new("Move", Span::call_site()),
        (_, AdaptOutput::Arc(_) | AdaptOutput::Unit) => syn::Ident::new("Read", Span::call_site()),
    };
    let access = access_override.unwrap_or(inferred_access);
    let inferred_from_ty = match &input_kind {
        AdaptInput::Owned(ty) | AdaptInput::Ref(ty) | AdaptInput::Mut(ty) | AdaptInput::Arc(ty) => {
            *ty
        }
    };
    let inferred_to_ty = match output_kind {
        AdaptOutput::Owned => output_ty,
        AdaptOutput::Arc(ty) => ty,
        AdaptOutput::Unit => inferred_from_ty,
    };
    let from_key = parsed
        .from
        .as_ref()
        .map(|from| quote! { ::std::string::String::from(#from) })
        .unwrap_or_else(|| {
            quote! {
                #runtime_crate::transport::typeexpr_transport_key(
                    &#data_crate::typing::type_expr::<#inferred_from_ty>()
                )
                .map(|__key| __key.to_string())
                .unwrap_or_else(|_| ::std::string::String::from(::core::any::type_name::<#inferred_from_ty>()))
            }
        });
    let to_key = parsed
        .to
        .as_ref()
        .map(|to| quote! { ::std::string::String::from(#to) })
        .unwrap_or_else(|| {
            quote! {
                #runtime_crate::transport::typeexpr_transport_key(
                    &#data_crate::typing::type_expr::<#inferred_to_ty>()
                )
                .map(|__key| __key.to_string())
                .unwrap_or_else(|_| ::std::string::String::from(::core::any::type_name::<#inferred_to_ty>()))
            }
        });
    let residency_option = residency
        .map(|residency| {
            quote! {
                __options.residency = Some(#transport_crate::Residency::#residency);
            }
        })
        .unwrap_or_default();
    let layout_option = layout
        .map(|layout| {
            quote! {
                __options.layout = Some(#transport_crate::Layout::new(#layout));
            }
        })
        .unwrap_or_default();
    let adapter_options = quote! {{
        let mut __options = #runtime_crate::plugins::TransportAdapterOptions::default();
        __options.cost.cpu_ns = (#cost as u64).min(::core::u32::MAX as u64) as u32;
        __options.cost.kind = #transport_crate::AdaptKind::#kind;
        __options.access = #transport_crate::AccessMode::#access;
        #residency_option
        #layout_option
        __options.requires_gpu = #requires_gpu;
        #(
            __options.feature_flags.push(#feature_flags.to_string());
        )*
        __options
    }};
    let register_body = match input_kind {
        AdaptInput::Mut(input_ty) => {
            if !matches!(output_kind, AdaptOutput::Unit) {
                return compile_error(
                    "`&mut T` adapt functions must return `Result<(), _>`".into(),
                )
                .into();
            }
            quote! {
                into.register_transport_adapter_fn_with_options(
                    #id,
                    #data_crate::model::TypeExpr::opaque(#from_key),
                    #data_crate::model::TypeExpr::opaque(#to_key),
                    #adapter_options,
                    |mut payload, _request| {
                        let __found = payload.type_key().clone();
                        let __value = payload.get_mut::<#input_ty>().ok_or_else(|| {
                            #transport_crate::TransportError::TypeMismatch {
                                expected: #transport_crate::TypeKey::new(#from_key),
                                found: __found,
                            }
                        })?;
                        #fn_ident(__value)?;
                        Ok(payload)
                    },
                )
            }
        }
        AdaptInput::Ref(input_ty) => match output_kind {
            AdaptOutput::Owned | AdaptOutput::Unit => {
                quote! {
                    into.register_transport_adapter_fn_with_options(
                        #id,
                        #data_crate::model::TypeExpr::opaque(#from_key),
                        #data_crate::model::TypeExpr::opaque(#to_key),
                        #adapter_options,
                        |payload, _request| {
                            let __found = payload.type_key().clone();
                            let __value = payload
                                .get_ref::<#input_ty>()
                                .ok_or_else(|| {
                                #transport_crate::TransportError::TypeMismatch {
                                    expected: #transport_crate::TypeKey::new(#from_key),
                                    found: __found,
                                }
                            })?;
                            let __output = #fn_ident(__value)?;
                            Ok(#transport_crate::Payload::owned(#transport_crate::TypeKey::new(#to_key), __output))
                        },
                    )
                }
            }
            AdaptOutput::Arc(_output_inner) => {
                quote! {
                        into.register_transport_adapter_fn_with_options(
                            #id,
                            #data_crate::model::TypeExpr::opaque(#from_key),
                            #data_crate::model::TypeExpr::opaque(#to_key),
                            #adapter_options,
                            |payload, _request| {
                            let __found = payload.type_key().clone();
                            let __value = payload
                                .get_ref::<#input_ty>()
                                .ok_or_else(|| {
                                #transport_crate::TransportError::TypeMismatch {
                                    expected: #transport_crate::TypeKey::new(#from_key),
                                    found: __found,
                                }
                            })?;
                            let __output = #fn_ident(__value)?;
                            Ok(#transport_crate::Payload::shared(#transport_crate::TypeKey::new(#to_key), __output))
                        },
                    )
                }
            }
        },
        AdaptInput::Owned(input_ty) => {
            let output_payload = match output_kind {
                AdaptOutput::Arc(_output_inner) => {
                    quote! { #transport_crate::Payload::shared(#transport_crate::TypeKey::new(#to_key), __output) }
                }
                AdaptOutput::Owned | AdaptOutput::Unit => {
                    quote! { #transport_crate::Payload::owned(#transport_crate::TypeKey::new(#to_key), __output) }
                }
            };
            quote! {
                into.register_transport_adapter_fn_with_options(
                    #id,
                    #data_crate::model::TypeExpr::opaque(#from_key),
                    #data_crate::model::TypeExpr::opaque(#to_key),
                    #adapter_options,
                    |payload, _request| {
                        let __found = payload.type_key().clone();
                        let __value = payload
                            .get_arc::<#input_ty>()
                            .ok_or_else(|| {
                            #transport_crate::TransportError::TypeMismatch {
                                expected: #transport_crate::TypeKey::new(#from_key),
                                found: __found,
                            }
                        })?;
                        ::core::mem::drop(payload);
                        let __value = ::std::sync::Arc::try_unwrap(__value).map_err(|_| {
                            #transport_crate::TransportError::Unsupported(
                                ::std::format!(
                                    "owned adapter {} requires a unique payload",
                                    #id,
                                ),
                            )
                        })?;
                        let __output = #fn_ident(__value)?;
                        Ok(#output_payload)
                    },
                )
            }
        }
        AdaptInput::Arc(input_ty) => {
            let output_payload = match output_kind {
                AdaptOutput::Arc(_output_inner) => {
                    quote! { #transport_crate::Payload::shared(#transport_crate::TypeKey::new(#to_key), __output) }
                }
                AdaptOutput::Owned | AdaptOutput::Unit => {
                    quote! { #transport_crate::Payload::owned(#transport_crate::TypeKey::new(#to_key), __output) }
                }
            };
            quote! {
                into.register_transport_adapter_fn_with_options(
                    #id,
                    #data_crate::model::TypeExpr::opaque(#from_key),
                    #data_crate::model::TypeExpr::opaque(#to_key),
                    #adapter_options,
                    |payload, _request| {
                        let __found = payload.type_key().clone();
                        let __value = payload
                            .get_arc::<#input_ty>()
                            .ok_or_else(|| {
                            #transport_crate::TransportError::TypeMismatch {
                                expected: #transport_crate::TypeKey::new(#from_key),
                                found: __found,
                            }
                        })?;
                        let __output = #fn_ident(__value)?;
                        Ok(#output_payload)
                    },
                )
            }
        }
    };

    let expanded = quote! {
        #input

        #vis fn #register_ident(
            into: &mut #runtime_crate::plugins::PluginRegistry,
        ) -> #runtime_crate::plugins::PluginResult<()> {
            #register_body
        }
    };

    expanded.into()
}

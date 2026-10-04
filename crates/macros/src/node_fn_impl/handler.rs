use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{ItemFn, LitStr};

use crate::helpers::{arc_inner_type, compile_error, is_unit_type, last_segment, strip_ref};

use super::descriptor::is_fanin_ty;
use super::handler_fetch;
use super::parse::PortMeta;
use super::shader;
use super::type_analysis::{ok_type_from_return, payload_inner_type};
use crate::type_expr::value_type_key;

pub(super) struct GraphCtxArg {
    pub(super) ident: syn::Ident,
    pub(super) is_mut_ref: bool,
}

pub(super) struct HandlerInputs<'a> {
    pub(super) is_low_level: bool,
    pub(super) input: &'a ItemFn,
    pub(super) inputs_vec: &'a [PortMeta],
    pub(super) outputs_len: usize,
    pub(super) output_names: &'a [LitStr],
    /// Explicit `type_key` per output port.
    pub(super) output_type_keys: &'a [Option<LitStr>],
    pub(super) output_idents: &'a [syn::Ident],
    pub(super) config_types: &'a [syn::Type],
    pub(super) shader_path: Option<&'a LitStr>,
    pub(super) shader_paths: &'a [LitStr],
    pub(super) shader_entry: &'a LitStr,
    pub(super) shader_workgroup: Option<[u32; 3]>,
    pub(super) shader_bindings: &'a [TokenStream],
    pub(super) shader_specs: &'a [(TokenStream, Option<LitStr>)],
    pub(super) state_ty_attr: Option<&'a syn::Type>,
    pub(super) capability_attr: Option<&'a LitStr>,
    pub(super) inner_fn_ident: &'a syn::Ident,
    pub(super) runtime_crate: &'a TokenStream,
    pub(super) gpu_crate: &'a TokenStream,
}

pub(super) struct HandlerBuild {
    pub(super) handler_body: TokenStream,
    /// Key expressions of the pushed outputs, evaluated once per handler registry (with
    /// `__types` in scope) into the `__output_keys` array the handler body indexes.
    pub(super) output_keys: Vec<TokenStream>,
    pub(super) effective_inputs_for_args: Vec<PortMeta>,
    pub(super) arg_types: Vec<syn::Type>,
    pub(super) arg_idents: Vec<syn::Ident>,
    pub(super) arg_mut_bindings: Vec<bool>,
    pub(super) graph_ctx_arg: Option<GraphCtxArg>,
    pub(super) runtime_node_present: bool,
    pub(super) exec_ctx_present: bool,
    pub(super) node_io_present: bool,
    pub(super) shader_ctx_present: bool,
}

pub(super) fn build_handler(inputs: HandlerInputs<'_>) -> Result<HandlerBuild, TokenStream> {
    let HandlerInputs {
        is_low_level,
        input,
        inputs_vec,
        outputs_len,
        output_names,
        output_type_keys,
        output_idents,
        config_types,
        shader_path,
        shader_paths,
        shader_entry,
        shader_workgroup,
        shader_bindings,
        shader_specs,
        state_ty_attr,
        capability_attr,
        inner_fn_ident,
        runtime_crate,
        gpu_crate,
    } = inputs;
    // Captured so we can reuse when building descriptors.
    let mut arg_types: Vec<syn::Type> = Vec::new();
    let mut arg_idents: Vec<syn::Ident> = Vec::new();
    let mut arg_names: Vec<LitStr> = Vec::new();
    let mut arg_mut_bindings: Vec<bool> = Vec::new();
    let mut effective_inputs_for_args: Vec<PortMeta> = Vec::new();

    let mut graph_ctx_arg: Option<GraphCtxArg> = None;
    let mut runtime_node_present = false;
    let mut exec_ctx_present = false;
    let mut node_io_present = false;
    let mut shader_ctx_present = false;
    let mut output_keys: Vec<TokenStream> = Vec::new();

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
        let state_ty = state_ty_attr.cloned();
        let mut state_param: Option<syn::Ident> = None;
        for arg in &input.sig.inputs {
            if let syn::FnArg::Typed(pat) = arg
                && let syn::Pat::Ident(id) = &*pat.pat
            {
                let last_ident = last_segment(strip_ref(&pat.ty)).map(|s| s.ident.to_string());
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
                    Some(
                        "NodeIo"
                            | "RuntimeNode"
                            | "ExecutionContext"
                            | "ShaderContext"
                            | "GraphCtx"
                    )
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
        if state_ty.is_some() && state_param.is_none() {
            return Err(compile_error(
                "state(...) specified but no matching state parameter found in signature".into(),
            ));
        }

        if shader_ctx_ident.is_some() && !has_shader_metadata {
            return Err(compile_error(
                "ShaderContext parameter requires shader metadata (missing shaders(...))".into(),
            ));
        }

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
            inputs_vec.to_vec()
        } else if has_fanin {
            return Err(compile_error(
                "FanIn params require inputs(...) entries for all typed args (include the FanIn prefix).".into(),
            ));
        } else {
            arg_names.iter().cloned().map(PortMeta::name_only).collect()
        };

        let mut call_args: Vec<proc_macro2::TokenStream> = Vec::new();
        for arg in &input.sig.inputs {
            if let syn::FnArg::Typed(pat) = arg {
                let is_shader_ctx_param = match &*pat.ty {
                    syn::Type::Path(tp) => tp
                        .path
                        .segments
                        .last()
                        .map(|s| s.ident == "ShaderContext")
                        .unwrap_or(false),
                    syn::Type::Reference(r) => match &*r.elem {
                        syn::Type::Path(tp) => tp
                            .path
                            .segments
                            .last()
                            .map(|s| s.ident == "ShaderContext")
                            .unwrap_or(false),
                        _ => false,
                    },
                    _ => false,
                };
                if is_shader_ctx_param && shader_ctx_ident.is_none() {
                    continue;
                }
                if let syn::Pat::Ident(id) = &*pat.pat {
                    let ident = &id.ident;
                    if let Some(n) = &runtime_node_ident
                        && ident == n
                    {
                        call_args.push(quote! { node });
                        continue;
                    }
                    if let Some(c) = &exec_ctx_ident
                        && ident == c
                    {
                        call_args.push(quote! { ctx });
                        continue;
                    }
                    if let Some(ioid) = &node_io_ident
                        && ident == ioid
                    {
                        call_args.push(quote! { io });
                        continue;
                    }
                    if let Some(ctx) = &shader_ctx_ident
                        && ident == ctx
                    {
                        call_args.push(quote! { __shader_ctx });
                        continue;
                    }
                    if let Some(st) = &state_param
                        && ident == st
                    {
                        call_args.push(quote! { #ident });
                        continue;
                    }
                    call_args.push(quote! { #ident });
                }
            }
        }

        let shader_tokens = shader::shader_tokens(
            shader_specs,
            shader_path,
            shader_entry,
            shader_workgroup,
            shader_bindings,
            shader_paths,
            gpu_crate,
        );

        let call = quote! { #inner_fn_ident(#(#call_args),*) };
        let out_port = output_names
            .first()
            .cloned()
            .unwrap_or_else(|| LitStr::new("out", Span::call_site()));

        let port_names: Vec<LitStr> = effective_inputs_for_args
            .iter()
            .map(|p| p.name.clone())
            .collect();

        let ret_handling = if node_io_ident.is_some() {
            let ok_ty = ok_type_from_return(&input.sig.output);
            if outputs_len > 0 && ok_ty.is_some_and(|t| !is_unit_type(t)) {
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
            if outputs_len > 1 {
                let out_ports = output_names;
                let out_idents = output_idents;
                let out_push_stmts: Vec<proc_macro2::TokenStream> = match ok_ty {
                    Some(syn::Type::Tuple(tuple)) => tuple
                        .elems
                        .iter()
                        .zip(out_ports.iter())
                        .zip(out_idents.iter())
                        .zip(output_type_keys.iter())
                        .map(|(((elem_ty, port), ident), key)| {
                            if let Some(inner) = payload_inner_type(elem_ty) {
                                quote! { io.push_compute::<#inner>(Some(#port), #ident); }
                            } else {
                                push_output(
                                    elem_ty,
                                    port,
                                    key.as_ref(),
                                    quote! { #ident },
                                    &mut output_keys,
                                )
                            }
                        })
                        .collect(),
                    _ => out_ports
                        .iter()
                        .zip(out_idents.iter())
                        .map(|(port, ident)| quote! {
                            {
                                let __key = #runtime_crate::transport_types::TypeKey::from_static("rust:unknown");
                                io.push_as_to(#port, __key, #ident);
                            }
                        })
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
                    .as_ref()
                    .and_then(|ty| payload_inner_type(ty))
                    .map(|inner| {
                        quote! { io.push_compute::<#inner>(Some(#out_port), val); }
                    })
                    .unwrap_or_else(|| {
                        if let Some(ok_ty) = ok_ty.as_ref() {
                            let key = output_type_keys.first().and_then(Option::as_ref);
                            push_output(ok_ty, &out_port, key, quote! { val }, &mut output_keys)
                        } else {
                            quote! {
                                {
                                    let __key = #runtime_crate::transport_types::TypeKey::from_static("rust:unknown");
                                    io.push_as_to(#out_port, __key, val);
                                }
                            }
                        }
                    });
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

        let state_binding = if let (Some(sty), Some(id)) = (state_ty.clone(), state_param.clone()) {
            Some(quote! {
                let mut __state_value: #sty = ctx.state
                    .take_node_state::<#sty>(&ctx.node_id)
                    .unwrap_or_default();
                let #id: &mut #sty = &mut __state_value;
            })
        } else {
            None
        };
        let ret_handling = if state_binding.is_some() {
            quote! {
                let __state_result = { #ret_handling };
                ctx.state.set_node_state(&ctx.node_id, __state_value);
                __state_result
            }
        } else {
            ret_handling
        };

        if let Some(cap_str) = capability_attr.cloned() {
            // Capability entries push their own payloads; the typed returns are unused.
            output_keys.clear();
            let cap_lit = cap_str;
            let port_idents: Vec<LitStr> = port_names.clone();
            quote! {
                let mut args_any: Vec<&dyn ::std::any::Any> = Vec::new();
                #(args_any.push(
                    io.payload_raw(#port_idents)
                        .ok_or_else(|| #runtime_crate::NodeError::InvalidInput(format!("missing {}", #port_idents)))?
                );)*
                {
                    let entries = ctx.capabilities
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
                            io.push_payload(#out_port, out);
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
            // Decoded configs and `&T` constants live in the node's state slot between calls
            // (`daedalus_runtime::const_cache`) and are decoded again only when an input changes.
            let mut config_fetch_stmts: Vec<proc_macro2::TokenStream> = Vec::new();
            let mut cache_restores: Vec<proc_macro2::TokenStream> = Vec::new();
            let mut take_cache = |ident: &syn::Ident, cache_ty: TokenStream| {
                cache_restores.push(quote! { ctx.state.set_node_state(&ctx.node_id, #ident); });
                quote! {
                    let mut #ident = ctx.state.take_node_state::<#cache_ty>(&ctx.node_id).unwrap_or_default();
                }
            };
            for (idx, cfg) in config_args.iter().enumerate() {
                let ident = &cfg.ident;
                let ty = &cfg.ty;
                let cache_ident = syn::Ident::new(&format!("__cfg_cache_{idx}"), Span::call_site());
                let owned_ident = syn::Ident::new(&format!("__cfg_owned_{idx}"), Span::call_site());
                let ref_ident = syn::Ident::new(&format!("__cfg_ref_{idx}"), Span::call_site());
                let take = take_cache(
                    &cache_ident,
                    quote! { #runtime_crate::const_cache::ConfigCache<#ty> },
                );
                let assign = match (cfg.is_ref, cfg.is_mut) {
                    (true, false) => quote! { let #ident = #ref_ident; },
                    (true, true) => quote! {
                        let mut #owned_ident = ::core::clone::Clone::clone(#ref_ident);
                        let #ident = &mut #owned_ident;
                    },
                    (false, _) => quote! { let #ident = ::core::clone::Clone::clone(#ref_ident); },
                };
                config_fetch_stmts.push(quote! {
                    #take
                    let #ref_ident: &#ty = #cache_ident.get(io, &node.id)?;
                    #assign
                });
            }
            let fetch = handler_fetch::input_fetch_stmts(handler_fetch::FetchInputs {
                arg_idents: &arg_idents,
                arg_types: &arg_types,
                arg_mut_bindings: &arg_mut_bindings,
                port_names: &port_names,
                runtime_crate,
            });
            let decoded_take = (!fetch.decode.is_empty()).then(|| {
                take_cache(
                    &syn::Ident::new(handler_fetch::DECODED, Span::call_site()),
                    quote! { #runtime_crate::const_cache::DecodedInputs },
                )
            });
            let (arg_fetch_mut_stmts, arg_decode_stmts, arg_fetch_ref_stmts) =
                (fetch.mutable, fetch.decode, fetch.borrowed);
            let ret_handling = if cache_restores.is_empty() {
                ret_handling
            } else {
                quote! {
                    let __result = { #ret_handling };
                    #(#cache_restores)*
                    __result
                }
            };

            let shader_gpu_init = if shader_tokens.is_some() {
                quote! { let __ctx_gpu: Option<#gpu_crate::GpuContextHandle> = ctx.gpu.clone(); }
            } else {
                quote! {}
            };

            quote! {
                #(#config_fetch_stmts)*
                #(#arg_fetch_mut_stmts)*
                #decoded_take
                #(#arg_decode_stmts)*
                #(#arg_fetch_ref_stmts)*
                #shader_gpu_init
                #state_binding
                #shader_tokens
                #ret_handling
            }
        }
    };

    Ok(HandlerBuild {
        handler_body,
        output_keys,
        effective_inputs_for_args,
        arg_types,
        arg_idents,
        arg_mut_bindings,
        graph_ctx_arg,
        runtime_node_present,
        exec_ctx_present,
        node_io_present,
        shader_ctx_present,
    })
}

/// Push `value` (of type `ty`, `Arc` of it, or `Option` of either) to `port` under its explicit
/// or leaf type key, which is appended to `keys` and read from `__output_keys` (computed once,
/// not per push).
fn push_output(
    ty: &syn::Type,
    port: &LitStr,
    explicit: Option<&LitStr>,
    value: TokenStream,
    keys: &mut Vec<TokenStream>,
) -> TokenStream {
    // A conditional output: nothing is pushed for `None`.
    if let Some(inner) = crate::helpers::generic_arg(ty, "Option", 0) {
        let push = push_output(inner, port, explicit, quote! { __value }, keys);
        return quote! { if let Some(__value) = #value { #push } };
    }
    let idx = keys.len();
    let key = quote! { __output_keys[#idx].clone() };
    match arc_inner_type(ty) {
        Some(inner) => {
            keys.push(value_type_key(inner, explicit));
            quote! { io.push_arc_as_to(#port, #key, #value); }
        }
        None => {
            keys.push(value_type_key(ty, explicit));
            quote! { io.push_as_to(#port, #key, #value); }
        }
    }
}

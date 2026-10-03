use std::collections::HashSet;

use proc_macro2::TokenStream;
use quote::ToTokens;

use crate::helpers::{generic_arg, last_segment, segment_type_arg, strip_ref};
use crate::type_expr::{GenericParams, TypeExprOptions};

/// Schema expression for a node port type. Generic parameters become `Opaque("generic")`
/// and residency wrappers (`Cpu<T>`, `Gpu<T>`, `Device<Cpu, T>`) encode their payload type.
pub(super) fn node_type_expr(
    ty: &syn::Type,
    generic_type_params: &HashSet<String>,
    data_crate: &TokenStream,
) -> TokenStream {
    TypeExprOptions {
        data_crate,
        generics: GenericParams::Opaque(generic_type_params),
        transparent: &[("Cpu", 0), ("Gpu", 0), ("Device", 1), ("Result", 0)],
        str_as_string: true,
        arrays_as_lists: false,
    }
    .type_expr(ty)
}

/// The success type of a return type: `T` for `Result<T, _>`, the type itself otherwise.
pub(super) fn ok_type_from_return(ret: &syn::ReturnType) -> Option<&syn::Type> {
    let syn::ReturnType::Type(_, ty) = ret else {
        return None;
    };
    match last_segment(ty) {
        Some(seg) if seg.ident == "Result" => segment_type_arg(seg, 0),
        _ => Some(ty),
    }
}

pub(super) fn payload_inner_type(ty: &syn::Type) -> Option<&syn::Type> {
    generic_arg(ty, "Compute", 0)
}

/// Strip any nesting of `Result<_, _>` / `Option<_>` wrappers.
pub(super) fn peel_result_or_option(ty: &syn::Type) -> &syn::Type {
    match generic_arg(ty, "Result", 0).or_else(|| generic_arg(ty, "Option", 0)) {
        Some(inner) => peel_result_or_option(inner),
        None => ty,
    }
}

pub(super) fn contract_type_for(ty: &syn::Type) -> Option<&syn::Type> {
    let ty = strip_ref(ty);
    let ident = last_segment(ty)?.ident.to_string();
    if matches!(
        ident.as_str(),
        "FanIn"
            | "Payload"
            | "CorrelatedPayload"
            | "NodeIo"
            | "RuntimeNode"
            | "ExecutionContext"
            | "ShaderContext"
            | "GraphCtx"
    ) {
        return None;
    }
    Some(ty)
}

pub(super) fn output_contract_types(ret: &syn::ReturnType, outputs_len: usize) -> Vec<&syn::Type> {
    let syn::ReturnType::Type(_, ty) = ret else {
        return Vec::new();
    };
    let base = peel_result_or_option(ty);
    if let syn::Type::Tuple(tuple) = base
        && tuple.elems.len() == outputs_len
    {
        return tuple.elems.iter().collect();
    }
    if outputs_len == 1 {
        return vec![base];
    }
    Vec::new()
}

pub(super) fn direct_payload_plain_type(ty: &syn::Type) -> Option<&syn::Type> {
    let ident = last_segment(ty)?.ident.to_string();
    (!matches!(ident.as_str(), "Arc" | "Compute" | "Option")).then_some(ty)
}

pub(super) fn direct_payload_same_type(left: &syn::Type, right: &syn::Type) -> bool {
    let mut left = left.to_token_stream().to_string();
    left.retain(|c| !c.is_whitespace());
    let mut right = right.to_token_stream().to_string();
    right.retain(|c| !c.is_whitespace());
    left == right
}

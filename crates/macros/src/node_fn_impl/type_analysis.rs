use std::collections::HashSet;

use proc_macro2::TokenStream;
use quote::ToTokens;

use crate::helpers::{generic_arg, last_segment, segment_type_arg, strip_ref};
use crate::type_expr::{GenericParams, TypeExprOptions};

/// Schema expression for a node port type. Generic parameters become `Opaque("generic")`
/// and handle/residency wrappers (`Arc<T>`, `Cpu<T>`, `Gpu<T>`, `Device<Cpu, T>`) encode their
/// payload type.
pub(super) fn node_type_expr(
    ty: &syn::Type,
    generic_type_params: &HashSet<String>,
    data_crate: &TokenStream,
) -> TokenStream {
    node_type_options(generic_type_params, data_crate).type_expr(ty)
}

/// The leaf types [`node_type_expr`] resolves by key (everything but containers, tuples,
/// references, wrappers and generic parameters).
pub(super) fn node_type_leaves(
    ty: &syn::Type,
    generic_type_params: &HashSet<String>,
    data_crate: &TokenStream,
) -> Vec<syn::Type> {
    let mut leaves = Vec::new();
    node_type_options(generic_type_params, data_crate).type_expr_collecting(ty, &mut leaves);
    leaves
}

fn node_type_options<'a>(
    generic_type_params: &'a HashSet<String>,
    data_crate: &'a TokenStream,
) -> TypeExprOptions<'a> {
    TypeExprOptions {
        data_crate,
        generics: GenericParams::Opaque(generic_type_params),
        transparent: PAYLOAD_WRAPPERS,
        str_as_string: true,
        arrays_as_lists: false,
    }
}

/// Wrappers whose type argument (at the given index) is the payload value type.
const PAYLOAD_WRAPPERS: &[(&str, usize)] = &[
    ("Arc", 0),
    ("Cpu", 0),
    ("Gpu", 0),
    ("Device", 1),
    ("Result", 0),
];

/// The value type a port parameter carries: references, `Option` and [`PAYLOAD_WRAPPERS`]
/// peeled.
pub(super) fn payload_value_type(ty: &syn::Type) -> &syn::Type {
    let ty = strip_ref(ty);
    let inner = std::iter::once(("Option", 0))
        .chain(PAYLOAD_WRAPPERS.iter().copied())
        .find_map(|(name, idx)| generic_arg(ty, name, idx));
    inner.map_or(ty, payload_value_type)
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
    // Foreign views carry an interface, not a Rust type (see `port_types`).
    if crate::foreign_type::is_foreign_view(ty) {
        return None;
    }
    let ty = strip_ref(ty);
    // An optional input carries its inner type.
    if let Some(inner) = generic_arg(ty, "Option", 0) {
        return contract_type_for(inner);
    }
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

/// Per output, whether it is conditional: an `Option` return (or tuple element) pushes nothing
/// for `None`.
pub(super) fn conditional_outputs(ret: &syn::ReturnType, outputs_len: usize) -> Vec<bool> {
    let syn::ReturnType::Type(_, ty) = ret else {
        return vec![false; outputs_len];
    };
    let mut ty: &syn::Type = ty;
    while let Some(inner) = generic_arg(ty, "Result", 0) {
        ty = inner;
    }
    if generic_arg(ty, "Option", 0).is_some() {
        return vec![true; outputs_len];
    }
    match ty {
        syn::Type::Tuple(tuple) if tuple.elems.len() == outputs_len => tuple
            .elems
            .iter()
            .map(|elem| generic_arg(elem, "Option", 0).is_some())
            .collect(),
        _ => vec![false; outputs_len],
    }
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

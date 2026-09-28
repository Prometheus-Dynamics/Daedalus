//! `TypeExpr` token generation shared by the node, `NodeConfig`, and `DaedalusTypeExpr` macros.
//!
//! Prefers explicit runtime overrides (typing registry) for containers, encodes common
//! containers and primitives structurally, and resolves every other type through
//! `typing::type_expr::<T>()`. Types that cannot be encoded fall back to a stable opaque
//! `rust:<type>` identity.

use std::collections::HashSet;

use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, quote};
use syn::{LitStr, Type};

use crate::helpers::{last_segment, segment_type_arg};

/// How bare generic type parameters of the annotated item are encoded.
#[derive(Clone, Copy)]
pub(crate) enum GenericParams<'a> {
    /// The item has no generic type parameters to treat specially.
    None,
    /// Generic parameters have no structural encoding (they get the opaque `rust:<T>` fallback).
    Fallback(&'a HashSet<String>),
    /// Generic parameters are encoded as `TypeExpr::Opaque("generic")`.
    Opaque(&'a HashSet<String>),
}

pub(crate) struct TypeExprOptions<'a> {
    pub data_crate: &'a TokenStream,
    pub generics: GenericParams<'a>,
    /// Wrappers encoded as their type argument at the given index, e.g. `("Arc", 0)`.
    pub transparent: &'a [(&'a str, usize)],
    /// Encode `&str` as `Scalar(String)` instead of resolving `str` through the typing registry.
    pub str_as_string: bool,
    /// Encode `[T; N]` as a list of `T`.
    pub arrays_as_lists: bool,
}

impl TypeExprOptions<'_> {
    /// Schema expression for `ty`, falling back to an opaque `rust:<type>` identity.
    pub(crate) fn type_expr(&self, ty: &Type) -> TokenStream {
        self.type_expr_collecting(ty, &mut Vec::new())
    }

    /// Like [`Self::type_expr`], pushing every type resolved through the typing registry
    /// to `leaves`.
    pub(crate) fn type_expr_collecting(&self, ty: &Type, leaves: &mut Vec<Type>) -> TokenStream {
        self.structural(ty, leaves)
            .unwrap_or_else(|| opaque_type_expr(ty, self.data_crate))
    }

    fn structural(&self, ty: &Type, leaves: &mut Vec<Type>) -> Option<TokenStream> {
        let data_crate = self.data_crate;
        match ty {
            Type::Path(p) if p.qself.is_none() => {
                if self.is_generic_param(p) {
                    return matches!(self.generics, GenericParams::Opaque(_)).then(|| {
                        quote! {
                            #data_crate::model::TypeExpr::Opaque(::std::string::String::from("generic"))
                        }
                    });
                }
                let seg = last_segment(ty)?;
                let ident = seg.ident.to_string();
                if let Some((_, idx)) = self.transparent.iter().find(|(name, _)| *name == ident) {
                    return self.structural(segment_type_arg(seg, *idx)?, leaves);
                }
                let container = match ident.as_str() {
                    "Vec" => quote! { List },
                    "Option" => quote! { Optional },
                    _ => {
                        leaves.push(ty.clone());
                        return Some(quote! { #data_crate::typing::type_expr::<#ty>() });
                    }
                };
                let inner = self.structural(segment_type_arg(seg, 0)?, leaves)?;
                Some(self.overridable(ty, quote! { #container(Box::new(#inner)) }))
            }
            Type::Array(a) if self.arrays_as_lists => {
                let inner = self.structural(&a.elem, leaves)?;
                Some(self.overridable(ty, quote! { List(Box::new(#inner)) }))
            }
            Type::Reference(r) => {
                if self.str_as_string && last_segment(&r.elem).is_some_and(|s| s.ident == "str") {
                    return Some(
                        quote! { #data_crate::model::TypeExpr::Scalar(#data_crate::model::ValueType::String) },
                    );
                }
                self.structural(&r.elem, leaves)
            }
            Type::Tuple(t) if t.elems.is_empty() => Some(
                quote! { #data_crate::model::TypeExpr::Scalar(#data_crate::model::ValueType::Unit) },
            ),
            Type::Tuple(t) => {
                let elems = t
                    .elems
                    .iter()
                    .map(|elem| self.structural(elem, leaves))
                    .collect::<Option<Vec<_>>>()?;
                Some(quote! { #data_crate::model::TypeExpr::Tuple(vec![#(#elems),*]) })
            }
            _ => None,
        }
    }

    fn is_generic_param(&self, p: &syn::TypePath) -> bool {
        let (GenericParams::Fallback(params) | GenericParams::Opaque(params)) = self.generics
        else {
            return false;
        };
        p.path
            .get_ident()
            .is_some_and(|ident| params.contains(&ident.to_string()))
    }

    /// Container encoding that defers to an explicit typing-registry override for `ty`.
    fn overridable(&self, ty: &Type, structural: TokenStream) -> TokenStream {
        let data_crate = self.data_crate;
        quote! {
            if let Some(explicit) = #data_crate::typing::override_type_expr::<#ty>() {
                explicit
            } else {
                #data_crate::model::TypeExpr::#structural
            }
        }
    }
}

/// Stable opaque `rust:<type>` identity for types without a structural encoding.
fn opaque_type_expr(ty: &Type, data_crate: &TokenStream) -> TokenStream {
    let mut raw = ty.to_token_stream().to_string();
    raw.retain(|c| !c.is_whitespace());
    let lit = LitStr::new(&format!("rust:{raw}"), Span::call_site());
    quote! { #data_crate::model::TypeExpr::Opaque(::std::string::String::from(#lit)) }
}

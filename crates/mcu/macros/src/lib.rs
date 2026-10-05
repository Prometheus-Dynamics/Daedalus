//! `#[daedalus_mcu::node]`: declares a plain function as a device node.
//!
//! The function is kept as written. Next to it (in the type namespace, so it may share the
//! function's name) the macro adds a module with:
//!
//! - `NODE: daedalus_mcu::NodeDesc`: id, port names, port type keys and readiness, read on the
//!   host by `daedalus-mcu-build` to declare the node to the planner;
//! - `In<k>`, `Out<k>`, `State`: the port and state types, named by the generated graph code;
//! - `run(state, ctx, inputs..)`: a uniform call shape over the function (borrows, optional
//!   inputs, conditional outputs, `Result`), which the generated `tick` calls directly.

use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, format_ident, quote};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{
    Expr, FnArg, GenericArgument, ItemFn, LitStr, Pat, PathArguments, ReturnType, Token, Type,
    parenthesized,
};

/// Declare a device node: `#[node(id = "..", inputs("a", ..), outputs("y", ..), state(T),
/// fire = "all")]`.
///
/// - `id` (required): the registry id graphs refer to (a `&'static str` constant expression).
/// - `inputs`: port names of the input parameters, in order (default: the parameter names).
///   Parameters are `T` (moved in), `&T`, `&mut T`, or `Option<T>` / `Option<&T>` (optional: never
///   blocks the node, `None` when nothing arrived).
/// - `outputs`: port names of the returned values (default `"out"` for one). The return type is
///   `()`, one value, or a tuple of values, optionally in `Result<_, E>` with
///   `E: Into<daedalus_mcu::NodeError>`; an `Option<T>` value is a conditional output.
/// - `state(T)`: a `&mut T` parameter is the node's state slot (`T: daedalus_mcu::NodeState`).
/// - A `&Ctx` parameter receives the tick context.
/// - `fire = "all"`: wait until every connected required input holds a value (cross-tick join).
#[proc_macro_attribute]
pub fn node(
    args: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let item = syn::parse_macro_input!(item as ItemFn);
    expand(args.into(), item)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[derive(Default)]
struct Args {
    id: Option<Expr>,
    inputs: Option<Vec<LitStr>>,
    outputs: Option<Vec<LitStr>>,
    state: Option<Type>,
    fire_all: bool,
}

fn parse_args(tokens: TokenStream) -> syn::Result<Args> {
    let mut args = Args::default();
    let names = |input: syn::parse::ParseStream| -> syn::Result<Vec<LitStr>> {
        let content;
        parenthesized!(content in input);
        Ok(Punctuated::<LitStr, Token![,]>::parse_terminated(&content)?
            .into_iter()
            .collect())
    };
    syn::meta::parser(|meta| {
        if meta.path.is_ident("id") {
            args.id = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("inputs") {
            args.inputs = Some(names(meta.input)?);
        } else if meta.path.is_ident("outputs") {
            args.outputs = Some(names(meta.input)?);
        } else if meta.path.is_ident("state") {
            let content;
            parenthesized!(content in meta.input);
            args.state = Some(content.parse()?);
        } else if meta.path.is_ident("fire") {
            let mode: LitStr = meta.value()?.parse()?;
            args.fire_all = match mode.value().as_str() {
                "all" => true,
                "any" => false,
                _ => return Err(syn::Error::new(mode.span(), "fire is \"any\" or \"all\"")),
            };
        } else {
            return Err(meta.error("expected id, inputs, outputs, state or fire"));
        }
        Ok(())
    })
    .parse2(tokens)?;
    Ok(args)
}

/// How a function parameter is fed from its `run` argument.
enum Param {
    State,
    Ctx,
    Input {
        ty: Box<Type>,
        optional: bool,
        /// Pass `&x` / `&mut x` / `x.as_ref()` instead of `x`.
        borrow: Option<Borrow>,
    },
}

#[derive(Clone, Copy)]
enum Borrow {
    Shared,
    Mut,
    OptionRef,
}

struct Output {
    ty: Type,
    conditional: bool,
}

fn expand(args: TokenStream, item: ItemFn) -> syn::Result<TokenStream> {
    let args = parse_args(args)?;
    let sig = &item.sig;
    let span = sig.ident.span();
    let id = args
        .id
        .clone()
        .ok_or_else(|| syn::Error::new(span, "missing `id = \"...\"`"))?;
    if !sig.generics.params.is_empty() || sig.asyncness.is_some() {
        return Err(syn::Error::new(
            span,
            "device nodes are plain, non-generic functions",
        ));
    }

    let mut params = Vec::new();
    let mut param_names = Vec::new();
    for arg in &sig.inputs {
        let FnArg::Typed(arg) = arg else {
            return Err(syn::Error::new_spanned(
                arg,
                "device nodes are free functions",
            ));
        };
        let param = classify_param(&arg.ty, args.state.as_ref());
        if let (Param::Input { .. }, Pat::Ident(name)) = (&param, &*arg.pat) {
            param_names.push(LitStr::new(&name.ident.to_string(), name.ident.span()));
        } else if matches!(param, Param::Input { .. }) {
            return Err(syn::Error::new_spanned(&arg.pat, "name input parameters"));
        }
        params.push(param);
    }
    if args.state.is_some() && !params.iter().any(|p| matches!(p, Param::State)) {
        return Err(syn::Error::new(span, "state(T) needs a `&mut T` parameter"));
    }

    let (outputs, fallible, tuple) = classify_return(&sig.output);
    let input_names = args.inputs.clone().unwrap_or(param_names);
    let output_names = match (&args.outputs, outputs.len()) {
        (Some(names), _) => names.clone(),
        (None, 0) => Vec::new(),
        (None, 1) => vec![LitStr::new("out", Span::call_site())],
        (None, _) => {
            return Err(syn::Error::new(
                span,
                "name the outputs: outputs(\"a\", ..)",
            ));
        }
    };
    let input_count = params
        .iter()
        .filter(|p| matches!(p, Param::Input { .. }))
        .count();
    if input_names.len() != input_count || output_names.len() != outputs.len() {
        return Err(syn::Error::new(
            span,
            format!(
                "{input_count} input parameter(s) and {} output(s), but {} input and {} output \
                 name(s)",
                outputs.len(),
                input_names.len(),
                output_names.len()
            ),
        ));
    }

    let fn_name = &sig.ident;
    let vis = &item.vis;
    let state_ty = args
        .state
        .as_ref()
        .map_or_else(|| quote!(()), ToTokens::to_token_stream);
    let fire_all = args.fire_all;

    let mut aliases = Vec::new();
    let mut input_descs = Vec::new();
    let mut run_params = Vec::new();
    let mut call_args = Vec::new();
    let mut k = 0usize;
    for param in &params {
        match param {
            Param::State => call_args.push(quote!(state)),
            Param::Ctx => call_args.push(quote!(ctx)),
            Param::Input {
                ty,
                optional,
                borrow,
            } => {
                let alias = format_ident!("In{k}");
                let var = format_ident!("i{k}");
                let name = &input_names[k];
                aliases.push(quote!(pub type #alias = #ty;));
                input_descs.push(port_desc(name, &alias, *optional));
                let mutability = matches!(borrow, Some(Borrow::Mut)).then(|| quote!(mut));
                run_params.push(if *optional {
                    quote!(#mutability #var: ::core::option::Option<#alias>)
                } else {
                    quote!(#mutability #var: #alias)
                });
                call_args.push(match borrow {
                    None => quote!(#var),
                    Some(Borrow::Shared) => quote!(&#var),
                    Some(Borrow::Mut) => quote!(&mut #var),
                    Some(Borrow::OptionRef) => quote!(#var.as_ref()),
                });
                k += 1;
            }
        }
    }

    let mut output_descs = Vec::new();
    let mut output_tys = Vec::new();
    let mut output_values = Vec::new();
    for (k, output) in outputs.iter().enumerate() {
        let alias = format_ident!("Out{k}");
        let ty = &output.ty;
        aliases.push(quote!(pub type #alias = #ty;));
        output_descs.push(port_desc(&output_names[k], &alias, output.conditional));
        output_tys.push(quote!(::core::option::Option<#alias>));
        let value = if tuple {
            let index = syn::Index::from(k);
            quote!(out.#index)
        } else {
            quote!(out)
        };
        output_values.push(if output.conditional {
            value
        } else {
            quote!(::core::option::Option::Some(#value))
        });
    }

    let call = quote!(super::#fn_name(#(#call_args),*));
    let call = if fallible {
        quote!(match #call {
            ::core::result::Result::Ok(out) => out,
            ::core::result::Result::Err(error) => {
                return ::core::result::Result::Err(
                    <_ as ::core::convert::Into<::daedalus_mcu::NodeError>>::into(error),
                );
            }
        })
    } else {
        call
    };
    let doc =
        format!("Device glue of the [`{fn_name}`](fn@{fn_name}) node (`#[daedalus_mcu::node]`).");

    Ok(quote! {
        #item

        #[doc = #doc]
        #[allow(non_snake_case, clippy::unused_unit)]
        #vis mod #fn_name {
            #[allow(unused_imports)]
            use super::*;

            #(#aliases)*
            pub type State = #state_ty;

            pub const NODE: ::daedalus_mcu::NodeDesc = ::daedalus_mcu::NodeDesc {
                id: #id,
                path: ::core::module_path!(),
                inputs: &[#(#input_descs),*],
                outputs: &[#(#output_descs),*],
                fire_all: #fire_all,
            };

            #[inline(always)]
            #[allow(unused_variables, clippy::let_unit_value)]
            pub fn run(
                state: &mut State,
                ctx: &::daedalus_mcu::Ctx,
                #(#run_params),*
            ) -> ::core::result::Result<(#(#output_tys,)*), ::daedalus_mcu::NodeError> {
                let out = #call;
                ::core::result::Result::Ok((#(#output_values,)*))
            }
        }
    })
}

fn port_desc(name: &LitStr, alias: &syn::Ident, optional: bool) -> TokenStream {
    quote!(::daedalus_mcu::PortDesc {
        name: #name,
        key: <#alias as ::daedalus_mcu::McuType>::KEY,
        optional: #optional,
    })
}

fn classify_param(ty: &Type, state: Option<&Type>) -> Param {
    if let Type::Reference(reference) = ty {
        let inner = &*reference.elem;
        if reference.mutability.is_some() {
            if state.is_some_and(|state| same_type(state, inner)) {
                return Param::State;
            }
            return input(inner, false, Some(Borrow::Mut));
        }
        if last_ident(inner).is_some_and(|ident| ident == "Ctx") {
            return Param::Ctx;
        }
        return input(inner, false, Some(Borrow::Shared));
    }
    match option_inner(ty) {
        Some(Type::Reference(reference)) if reference.mutability.is_none() => {
            input(&reference.elem, true, Some(Borrow::OptionRef))
        }
        Some(inner) => input(inner, true, None),
        None => input(ty, false, None),
    }
}

fn input(ty: &Type, optional: bool, borrow: Option<Borrow>) -> Param {
    Param::Input {
        ty: Box::new(ty.clone()),
        optional,
        borrow,
    }
}

/// Outputs of a return type, whether it is a `Result`, and whether the values are a tuple.
fn classify_return(output: &ReturnType) -> (Vec<Output>, bool, bool) {
    let ReturnType::Type(_, ty) = output else {
        return (Vec::new(), false, true);
    };
    let (ty, fallible) = match generic_arg(ty, "Result") {
        Some(ok) => (ok, true),
        None => (&**ty, false),
    };
    let (values, tuple): (Vec<&Type>, bool) = match ty {
        Type::Tuple(tuple) => (tuple.elems.iter().collect(), true),
        Type::Paren(inner) => (vec![&*inner.elem], false),
        other => (vec![other], false),
    };
    let outputs = values
        .into_iter()
        .map(|ty| match option_inner(ty) {
            Some(inner) => Output {
                ty: inner.clone(),
                conditional: true,
            },
            None => Output {
                ty: ty.clone(),
                conditional: false,
            },
        })
        .collect();
    (outputs, fallible, tuple)
}

fn option_inner(ty: &Type) -> Option<&Type> {
    generic_arg(ty, "Option")
}

/// The first type argument of `ty` when its last path segment is `name` (`Option<T>` -> `T`).
fn generic_arg<'a>(ty: &'a Type, name: &str) -> Option<&'a Type> {
    let Type::Path(path) = ty else { return None };
    let segment = path.path.segments.last()?;
    if segment.ident != name {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.iter().find_map(|arg| match arg {
        GenericArgument::Type(ty) => Some(ty),
        _ => None,
    })
}

fn last_ident(ty: &Type) -> Option<&syn::Ident> {
    match ty {
        Type::Path(path) => path.path.segments.last().map(|s| &s.ident),
        _ => None,
    }
}

fn same_type(a: &Type, b: &Type) -> bool {
    a.to_token_stream().to_string() == b.to_token_stream().to_string()
}

//! Turning a signature and the attribute into ports.

use proc_macro2::{Group, Literal, Span, TokenStream, TokenTree};
use quote::{ToTokens, quote};
use syn::{
    Error, Expr, FnArg, GenericArgument, Lit, LitStr, Pat, PathArguments, Result, ReturnType,
    Signature, Type, UnOp,
};

use crate::args::{NodeArgs, PortArg};

/// Replaces an identifier everywhere in a type.
pub(crate) fn substitute(tokens: TokenStream, name: &syn::Ident, with: &Type) -> TokenStream {
    tokens
        .into_iter()
        .flat_map(|token| match token {
            TokenTree::Ident(ident) if ident == *name => with.to_token_stream(),
            TokenTree::Group(group) => {
                let inner = substitute(group.stream(), name, with);
                let mut copy = Group::new(group.delimiter(), inner);
                copy.set_span(group.span());
                TokenTree::Group(copy).into()
            }
            other => other.into(),
        })
        .collect()
}

fn same(a: &Type, b: &Type) -> bool {
    a.to_token_stream().to_string() == b.to_token_stream().to_string()
}

/// The argument of `Name<T>` if the last path segment is `name`.
pub(crate) fn generic_of<'a>(ty: &'a Type, name: &str) -> Option<Vec<&'a Type>> {
    let Type::Path(path) = ty else { return None };
    let last = path.path.segments.last()?;
    if last.ident != name {
        return None;
    }
    let PathArguments::AngleBracketed(arguments) = &last.arguments else {
        return None;
    };
    Some(
        arguments
            .args
            .iter()
            .filter_map(|arg| match arg {
                GenericArgument::Type(ty) => Some(ty),
                _ => None,
            })
            .collect(),
    )
}

fn unref(ty: &Type) -> &Type {
    match ty {
        Type::Reference(reference) => &reference.elem,
        other => other,
    }
}

pub(crate) struct Port {
    pub name: String,
    pub span: Span,
    pub type_ref: TokenStream,
    pub access: TokenStream,
    pub optional: bool,
    pub conditional: bool,
    pub arg: PortArg,
}

pub(crate) struct FanIn {
    pub prefix: String,
    pub type_ref: TokenStream,
}

/// What a signature comes to, for one generic instance or the plain function.
pub(crate) struct Shape {
    pub inputs: Vec<Port>,
    pub outputs: Vec<Port>,
    pub fanin: Vec<FanIn>,
    /// The parameters, with the instance's type filled in.
    pub params: TokenStream,
    pub ret: TokenStream,
    /// One `role position "port" name: Type,` per parameter, for a backend that binds them.
    pub bind: TokenStream,
    /// `plain | fallible, pattern, [value | option position name: Type, ..]`.
    pub returns: TokenStream,
    /// How many inputs aren't fan-in. Fan-in ports come after them.
    pub fixed: usize,
    /// A `Result` return's error type, which has to be `Display`.
    pub error: Option<Type>,
    pub modifies: bool,
}

/// The type's key, checked to be usable from this crate.
pub(crate) fn checked_key(ty: &Type) -> TokenStream {
    quote! {{
        const _: () = {
            static KEY: ::daedalus_registry::__private::TypeKey =
                <#ty as ::daedalus_registry::__private::Typed>::KEY;
            ::daedalus_registry::__private::require_local_key(&KEY, ::core::module_path!())
        };
        <#ty as ::daedalus_registry::__private::Typed>::KEY
    }}
}

pub(crate) fn key_of(ty: &Type) -> TokenStream {
    let key = checked_key(ty);
    quote!(::daedalus_registry::TypeRef::Key(#key))
}

pub(crate) fn body_type() -> TokenStream {
    quote!(::daedalus_registry::TypeRef::Body)
}

/// The type a port's attribute asks for, if it asks.
pub(crate) fn declared_type(arg: &PortArg) -> Option<TokenStream> {
    if let Some(key) = &arg.type_key {
        return Some(quote! {
            ::daedalus_registry::TypeRef::Key(::daedalus_registry::__private::TypeKey::new(#key))
        });
    }
    arg.ty.as_ref().map(key_of)
}

fn access(name: &str) -> TokenStream {
    let name = syn::Ident::new(name, Span::call_site());
    quote!(::daedalus_registry::__private::Access::#name)
}

/// The parameter `&Ctx`, which the runner fills. Recognized by name, like `FanIn`.
fn is_ctx(ty: &Type) -> bool {
    let Type::Reference(reference) = ty else {
        return false;
    };
    let Type::Path(path) = &*reference.elem else {
        return false;
    };
    reference.mutability.is_none() && path.path.segments.last().is_some_and(|s| s.ident == "Ctx")
}

fn role_of(mode: &str, optional: bool) -> syn::Ident {
    let role = match (mode, optional) {
        ("Modify", false) => "modify",
        ("Modify", true) => "opt_modify",
        ("Read", false) => "read",
        ("Read", true) => "opt_read",
        (_, false) => "take",
        (_, true) => "opt_take",
    };
    syn::Ident::new(role, Span::call_site())
}

fn input_port(name: String, span: Span, ty: &Type, arg: PortArg) -> (Port, bool, syn::Ident) {
    let inner = generic_of(ty, "Option").and_then(|args| args.first().copied());
    let optional = inner.is_some();
    let value = inner.unwrap_or(ty);
    let (mode, modifies) = match value {
        Type::Reference(reference) if reference.mutability.is_some() => ("Modify", true),
        Type::Reference(_) => ("Read", false),
        _ => ("Move", false),
    };
    let type_ref = declared_type(&arg).unwrap_or_else(|| key_of(unref(value)));
    let span = arg.name.as_ref().map_or(span, LitStr::span);
    let role = role_of(mode, optional);
    let port = Port {
        name,
        span,
        type_ref,
        access: access(mode),
        optional,
        conditional: false,
        arg,
    };
    (port, modifies, role)
}

fn take_arg(ports: &mut Option<std::vec::IntoIter<PortArg>>) -> PortArg {
    ports.as_mut().and_then(Iterator::next).unwrap_or_default()
}

fn output_types(ty: &Type) -> Vec<&Type> {
    match ty {
        Type::Tuple(tuple) => tuple.elems.iter().collect(),
        other => vec![other],
    }
}

/// Reads a function's signature against the attribute. `instance` fills in a generic parameter.
pub(crate) fn shape(
    args: &NodeArgs,
    inputs: Option<Vec<PortArg>>,
    outputs: Option<Vec<PortArg>>,
    signature: &Signature,
    instance: Option<(&syn::Ident, &Type)>,
) -> Result<Shape> {
    let fill = |tokens: TokenStream| -> TokenStream {
        match instance {
            Some((name, with)) => substitute(tokens, name, with),
            None => tokens,
        }
    };
    let fill_type = |ty: &Type| -> Result<Type> { syn::parse2(fill(ty.to_token_stream())) };
    let declared = inputs.as_ref().map(Vec::len);
    let mut names = inputs.map(Vec::into_iter);
    let mut shape = Shape {
        inputs: Vec::new(),
        outputs: Vec::new(),
        fanin: Vec::new(),
        params: TokenStream::new(),
        ret: quote!(()),
        bind: TokenStream::new(),
        returns: quote!(plain, (), []),
        fixed: 0,
        error: None,
        modifies: false,
    };
    for param in &signature.inputs {
        let FnArg::Typed(param) = param else {
            return Err(Error::new_spanned(param, "a node can't take self"));
        };
        let Pat::Ident(pattern) = &*param.pat else {
            return Err(Error::new_spanned(
                &param.pat,
                "a node parameter needs a plain name",
            ));
        };
        let ident = &pattern.ident;
        let ty = fill_type(&param.ty)?;
        shape.params.extend(quote!(#ident: #ty,));
        let is_state = matches!(&ty, Type::Reference(r) if r.mutability.is_some())
            && args
                .state
                .as_ref()
                .is_some_and(|state| same(unref(&ty), state));
        let zero = Literal::usize_unsuffixed(0);
        let value = unref(&ty).clone();
        if is_state {
            shape.bind.extend(quote!(state #zero "" #ident: #value,));
            continue;
        }
        if args
            .config
            .as_ref()
            .is_some_and(|config| same(unref(&ty), config))
        {
            let role = match &ty {
                Type::Reference(_) => quote!(config_ref),
                _ => quote!(config_val),
            };
            shape.bind.extend(quote!(#role #zero "" #ident: #value,));
            continue;
        }
        if is_ctx(&ty) {
            shape.bind.extend(quote!(ctx #zero "" #ident: #value,));
            continue;
        }
        if let Some(element) = generic_of(&ty, "FanIn").and_then(|a| a.first().copied()) {
            let arg = take_arg(&mut names);
            let prefix = arg
                .name
                .as_ref()
                .map_or(ident.to_string(), |name| name.value());
            let type_ref = declared_type(&arg).unwrap_or_else(|| key_of(element));
            shape.bind.extend(quote!(fanin #zero "" #ident: #element,));
            shape.fanin.push(FanIn { prefix, type_ref });
            continue;
        }
        let arg = take_arg(&mut names);
        let name = arg
            .name
            .as_ref()
            .map_or(ident.to_string(), |name| name.value());
        let at = Literal::usize_unsuffixed(shape.inputs.len());
        let port_name = name.clone();
        let inner = generic_of(&ty, "Option").and_then(|a| a.first().copied());
        let value = unref(inner.unwrap_or(&ty)).clone();
        let (port, modifies, role) = input_port(name, ident.span(), &ty, arg);
        shape
            .bind
            .extend(quote!(#role #at #port_name #ident: #value,));
        shape.modifies |= modifies;
        shape.inputs.push(port);
    }
    if let Some(declared) = declared {
        let used = shape.inputs.len() + shape.fanin.len();
        if declared != used {
            return Err(Error::new_spanned(
                &signature.ident,
                format!("inputs() names {declared} ports but the function has {used}"),
            ));
        }
    }
    shape.fixed = shape.inputs.len();
    let ReturnType::Type(_, returned) = &signature.output else {
        if outputs.as_ref().is_some_and(|names| !names.is_empty()) {
            return Err(Error::new_spanned(
                &signature.ident,
                "outputs() on a function with no return",
            ));
        }
        return Ok(shape);
    };
    let returned = fill_type(returned)?;
    shape.ret = returned.to_token_stream();
    let mut value = &returned;
    let fallible = generic_of(&returned, "Result").is_some();
    if let Some(parts) = generic_of(&returned, "Result") {
        value = parts.first().copied().unwrap_or(value);
        shape.error = parts.get(1).map(|error| (*error).clone());
    }
    let types = output_types(value);
    let is_tuple = matches!(value, Type::Tuple(_));
    if let Some(names) = &outputs
        && names.len() != types.len()
    {
        return Err(Error::new_spanned(
            &signature.output,
            format!(
                "outputs() names {} ports but the function returns {}",
                names.len(),
                types.len()
            ),
        ));
    }
    let single = types.len() == 1;
    let mut names = outputs.map(Vec::into_iter);
    let mut written = TokenStream::new();
    let mut bound = Vec::new();
    for (index, ty) in types.into_iter().enumerate() {
        let arg = take_arg(&mut names);
        let fallback = if single {
            "out".to_string()
        } else {
            format!("out{index}")
        };
        let name = arg.name.as_ref().map_or(fallback, |name| name.value());
        let inner = generic_of(ty, "Option").and_then(|args| args.first().copied());
        let type_ref = declared_type(&arg).unwrap_or_else(|| key_of(inner.unwrap_or(ty)));
        let conditional = inner.is_some();
        let (kind, held) = match inner {
            Some(inner) => (quote!(option), inner),
            None => (quote!(value), ty),
        };
        let (at, out) = (
            Literal::usize_unsuffixed(index),
            syn::Ident::new(&format!("__out{index}"), Span::call_site()),
        );
        written.extend(quote!(#kind #at #out: #held,));
        bound.push(out);
        shape.outputs.push(Port {
            name,
            span: arg
                .name
                .as_ref()
                .map_or(signature.ident.span(), LitStr::span),
            type_ref,
            access: access("Read"),
            optional: false,
            conditional,
            arg,
        });
    }
    let pattern = match (is_tuple, bound.as_slice()) {
        (false, [only]) => quote!(#only),
        (_, [only]) => quote!((#only,)),
        (_, many) => quote!((#(#many),*)),
    };
    let flow = if fallible {
        quote!(fallible)
    } else {
        quote!(plain)
    };
    shape.returns = quote!(#flow, #pattern, [#written]);
    Ok(shape)
}

/// A `default = ..` literal as a `Literal` expression.
pub(crate) fn literal(expr: &Expr) -> Result<TokenStream> {
    let path = quote!(::daedalus_registry::__private::Literal);
    let unsupported = || Error::new_spanned(expr, "a default must be a bool, number or string");
    let (lit, negative) = match expr {
        Expr::Lit(lit) => (&lit.lit, false),
        Expr::Unary(unary) if matches!(unary.op, UnOp::Neg(_)) => match &*unary.expr {
            Expr::Lit(lit) => (&lit.lit, true),
            _ => return Err(unsupported()),
        },
        _ => return Err(unsupported()),
    };
    let sign = negative.then(|| quote!(-));
    Ok(match lit {
        Lit::Bool(value) if !negative => quote!(#path::Bool(#value)),
        Lit::Int(value) => {
            let value = value.base10_parse::<i64>()?;
            quote!(#path::Int(#sign #value))
        }
        Lit::Float(value) => {
            let value = value.base10_parse::<f64>()?;
            quote!(#path::Float(#sign #value))
        }
        Lit::Str(value) if !negative => quote!(#path::Str(#value)),
        _ => return Err(unsupported()),
    })
}

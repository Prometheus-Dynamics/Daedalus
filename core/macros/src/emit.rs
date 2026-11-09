//! Writing out the records.

use std::collections::BTreeSet;

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{Error, Expr, Ident, Item, ItemConst, ItemFn, LitStr, Result, Type};

use crate::args::{NodeArgs, PortArg};
use crate::ports::{FanIn, Port, Shape, body_type, declared_type, key_of, literal, shape};

fn text(value: &Option<LitStr>) -> TokenStream {
    match value {
        Some(value) => quote!(#value),
        None => quote!(""),
    }
}

fn port_tokens(port: &Port) -> Result<TokenStream> {
    let name = &port.name;
    let (ty, access, optional, conditional) = (
        &port.type_ref,
        &port.access,
        port.optional,
        port.conditional,
    );
    let default = match &port.arg.default {
        Some(expr) => {
            let value = literal(expr)?;
            quote!(::core::option::Option::Some(#value))
        }
        None => quote!(::core::option::Option::None),
    };
    let (label, unit, group) = (
        text(&port.arg.label),
        text(&port.arg.unit),
        text(&port.arg.group),
    );
    let (description, source) = (text(&port.arg.description), text(&port.arg.source));
    let advanced = port.arg.advanced;
    Ok(quote! {
        ::daedalus_registry::PortDecl {
            name: #name,
            ty: #ty,
            access: #access,
            optional: #optional,
            conditional: #conditional,
            default: #default,
            label: #label,
            unit: #unit,
            group: #group,
            advanced: #advanced,
            description: #description,
            source: #source,
        }
    })
}

fn no_duplicates(ports: &[Port], fanin: &[FanIn]) -> Result<()> {
    let mut seen: BTreeSet<&str> = fanin.iter().map(|f| f.prefix.as_str()).collect();
    for port in ports {
        if !seen.insert(&port.name) {
            return Err(Error::new(
                port.span,
                format!("duplicate port `{}`", port.name),
            ));
        }
    }
    Ok(())
}

/// Everything the records need that doesn't depend on the item's kind.
struct Parts<'a> {
    args: &'a NodeArgs,
    id: TokenStream,
    upper: String,
    body_name: TokenStream,
    kind: TokenStream,
    shape: Shape,
}

fn fire_tokens(args: &NodeArgs) -> Result<TokenStream> {
    let variant = match &args.fire {
        None => quote!(All),
        Some(fire) => match fire.value().as_str() {
            "any" => quote!(Any),
            "all" => quote!(All),
            _ => return Err(Error::new(fire.span(), "fire must be \"any\" or \"all\"")),
        },
    };
    Ok(quote!(::daedalus_registry::Fire::#variant))
}

fn check_rules(args: &NodeArgs, shape: &Shape) -> Result<()> {
    no_duplicates(&shape.inputs, &shape.fanin)?;
    no_duplicates(&shape.outputs, &[])?;
    if args.reusable && (args.state.is_some() || shape.modifies) {
        return Err(Error::new(
            Span::call_site(),
            "a reusable node must be pure: no state and no `&mut` inputs",
        ));
    }
    let single = shape.inputs.len() == 1 && shape.outputs.len() == 1 && shape.fanin.is_empty();
    if args.conversion.is_some() && !single {
        return Err(Error::new(
            Span::call_site(),
            "a conversion has one input and one output",
        ));
    }
    let known = |port: &Ident| {
        shape
            .inputs
            .iter()
            .chain(&shape.outputs)
            .any(|known| *port == known.name.as_str())
    };
    for (port, _) in &args.memory {
        if !known(port) {
            return Err(Error::new(port.span(), format!("no port named `{port}`")));
        }
    }
    Ok(())
}

fn state_tokens(args: &NodeArgs) -> (TokenStream, TokenStream) {
    let Some(state) = &args.state else {
        return (quote!(::core::option::Option::None), TokenStream::new());
    };
    let check = quote_spanned! {state.span()=>
        const _: () = {
            const fn state_must_be_default_send_static<
                T: ::core::default::Default + ::core::marker::Send + 'static,
            >() {}
            state_must_be_default_send_static::<#state>()
        };
    };
    let record = quote! {
        ::core::option::Option::Some(::daedalus_registry::StateDecl {
            size: ::core::mem::size_of::<#state>(),
            align: ::core::mem::align_of::<#state>(),
            name: ::core::stringify!(#state),
        })
    };
    (record, check)
}

fn config_tokens(args: &NodeArgs) -> TokenStream {
    let Some(config) = &args.config else {
        return TokenStream::new();
    };
    let ty = key_of(config);
    quote! {
        ::daedalus_registry::ConfigDecl {
            name: "config",
            ty: #ty,
            fields: <#config>::FIELDS,
        },
    }
}

fn conversion_tokens(args: &NodeArgs) -> TokenStream {
    match &args.conversion {
        Some(conversion) => {
            let (cost, lossy) = (conversion.cost, conversion.lossy);
            quote! {
                ::core::option::Option::Some(::daedalus_registry::Conversion {
                    cost: #cost,
                    lossy: #lossy,
                })
            }
        }
        None => quote!(::core::option::Option::None),
    }
}

fn error_check(error: &Option<Type>) -> TokenStream {
    let Some(error) = error else {
        return TokenStream::new();
    };
    quote_spanned! {error.span()=>
        const _: () = {
            const fn error_must_be_display<T: ::core::fmt::Display>() {}
            error_must_be_display::<#error>()
        };
    }
}

fn records(parts: Parts<'_>) -> Result<TokenStream> {
    let Parts {
        args,
        id,
        upper,
        body_name,
        kind,
        shape,
    } = parts;
    check_rules(args, &shape)?;
    let (summary, description) = (text(&args.summary), text(&args.description));
    let inputs = shape
        .inputs
        .iter()
        .map(port_tokens)
        .collect::<Result<Vec<_>>>()?;
    let outputs = shape
        .outputs
        .iter()
        .map(port_tokens)
        .collect::<Result<Vec<_>>>()?;
    let fanin = shape.fanin.iter().map(|fan| {
        let (prefix, ty) = (&fan.prefix, &fan.type_ref);
        quote!(::daedalus_registry::FanInDecl { prefix: #prefix, start: 0, ty: #ty })
    });
    let fire = fire_tokens(args)?;
    let (reusable, shareable) = (args.reusable, args.shareable);
    let conversion = conversion_tokens(args);
    let config = config_tokens(args);
    let (state, state_check) = state_tokens(args);
    let result_check = error_check(&shape.error);
    let memory = args.memory.iter().map(|(port, space)| {
        let port = port.to_string();
        quote!((#port, #space))
    });
    let (path, backend_args) = match &args.backend {
        Some(backend) => {
            let path = &backend.path;
            (quote_spanned!(path.span()=> #path), backend.args.clone())
        }
        None => (quote!(::daedalus_cpu_author), TokenStream::new()),
    };
    let (params, ret) = (&shape.params, &shape.ret);
    let decl = format_ident!("{upper}_DECL");
    let node = format_ident!("{upper}_NODE");
    Ok(quote! {
        const _: ::daedalus_registry::__private::TypeKey =
            ::daedalus_registry::__private::TypeKey::new(#id);
        #state_check
        #result_check

        pub const #decl: ::daedalus_registry::NodeDecl = ::daedalus_registry::NodeDecl {
            id: #id,
            at: ::daedalus_registry::SourceAt(
                ::core::concat!(::core::file!(), ":", ::core::line!())
            ),
            summary: #summary,
            description: #description,
            inputs: &[#(#inputs),*],
            outputs: &[#(#outputs),*],
            config: &[#config],
            fanin: &[#(#fanin),*],
            fire: #fire,
            reusable: #reusable,
            shareable: #shareable,
            conversion: #conversion,
            group: ::core::option::Option::None,
        };

        pub const #node: ::daedalus_registry::NodeRecord = ::daedalus_registry::NodeRecord {
            decl: &#decl,
            body: {
                const __DAEDALUS_BODY: ::daedalus_registry::BodyRecord = #path::__daedalus_body!(
                    kind = #kind,
                    name = (#body_name),
                    args = (#backend_args),
                    params = (#params),
                    ret = #ret,
                    decl = #decl
                );
                __DAEDALUS_BODY.with_node(#state, &[#(#memory),*])
            },
        };
    })
}

fn clone_ports(ports: &Option<Vec<PortArg>>) -> Option<Vec<PortArg>> {
    ports.as_ref().map(|ports| {
        ports
            .iter()
            .map(|port| PortArg {
                name: port.name.clone(),
                ty: port.ty.clone(),
                type_key: port.type_key.clone(),
                source: port.source.clone(),
                default: port.default.clone(),
                label: port.label.clone(),
                unit: port.unit.clone(),
                group: port.group.clone(),
                advanced: port.advanced,
                description: port.description.clone(),
            })
            .collect()
    })
}

fn required_id(args: &NodeArgs) -> Result<&Expr> {
    args.id
        .as_ref()
        .ok_or_else(|| Error::new(Span::call_site(), "#[node] needs an id"))
}

fn upper(name: &Ident) -> String {
    name.to_string().to_uppercase()
}

fn expand_fn(args: NodeArgs, item: ItemFn) -> Result<TokenStream> {
    let id = required_id(&args)?;
    let name = &item.sig.ident;
    let mut out = quote!(#item);
    let declared_param = item
        .sig
        .generics
        .type_params()
        .next()
        .map(|param| param.ident.clone());
    let Some(generics) = &args.generics else {
        if let Some(param) = declared_param {
            return Err(Error::new(
                param.span(),
                "a generic node needs generics(T, instance(Type, \"suffix\"), ...)",
            ));
        }
        let shape = shape(
            &args,
            clone_ports(&args.inputs),
            clone_ports(&args.outputs),
            &item.sig,
            None,
        )?;
        out.extend(records(Parts {
            args: &args,
            id: quote!(#id),
            upper: upper(name),
            body_name: quote!(#name),
            kind: quote!(fn),
            shape,
        })?);
        return Ok(out);
    };
    if declared_param.as_ref() != Some(&generics.param) {
        return Err(Error::new(
            generics.param.span(),
            "not a type parameter of this function",
        ));
    }
    if generics.instances.is_empty() {
        return Err(Error::new(
            generics.param.span(),
            "generics() needs at least one instance",
        ));
    }
    let Expr::Lit(_) = id else {
        return Err(Error::new_spanned(
            id,
            "a generic node's id must be a string literal",
        ));
    };
    for instance in &generics.instances {
        let shape = shape(
            &args,
            clone_ports(&args.inputs),
            clone_ports(&args.outputs),
            &item.sig,
            Some((&generics.param, &instance.ty)),
        )?;
        let suffix = &instance.suffix;
        let ty = &instance.ty;
        let tail: String = suffix
            .value()
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c.to_ascii_uppercase()
                } else {
                    '_'
                }
            })
            .collect();
        out.extend(records(Parts {
            args: &args,
            id: quote!(::core::concat!(#id, ".", #suffix)),
            upper: format!("{}_{tail}", upper(name)),
            body_name: quote!(#name::<#ty>),
            kind: quote!(fn),
            shape,
        })?);
    }
    Ok(out)
}

fn const_port(port: PortArg) -> Result<Port> {
    let name = port.name.as_ref().expect("the parser requires a name");
    Ok(Port {
        name: name.value(),
        span: name.span(),
        type_ref: declared_type(&port).unwrap_or_else(body_type),
        access: quote!(::daedalus_registry::__private::Access::Read),
        optional: false,
        conditional: false,
        arg: port,
    })
}

/// A body held in a const: its ports come from the attribute, and their types from the body.
fn expand_const(args: NodeArgs, item: ItemConst) -> Result<TokenStream> {
    let id = required_id(&args)?;
    let name = &item.ident;
    let ports = |list: Option<Vec<PortArg>>| -> Result<Vec<Port>> {
        list.unwrap_or_default()
            .into_iter()
            .map(const_port)
            .collect()
    };
    let shape = Shape {
        inputs: ports(clone_ports(&args.inputs))?,
        outputs: ports(clone_ports(&args.outputs))?,
        fanin: Vec::new(),
        params: TokenStream::new(),
        ret: quote!(()),
        error: None,
        modifies: false,
    };
    let mut out = quote!(#item);
    out.extend(records(Parts {
        args: &args,
        id: quote!(#id),
        upper: upper(name),
        body_name: quote!(#name),
        kind: quote!(const),
        shape,
    })?);
    Ok(out)
}

pub(crate) fn expand(args: NodeArgs, item: Item) -> Result<TokenStream> {
    match item {
        Item::Fn(item) => expand_fn(args, item),
        Item::Const(item) => expand_const(args, item),
        other => Err(Error::new_spanned(
            other,
            "#[node] goes on a function or a const",
        )),
    }
}

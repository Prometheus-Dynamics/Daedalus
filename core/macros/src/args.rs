//! Parsing the attribute's arguments.

use proc_macro2::TokenStream;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Error, Expr, Ident, LitBool, LitInt, LitStr, Path, Result, Token, Type, parenthesized};

#[derive(Default)]
pub(crate) struct PortArg {
    pub name: Option<LitStr>,
    pub ty: Option<Type>,
    pub type_key: Option<LitStr>,
    pub source: Option<LitStr>,
    pub default: Option<Expr>,
    pub label: Option<LitStr>,
    pub unit: Option<LitStr>,
    pub group: Option<LitStr>,
    pub advanced: bool,
    pub description: Option<LitStr>,
}

pub(crate) enum InputArg {
    Port(Box<PortArg>),
    Config(Box<Type>),
}

pub(crate) struct Instance {
    pub ty: Type,
    pub suffix: LitStr,
}

pub(crate) struct GenericsArg {
    pub param: Ident,
    pub instances: Vec<Instance>,
}

pub(crate) struct ConversionArg {
    pub cost: u32,
    pub lossy: bool,
}

pub(crate) struct BackendArg {
    pub path: Path,
    pub args: TokenStream,
}

#[derive(Default)]
pub(crate) struct NodeArgs {
    pub id: Option<Expr>,
    pub summary: Option<LitStr>,
    pub description: Option<LitStr>,
    pub inputs: Option<Vec<PortArg>>,
    pub config: Option<Type>,
    pub outputs: Option<Vec<PortArg>>,
    pub fire: Option<LitStr>,
    pub reusable: bool,
    pub shareable: bool,
    pub conversion: Option<ConversionArg>,
    pub state: Option<Type>,
    pub generics: Option<GenericsArg>,
    pub backend: Option<BackendArg>,
    pub memory: Vec<(Ident, LitStr)>,
}

pub(crate) fn value<T: Parse>(input: ParseStream<'_>) -> Result<T> {
    input.parse::<Token![=]>()?;
    input.parse()
}

fn name_of(input: ParseStream<'_>) -> Result<LitStr> {
    if input.peek(LitStr) {
        input.parse()
    } else {
        let name: Ident = input.parse()?;
        Ok(LitStr::new(&name.to_string(), name.span()))
    }
}

fn parse_port_body(input: ParseStream<'_>) -> Result<PortArg> {
    let mut port = PortArg::default();
    while !input.is_empty() {
        let key: Ident = input.parse()?;
        match key.to_string().as_str() {
            "name" => {
                input.parse::<Token![=]>()?;
                port.name = Some(name_of(input)?);
            }
            "ty" => port.ty = Some(value(input)?),
            "type_key" => port.type_key = Some(value(input)?),
            "source" => port.source = Some(value(input)?),
            "default" => port.default = Some(value(input)?),
            "label" => port.label = Some(value(input)?),
            "unit" => port.unit = Some(value(input)?),
            "group" => port.group = Some(value(input)?),
            "description" => port.description = Some(value(input)?),
            "advanced" => {
                port.advanced = if input.peek(Token![=]) {
                    value::<LitBool>(input)?.value
                } else {
                    true
                }
            }
            _ => return Err(Error::new(key.span(), "unsupported port argument")),
        }
        if !input.is_empty() {
            input.parse::<Token![,]>()?;
        }
    }
    if port.name.is_none() {
        return Err(input.error("a port needs a name"));
    }
    if let (Some(ty), Some(_)) = (&port.ty, &port.type_key) {
        return Err(Error::new_spanned(
            ty,
            "a port takes `ty` or `type_key`, not both",
        ));
    }
    Ok(port)
}

fn named(name: LitStr) -> InputArg {
    InputArg::Port(Box::new(PortArg {
        name: Some(name),
        ..PortArg::default()
    }))
}

impl Parse for InputArg {
    fn parse(input: ParseStream<'_>) -> Result<InputArg> {
        if input.peek(LitStr) {
            let name = input.parse()?;
            return Ok(named(name));
        }
        let word: Ident = input.fork().parse()?;
        if word == "port" && input.peek2(syn::token::Paren) {
            input.parse::<Ident>()?;
            let body;
            parenthesized!(body in input);
            return Ok(InputArg::Port(Box::new(parse_port_body(&body)?)));
        }
        if word == "config" && input.peek2(Token![=]) {
            input.parse::<Ident>()?;
            return Ok(InputArg::Config(Box::new(value(input)?)));
        }
        let name = name_of(input)?;
        Ok(named(name))
    }
}

fn parse_ports(args: &mut NodeArgs, input: ParseStream<'_>, is_input: bool) -> Result<()> {
    let body;
    parenthesized!(body in input);
    let mut ports = Vec::new();
    for item in Punctuated::<InputArg, Token![,]>::parse_terminated(&body)? {
        match item {
            InputArg::Port(port) => ports.push(*port),
            InputArg::Config(ty) if is_input => args.config = Some(*ty),
            InputArg::Config(_) => {
                return Err(body.error("only inputs can name a config type"));
            }
        }
    }
    if is_input {
        args.inputs = Some(ports);
    } else {
        args.outputs = Some(ports);
    }
    Ok(())
}

fn parse_generics(input: ParseStream<'_>) -> Result<GenericsArg> {
    let body;
    parenthesized!(body in input);
    let param: Ident = body.parse()?;
    let mut instances = Vec::new();
    while !body.is_empty() {
        body.parse::<Token![,]>()?;
        if body.is_empty() {
            break;
        }
        let word: Ident = body.parse()?;
        if word != "instance" {
            return Err(Error::new(
                word.span(),
                "expected instance(Type, \"suffix\")",
            ));
        }
        let inner;
        parenthesized!(inner in body);
        let ty = inner.parse()?;
        inner.parse::<Token![,]>()?;
        instances.push(Instance {
            ty,
            suffix: inner.parse()?,
        });
    }
    Ok(GenericsArg { param, instances })
}

fn parse_conversion(input: ParseStream<'_>) -> Result<ConversionArg> {
    let mut conversion = ConversionArg {
        cost: 1,
        lossy: false,
    };
    if !input.peek(syn::token::Paren) {
        return Ok(conversion);
    }
    let body;
    parenthesized!(body in input);
    while !body.is_empty() {
        let key: Ident = body.parse()?;
        match key.to_string().as_str() {
            "cost" => conversion.cost = value::<LitInt>(&body)?.base10_parse()?,
            "lossy" => conversion.lossy = true,
            _ => return Err(Error::new(key.span(), "unsupported conversion argument")),
        }
        if !body.is_empty() {
            body.parse::<Token![,]>()?;
        }
    }
    Ok(conversion)
}

pub(crate) fn parse_backend(input: ParseStream<'_>) -> Result<BackendArg> {
    input.parse::<Token![=]>()?;
    let path = input.call(Path::parse_mod_style)?;
    let mut args = TokenStream::new();
    if input.peek(syn::token::Paren) {
        let body;
        parenthesized!(body in input);
        args = body.parse()?;
    }
    Ok(BackendArg { path, args })
}

fn parse_memory(input: ParseStream<'_>) -> Result<Vec<(Ident, LitStr)>> {
    let body;
    parenthesized!(body in input);
    let mut memory = Vec::new();
    while !body.is_empty() {
        let port: Ident = body.parse()?;
        memory.push((port, value(&body)?));
        if !body.is_empty() {
            body.parse::<Token![,]>()?;
        }
    }
    Ok(memory)
}

impl Parse for NodeArgs {
    fn parse(input: ParseStream<'_>) -> Result<NodeArgs> {
        let mut args = NodeArgs::default();
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            match key.to_string().as_str() {
                "id" => args.id = Some(value(input)?),
                "summary" => args.summary = Some(value(input)?),
                "description" => args.description = Some(value(input)?),
                "inputs" => parse_ports(&mut args, input, true)?,
                "outputs" => parse_ports(&mut args, input, false)?,
                "fire" => args.fire = Some(value(input)?),
                "reusable" => args.reusable = true,
                "shareable" => args.shareable = true,
                "conversion" => args.conversion = Some(parse_conversion(input)?),
                "state" => {
                    let body;
                    parenthesized!(body in input);
                    args.state = Some(body.parse()?);
                }
                "generics" => args.generics = Some(parse_generics(input)?),
                "backend" => args.backend = Some(parse_backend(input)?),
                "memory" => args.memory = parse_memory(input)?,
                _ => return Err(Error::new(key.span(), "unsupported #[node] argument")),
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(args)
    }
}

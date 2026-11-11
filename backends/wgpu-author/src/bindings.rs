//! Turns what `#[node]` passes for a const body into a `BodyRecord` holding a `ShaderSpec`.

use proc_macro2::{Span, TokenStream};
use quote::{ToTokens, quote};
use syn::parse::{Parse, ParseStream, Parser};
use syn::punctuated::Punctuated;
use syn::{Error, Expr, ExprLit, Ident, Lit, LitInt, LitStr, Result, Token, Type, parenthesized};

use crate::infer::{self, Kind};

/// The call `#[node]` makes: `key = value` pairs in a fixed vocabulary.
struct Call {
    kind: String,
    name: TokenStream,
    args: TokenStream,
    init: Option<TokenStream>,
    decl: Option<Ident>,
}

impl Parse for Call {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut call = Call {
            kind: String::new(),
            name: TokenStream::new(),
            args: TokenStream::new(),
            init: None,
            decl: None,
        };
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            let group = |input: ParseStream<'_>| -> Result<TokenStream> {
                let inner;
                parenthesized!(inner in input);
                inner.parse()
            };
            match key.to_string().as_str() {
                "kind" => {
                    call.kind = if input.parse::<Token![const]>().is_ok() {
                        "const".into()
                    } else if input.parse::<Token![fn]>().is_ok() {
                        "fn".into()
                    } else {
                        input.parse::<Ident>()?.to_string()
                    };
                }
                "name" => call.name = group(input)?,
                "args" => call.args = group(input)?,
                "init" => call.init = Some(group(input)?),
                "params" | "bind" => drop(group(input)?),
                "ret" => drop(input.parse::<Type>()?),
                "decl" => call.decl = Some(input.parse()?),
                _ => return Err(Error::new(key.span(), "not a key #[node] passes")),
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(call)
    }
}

/// What the author wrote in `backend = daedalus_wgpu_author(..)`.
#[derive(Default)]
struct Args {
    ports: Vec<(Ident, Type)>,
    entry: Option<LitStr>,
    count: Option<[u32; 3]>,
    per: Option<Ident>,
    /// The body is the author's own recording, not a shader.
    record: bool,
    /// The body is a per-pixel function, which can merge with its neighbours.
    pixel: bool,
}

fn parse_args(input: ParseStream<'_>) -> Result<Args> {
    let mut args = Args::default();
    while !input.is_empty() {
        let name: Ident = input.parse()?;
        let bare = input.is_empty() || input.peek(Token![,]);
        if name == "record" && bare {
            args.record = true;
        } else if name == "pixel" && bare {
            args.pixel = true;
        } else if input.parse::<Token![:]>().is_ok() {
            args.ports.push((name, input.parse()?));
        } else {
            input.parse::<Token![=]>()?;
            match name.to_string().as_str() {
                "entry" => args.entry = Some(input.parse()?),
                "per" => args.per = Some(input.parse()?),
                "count" => {
                    let inner;
                    syn::bracketed!(inner in input);
                    let sizes = Punctuated::<LitInt, Token![,]>::parse_terminated(&inner)?;
                    let mut count = [1u32; 3];
                    if sizes.is_empty() || sizes.len() > 3 {
                        return Err(Error::new(name.span(), "`count` is one to three numbers"));
                    }
                    for (slot, size) in count.iter_mut().zip(&sizes) {
                        *slot = size.base10_parse()?;
                    }
                    args.count = Some(count);
                }
                _ => {
                    return Err(Error::new(
                        name.span(),
                        "a shader node takes `port: Type`, `entry = \"..\"`, `count = [x, y, z]` and `per = buffer`",
                    ));
                }
            }
        }
        if !input.is_empty() {
            input.parse::<Token![,]>()?;
        }
    }
    Ok(args)
}

/// The WGSL behind a const's initializer: a string, or `include_str!` of a file beside the
/// source file.
fn source(init: TokenStream) -> Result<(String, Span)> {
    let expr: Expr = syn::parse2(init)?;
    let span = syn::spanned::Spanned::span(&expr);
    match expr {
        Expr::Lit(ExprLit {
            lit: Lit::Str(text),
            ..
        }) => Ok((text.value(), text.span())),
        Expr::Macro(call) if call.mac.path.is_ident("include_str") => {
            let path: LitStr = call.mac.parse_body()?;
            let here = path.span().unwrap().local_file().ok_or_else(|| {
                Error::new(
                    path.span(),
                    "can't tell which file this is in, to find the shader",
                )
            })?;
            let file = here.parent().unwrap_or(&here).join(path.value());
            let text = std::fs::read_to_string(&file).map_err(|error| {
                Error::new(
                    path.span(),
                    format!("can't read {}: {error}", file.display()),
                )
            })?;
            Ok((text, path.span()))
        }
        _ => Err(Error::new(
            span,
            "a shader node's const is a string of WGSL, or include_str! of a file of it",
        )),
    }
}

fn kind_tokens(kind: &Kind) -> TokenStream {
    let shader = quote!(::daedalus_wgpu::shader);
    match *kind {
        Kind::Storage { read_only, stride } => {
            quote!(#shader::BindingKind::Storage { read_only: #read_only, stride: #stride })
        }
        Kind::Uniform { size } => quote!(#shader::BindingKind::Uniform { size: #size }),
        Kind::Texture { sample } => {
            let sample = Ident::new(sample, Span::call_site());
            quote!(#shader::BindingKind::Texture { sample: #shader::SampleKind::#sample })
        }
        Kind::StorageTexture { format, access } => {
            let format = Ident::new(format, Span::call_site());
            let access = Ident::new(access, Span::call_site());
            quote!(#shader::BindingKind::StorageTexture {
                format: ::daedalus_wgpu::Format::#format,
                access: #shader::TextureAccess::#access,
            })
        }
        Kind::Sampler { comparison } => {
            quote!(#shader::BindingKind::Sampler { comparison: #comparison })
        }
    }
}

/// A body that is the author's own recording: the const is a `RecordBody`, and there is no
/// WGSL to read. Its ports are its inputs and outputs in the order the node declares them.
fn recording(name: &TokenStream, args: &Args) -> TokenStream {
    let ports = args.ports.iter().map(|(port, ty)| {
        let port = port.to_string();
        quote!((
            #port,
            ::daedalus_registry::port_type::<#ty>(),
        ))
    });
    quote! {{
        const BODY: ::daedalus_wgpu::record::RecordBody = #name;
        ::daedalus_registry::BodyRecord {
            backend: "wgpu",
            at: ::daedalus_registry::SourceAt(::core::concat!(::core::file!(), ":", ::core::line!())),
            needs: &[],
            state: ::core::option::Option::None,
            ports: &[#(#ports),*],
            memory: &[],
            code: &BODY,
            text: "",
        }
    }}
}

/// A per-pixel body: the record holds a `PixelSpec`, which the backend merges with the pixel
/// bodies around it when it compiles a region.
fn pixel_body(
    name: &TokenStream,
    decl: &Ident,
    args: &Args,
    pixel: &crate::pixel::Pixel,
    text: &str,
) -> TokenStream {
    let ports = args.ports.iter().map(|(port, ty)| {
        let port = port.to_string();
        quote!((
            #port,
            ::daedalus_registry::port_type::<#ty>(),
        ))
    });
    let merge = quote!(::daedalus_wgpu::merge);
    let label = name.to_token_stream().to_string();
    let (names, params) = (&pixel.names, &pixel.params);
    let (input, output) = (&pixel.input, &pixel.output);
    let (input_stride, output_stride) = (pixel.input_stride, pixel.output_stride);
    let config = match &pixel.config {
        Some((ty, size)) => quote!(::core::option::Option::Some((#ty, #size))),
        None => quote!(::core::option::Option::None),
    };
    let sides = pixel.sides.iter().map(|side| {
        let (name, ty, stride) = (&side.name, &side.ty, side.stride);
        quote!(#merge::PixelSide { name: #name, ty: #ty, stride: #stride })
    });
    let hash = crate::pixel::source_hash(text);
    quote! {{
        const PARAMS: &[&str] = &[#(#params),*];
        // Which parameter each declared port feeds. A port with no parameter of its name
        // fails here, and so does a node with more or fewer than one output.
        const INPUTS: [u32; #decl.inputs.len()] = #merge::pixel_ports(#decl.inputs, PARAMS);
        const _: () = ::core::assert!(
            #decl.outputs.len() == 1,
            "a pixel body has exactly one output: the pixel `apply` returns"
        );
        ::daedalus_registry::BodyRecord {
            backend: "wgpu",
            at: ::daedalus_registry::SourceAt(::core::concat!(::core::file!(), ":", ::core::line!())),
            needs: &[],
            state: ::core::option::Option::None,
            ports: &[#(#ports),*],
            memory: &[],
            code: &#merge::PixelSpec {
                name: #label,
                source: #name,
                names: &[#(#names),*],
                input: #input,
                output: #output,
                input_stride: #input_stride,
                output_stride: #output_stride,
                config: #config,
                sides: &[#(#sides),*],
                params: PARAMS,
                inputs: &INPUTS,
                source_hash: #hash,
            },
            text: #name,
        }
    }}
}

pub(crate) fn expand(input: TokenStream) -> Result<TokenStream> {
    let call: Call = syn::parse2(input)?;
    let name = call.name;
    if call.kind != "const" {
        return Err(Error::new_spanned(
            &name,
            "a shader node is a const holding WGSL, not a function",
        ));
    }
    let init = call
        .init
        .ok_or_else(|| Error::new_spanned(&name, "#[node] passed no initializer for this const"))?;
    let decl = call
        .decl
        .ok_or_else(|| Error::new_spanned(&name, "#[node] passed no declaration for this const"))?;
    let args = parse_args.parse2(call.args)?;
    if args.record {
        return Ok(recording(&name, &args));
    }
    let (text, at) = source(init)?;
    if args.pixel {
        let pixel = crate::pixel::read(&text).map_err(|error| Error::new(at, error))?;
        return Ok(pixel_body(&name, &decl, &args, &pixel, &text));
    }
    let entry = args.entry.as_ref().map(LitStr::value);
    let shader = infer::read(&text, entry.as_deref()).map_err(|error| Error::new(at, error))?;

    let find = |name: &Ident| {
        shader
            .bindings
            .iter()
            .find(|binding| name == &binding.name)
            .ok_or_else(|| {
                let known: Vec<&str> = shader.bindings.iter().map(|b| &*b.name).collect();
                Error::new(
                    name.span(),
                    format!(
                        "the shader has no binding named `{name}`: it has {}",
                        known.join(", ")
                    ),
                )
            })
    };
    let mut ports = Vec::new();
    for (port, ty) in &args.ports {
        if matches!(find(port)?.kind, Kind::Sampler { .. }) {
            return Err(Error::new(port.span(), "a sampler isn't a port"));
        }
        let port = port.to_string();
        ports.push(quote!((
            #port,
            ::daedalus_registry::port_type::<#ty>(),
        )));
    }

    let shader_path = quote!(::daedalus_wgpu::shader);
    let dispatch = match (&args.count, &args.per) {
        (Some(_), Some(per)) => {
            return Err(Error::new(per.span(), "give `count` or `per`, not both"));
        }
        (Some([x, y, z]), None) => quote!(#shader_path::Dispatch::Counts([#x, #y, #z])),
        (None, per) => {
            let chosen = match per {
                Some(per) => find(per)?,
                None => shader
                    .bindings
                    .iter()
                    .find(|binding| {
                        matches!(binding.kind, Kind::Storage { read_only: false, stride } if stride > 0)
                    })
                    .ok_or_else(|| {
                        Error::new(
                            at,
                            "the shader writes no storage buffer to size the run by: say how many \
                             workgroups with `count = [x, y, z]`, or which buffer with `per = name`",
                        )
                    })?,
            };
            let Kind::Storage { stride, .. } = chosen.kind else {
                return Err(Error::new(
                    at,
                    format!("`{}` isn't a storage buffer", chosen.name),
                ));
            };
            if stride == 0 {
                return Err(Error::new(
                    at,
                    format!("`{}` isn't an array, so it has no elements", chosen.name),
                ));
            }
            let index = chosen.index;
            quote!(#shader_path::Dispatch::PerElement { binding: #index, stride: #stride })
        }
    };

    let bindings = shader.bindings.iter().map(|binding| {
        let (index, name, kind) = (binding.index, &binding.name, kind_tokens(&binding.kind));
        quote!(#shader_path::BindingSpec { index: #index, name: #name, kind: #kind })
    });
    let [x, y, z] = shader.workgroup;
    let count = |want: fn(&Kind) -> bool| shader.bindings.iter().filter(|b| want(&b.kind)).count();
    let needs = [
        format!("workgroup={x}x{y}x{z}"),
        format!("workgroup_storage={}", shader.workgroup_storage),
        format!(
            "storage_buffers={}",
            count(|kind| matches!(kind, Kind::Storage { .. }))
        ),
        format!(
            "storage_textures={}",
            count(|kind| matches!(kind, Kind::StorageTexture { .. }))
        ),
    ];
    let entry = &shader.entry;
    let label = name.to_token_stream().to_string();
    Ok(quote! {{
        const BINDINGS: &[#shader_path::BindingSpec] = &[#(#bindings),*];
        // Which binding each declared port feeds. A port with no binding of its name fails here.
        const INPUTS: [u32; #decl.inputs.len()] = #shader_path::port_bindings(#decl.inputs, BINDINGS);
        const OUTPUTS: [u32; #decl.outputs.len()] =
            #shader_path::port_bindings(#decl.outputs, BINDINGS);
        ::daedalus_registry::BodyRecord {
            backend: "wgpu",
            at: ::daedalus_registry::SourceAt(::core::concat!(::core::file!(), ":", ::core::line!())),
            needs: &[#(#needs),*],
            state: ::core::option::Option::None,
            ports: &[#(#ports),*],
            memory: &[],
            code: &#shader_path::ShaderSpec {
                name: #label,
                entry: #entry,
                source: #name,
                workgroup: [#x, #y, #z],
                bindings: BINDINGS,
                dispatch: #dispatch,
                inputs: &INPUTS,
                outputs: &OUTPUTS,
            },
            text: #name,
        }
    }})
}

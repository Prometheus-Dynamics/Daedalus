//! `#[derive(Type)]`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Error, Expr, Fields, Ident, Index, Member, Result, Type};

use crate::local_key_check;

fn expr_of(ty: &Type) -> TokenStream {
    quote!(<#ty as ::daedalus_registry::__private::Typed>::EXPR)
}

/// The expression for a set of fields, plus the checks on their types.
fn fields_expr(fields: &Fields, checks: &mut TokenStream) -> TokenStream {
    let p = quote!(::daedalus_registry::__private);
    for field in fields {
        checks.extend(local_key_check(&field.ty));
    }
    match fields {
        Fields::Named(named) => {
            let rows = named.named.iter().map(|field| {
                let name = field.ident.as_ref().map(ToString::to_string);
                let ty = expr_of(&field.ty);
                quote!(#p::Field { name: #name, ty: #ty })
            });
            quote!({
                const FIELDS: &[#p::Field] = &[#(#rows),*];
                #p::TypeExpr::Struct(FIELDS)
            })
        }
        Fields::Unnamed(unnamed) if unnamed.unnamed.len() == 1 => expr_of(&unnamed.unnamed[0].ty),
        Fields::Unnamed(unnamed) => {
            let rows = unnamed.unnamed.iter().map(|field| expr_of(&field.ty));
            quote!({
                const ITEMS: &[#p::TypeExpr] = &[#(#rows),*];
                #p::TypeExpr::Tuple(ITEMS)
            })
        }
        Fields::Unit => quote!(#p::TypeExpr::Scalar(#p::Scalar::Unit)),
    }
}

fn key_expr(input: &DeriveInput) -> Result<TokenStream> {
    let mut explicit = None;
    for attr in &input.attrs {
        if attr.path().is_ident("type_key") {
            explicit = Some(attr.parse_args::<Expr>()?);
        }
    }
    Ok(match explicit {
        Some(key) => quote!(#key),
        None => {
            let name = input.ident.to_string().to_lowercase();
            quote!(::core::concat!("rust:", ::core::module_path!(), "::", #name))
        }
    })
}

pub(crate) fn derive(input: DeriveInput) -> Result<TokenStream> {
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            "#[derive(Type)] takes no generics",
        ));
    }
    let p = quote!(::daedalus_registry::__private);
    let name = &input.ident;
    let structural = input
        .attrs
        .iter()
        .any(|attr| attr.path().is_ident("structural"));
    if structural
        && input
            .attrs
            .iter()
            .any(|attr| attr.path().is_ident("type_key"))
    {
        return Err(Error::new_spanned(
            name,
            "a #[structural] type is compared by what it is made of, so it has no #[type_key]",
        ));
    }
    let key = match structural {
        true => quote!("structural"),
        false => key_expr(&input)?,
    };
    let mut checks = TokenStream::new();
    let expr = match &input.data {
        Data::Struct(data) => fields_expr(&data.fields, &mut checks),
        Data::Enum(data) => {
            let mut rows = Vec::new();
            for variant in &data.variants {
                let text = variant.ident.to_string();
                rows.push(if matches!(variant.fields, Fields::Unit) {
                    quote!(#p::Variant { name: #text, payload: ::core::option::Option::None })
                } else {
                    let payload = fields_expr(&variant.fields, &mut checks);
                    quote!(#p::Variant { name: #text, payload: ::core::option::Option::Some(#payload) })
                });
            }
            quote!({
                const VARIANTS: &[#p::Variant] = &[#(#rows),*];
                #p::TypeExpr::Enum(VARIANTS)
            })
        }
        Data::Union(data) => {
            return Err(Error::new_spanned(
                data.union_token,
                "#[derive(Type)] can't describe a union",
            ));
        }
    };
    Ok(quote! {
        #checks

        impl #p::Typed for #name {
            const KEY: #p::TypeKey = #p::TypeKey::new(#key);
            const EXPR: #p::TypeExpr = #expr;
            const STRUCTURAL: bool = #structural;
        }
    })
}

/// The members and types of a set of fields, in order.
fn members(fields: &Fields) -> Vec<(Member, &Type)> {
    fields
        .iter()
        .enumerate()
        .map(|(at, field)| {
            let member = match &field.ident {
                Some(ident) => Member::Named(ident.clone()),
                None => Member::Unnamed(Index::from(at)),
            };
            (member, &field.ty)
        })
        .collect()
}

/// Names to bind fields to, one per field.
fn binds(count: usize) -> Vec<Ident> {
    (0..count)
        .map(|at| quote::format_ident!("__field{at}"))
        .collect()
}

/// `WireForm` for the type: its fields' wire forms in order, as its type expression lists them.
/// A newtype is its one field's form; an enum is its variant's `u32` index, then the payload.
pub(crate) fn wire_form(input: &DeriveInput) -> Result<TokenStream> {
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            "#[derive(WireForm)] takes no generics",
        ));
    }
    Ok(wire_form_impl(input))
}

fn wire_form_impl(input: &DeriveInput) -> TokenStream {
    let p = quote!(::daedalus_registry::__private);
    let w = quote!(#p::WireForm);
    let name = &input.ident;
    match &input.data {
        Data::Struct(data) if matches!(&data.fields, Fields::Unnamed(f) if f.unnamed.len() == 1) => {
            let ty = &data.fields.iter().next().map(|field| &field.ty);
            quote! {
                impl #w for #name {
                    const FLAT: bool = <#ty as #w>::FLAT
                        && ::core::mem::size_of::<Self>() == ::core::mem::size_of::<#ty>();
                    fn wire_len(&self) -> usize { #w::wire_len(&self.0) }
                    fn write_wire(&self, out: &mut [u8]) -> usize { #w::write_wire(&self.0, out) }
                    fn read_wire(bytes: &[u8]) -> ::core::option::Option<(Self, usize)> {
                        <#ty as #w>::read_wire(bytes).map(|(value, used)| (Self(value), used))
                    }
                    fn cell_len(&self) -> usize { #w::cell_len(&self.0) }
                    fn write_cell(&self, out: &mut [u8]) -> usize { #w::write_cell(&self.0, out) }
                    fn read_cell(bytes: &[u8]) -> ::core::option::Option<Self> {
                        <#ty as #w>::read_cell(bytes).map(Self)
                    }
                }
            }
        }
        Data::Struct(data) => {
            let fields = members(&data.fields);
            let flat = fields.iter().map(|(member, ty)| {
                quote!((<#ty as #w>::FLAT, ::core::mem::offset_of!(Self, #member), ::core::mem::size_of::<#ty>()))
            });
            let lens = fields
                .iter()
                .map(|(member, _)| quote!(+ #w::wire_len(&self.#member)));
            let writes = fields
                .iter()
                .map(|(member, _)| quote!(at += #w::write_wire(&self.#member, &mut out[at..]);));
            let names = binds(fields.len());
            let reads = fields.iter().zip(&names).map(|((_, ty), bind)| {
                quote!(let (#bind, used) = <#ty as #w>::read_wire(&bytes[at..])?; at += used;)
            });
            let build = fields
                .iter()
                .zip(&names)
                .map(|((member, _), bind)| quote!(#member: #bind));
            quote! {
                impl #w for #name {
                    const FLAT: bool = #p::flat_layout(::core::mem::size_of::<Self>(), &[#(#flat),*]);
                    #[inline]
                    fn wire_len(&self) -> usize { 0 #(#lens)* }
                    #[inline]
                    #[allow(unused_mut)]
                    fn write_wire(&self, out: &mut [u8]) -> usize {
                        let mut at = 0;
                        #(#writes)*
                        at
                    }
                    #[inline]
                    #[allow(unused_mut, unused_variables)]
                    fn read_wire(bytes: &[u8]) -> ::core::option::Option<(Self, usize)> {
                        let mut at = 0;
                        #(#reads)*
                        ::core::option::Option::Some((Self { #(#build),* }, at))
                    }
                }
            }
        }
        Data::Enum(data) => {
            let mut lens = Vec::new();
            let mut writes = Vec::new();
            let mut reads = Vec::new();
            for (index, variant) in data.variants.iter().enumerate() {
                let index = index as u32;
                let ident = &variant.ident;
                let fields = members(&variant.fields);
                let names = binds(fields.len());
                let pattern = match &variant.fields {
                    Fields::Named(_) => {
                        let pairs = fields.iter().zip(&names).map(|((m, _), b)| quote!(#m: #b));
                        quote!(Self::#ident { #(#pairs),* })
                    }
                    Fields::Unnamed(_) => quote!(Self::#ident(#(#names),*)),
                    Fields::Unit => quote!(Self::#ident),
                };
                lens.push(quote!(#pattern => 4 #(+ #w::wire_len(#names))*));
                writes.push(quote!(#pattern => {
                    let mut at = 0;
                    out[..4].copy_from_slice(&#index.to_le_bytes());
                    at += 4;
                    #(at += #w::write_wire(#names, &mut out[at..]);)*
                    at
                }));
                let field_reads = fields.iter().zip(&names).map(|((_, ty), bind)| {
                    quote!(let (#bind, used) = <#ty as #w>::read_wire(&bytes[at..])?; at += used;)
                });
                reads.push(quote!(#index => {
                    #(#field_reads)*
                    ::core::option::Option::Some((#pattern, at))
                }));
            }
            quote! {
                impl #w for #name {
                    fn wire_len(&self) -> usize {
                        match self { #(#lens,)* }
                    }
                    #[allow(unused_mut)]
                    fn write_wire(&self, out: &mut [u8]) -> usize {
                        match self { #(#writes)* }
                    }
                    #[allow(unused_mut)]
                    fn read_wire(bytes: &[u8]) -> ::core::option::Option<(Self, usize)> {
                        let index = u32::from_le_bytes(bytes.get(..4)?.try_into().ok()?);
                        let mut at = 4;
                        match index {
                            #(#reads)*
                            _ => ::core::option::Option::None,
                        }
                    }
                }
            }
        }
        Data::Union(_) => TokenStream::new(),
    }
}

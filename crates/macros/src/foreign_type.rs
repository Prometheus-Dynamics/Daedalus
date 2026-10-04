//! Foreign interfaces in the node and plugin macros: foreign view inputs (`FrameView<'_>`,
//! `ForeignRef<'_, I>`) and `#[plugin(foreign_providers(Owner => Interface))]`.

use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{MetaList, Path, Token, Type};

use crate::helpers::last_ident_is;
use crate::type_expr::leaf_type_key;

/// Whether a by-value node parameter is a foreign view: the payload carries a `ForeignHandle`
/// and the macro fetches it with `NodeIo::get_foreign` instead of a typed downcast. Recognized
/// by name (`FrameView`, `ForeignRef`), since aliases are invisible to macros.
pub(crate) fn is_foreign_view(ty: &Type) -> bool {
    last_ident_is(ty, "FrameView") || last_ident_is(ty, "ForeignRef")
}

/// `Owner => Interface`.
pub(crate) struct ForeignProvider {
    owner: Type,
    interface: Path,
}

impl Parse for ForeignProvider {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let owner = input.parse()?;
        input.parse::<Token![=>]>()?;
        Ok(Self {
            owner,
            interface: input.parse()?,
        })
    }
}

/// `foreign_providers(Owner => Interface, ...)`.
pub(crate) fn collect_foreign_providers(list: &MetaList) -> syn::Result<Vec<ForeignProvider>> {
    list.parse_args_with(Punctuated::<ForeignProvider, Token![,]>::parse_terminated)
        .map(|providers| providers.into_iter().collect())
}

/// Install statements registering each provider under the owner's own key.
pub(crate) fn register_foreign_providers(providers: &[ForeignProvider]) -> Vec<TokenStream> {
    providers
        .iter()
        .map(|ForeignProvider { owner, interface }| {
            let key = leaf_type_key(owner);
            quote! { registry.register_foreign_provider_as::<#owner, #interface>(#key)?; }
        })
        .collect()
}

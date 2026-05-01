use proc_macro2::TokenStream;
use quote::quote;
use syn::LitStr;

pub(super) struct FinalTokens {
    pub(super) input: syn::ItemFn,
    pub(super) struct_ident: syn::Ident,
    pub(super) id: LitStr,
    pub(super) node_decl_fn: TokenStream,
    pub(super) boundary_contracts_fn: TokenStream,
    pub(super) handler_registry_fn: TokenStream,
    pub(super) register_fn: TokenStream,
    pub(super) capability_helper: TokenStream,
    pub(super) node_install_impl: TokenStream,
    pub(super) inputs_ident: syn::Ident,
    pub(super) handle_input_idents: Vec<syn::Ident>,
    pub(super) runtime_crate: TokenStream,
    pub(super) handle_input_names: Vec<LitStr>,
    pub(super) outputs_ident: syn::Ident,
    pub(super) output_idents: Vec<syn::Ident>,
    pub(super) output_names: Vec<LitStr>,
    pub(super) handle_ident: syn::Ident,
}

pub(super) fn render_final(tokens: FinalTokens) -> TokenStream {
    let FinalTokens {
        input,
        struct_ident,
        id,
        node_decl_fn,
        boundary_contracts_fn,
        handler_registry_fn,
        register_fn,
        capability_helper,
        node_install_impl,
        inputs_ident,
        handle_input_idents,
        runtime_crate,
        handle_input_names,
        outputs_ident,
        output_idents,
        output_names,
        handle_ident,
    } = tokens;

    quote! {
        #input

        pub struct #struct_ident;

        impl #struct_ident {
            pub const ID: &'static str = #id;

            #node_decl_fn

            #boundary_contracts_fn

            #handler_registry_fn

            #register_fn

            #capability_helper
        }

        #node_install_impl

        #[derive(Clone, Debug)]
        pub struct #inputs_ident {
            #(pub #handle_input_idents: #runtime_crate::handles::PortHandle),*
        }

        impl #inputs_ident {
            pub fn new(alias: &str) -> Self {
                Self {
                    #(#handle_input_idents: #runtime_crate::handles::PortHandle::new(alias, #handle_input_names)),*
                }
            }
        }

        #[derive(Clone, Debug)]
        pub struct #outputs_ident {
            #(pub #output_idents: #runtime_crate::handles::PortHandle),*
        }

        impl #outputs_ident {
            pub fn new(alias: &str) -> Self {
                Self {
                    #(#output_idents: #runtime_crate::handles::PortHandle::new(alias, #output_names)),*
                }
            }
        }

        #[derive(Clone, Debug)]
        pub struct #handle_ident {
            pub spec: #runtime_crate::handles::NodeHandle,
            pub inputs: #inputs_ident,
            pub outputs: #outputs_ident,
        }

        impl #handle_ident {
            pub fn new() -> Self {
                Self::new_with_id(#struct_ident::ID)
            }

            pub fn new_with_id(id: impl Into<String>) -> Self {
                let id_str = id.into();
                let spec = #runtime_crate::handles::NodeHandle::new(id_str);
                let alias = spec.alias_name().to_string();
                Self {
                    spec,
                    inputs: #inputs_ident::new(&alias),
                    outputs: #outputs_ident::new(&alias),
                }
            }

            pub fn with_prefix(mut self, prefix: &str) -> Self {
                let id = #runtime_crate::apply_node_prefix(prefix, self.spec.id());
                self.spec = #runtime_crate::handles::NodeHandle::new(id.clone()).alias(id.clone());
                self.inputs = #inputs_ident::new(&id);
                self.outputs = #outputs_ident::new(&id);
                self
            }

            pub fn alias(mut self, alias: impl Into<String>) -> Self {
                let a = alias.into();
                self.spec = self.spec.alias(a.clone());
                self.inputs = #inputs_ident::new(&a);
                self.outputs = #outputs_ident::new(&a);
                self
            }
        }

        impl #runtime_crate::handles::NodeHandleLike for #handle_ident {
            fn id(&self) -> &str { self.spec.id() }
            fn alias(&self) -> &str { self.spec.alias_name() }
        }

        impl #struct_ident {
            pub fn handle() -> #handle_ident {
                #handle_ident::new()
            }
        }

    }
}

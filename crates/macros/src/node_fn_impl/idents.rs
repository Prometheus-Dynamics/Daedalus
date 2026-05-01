use proc_macro2::Span;

pub(super) fn is_rust_keyword(ident: &str) -> bool {
    matches!(
        ident,
        "as" | "break"
            | "const"
            | "continue"
            | "crate"
            | "else"
            | "enum"
            | "extern"
            | "false"
            | "fn"
            | "for"
            | "if"
            | "impl"
            | "in"
            | "let"
            | "loop"
            | "match"
            | "mod"
            | "move"
            | "mut"
            | "pub"
            | "ref"
            | "return"
            | "self"
            | "Self"
            | "static"
            | "struct"
            | "super"
            | "trait"
            | "true"
            | "type"
            | "unsafe"
            | "use"
            | "where"
            | "while"
            | "async"
            | "await"
            | "dyn"
            | "abstract"
            | "become"
            | "box"
            | "do"
            | "final"
            | "macro"
            | "override"
            | "priv"
            | "try"
            | "typeof"
            | "unsized"
            | "virtual"
            | "yield"
    )
}

pub(super) fn node_struct_ident(fn_ident: &syn::Ident) -> syn::Ident {
    let mut out = String::new();
    let mut capitalize = true;
    for ch in fn_ident.to_string().chars() {
        if ch == '_' {
            capitalize = true;
            continue;
        }
        if capitalize {
            out.extend(ch.to_uppercase());
            capitalize = false;
        } else {
            out.push(ch);
        }
    }
    if out.is_empty() || !out.ends_with("Node") {
        out.push_str("Node");
    }
    syn::Ident::new(&out, Span::call_site())
}

pub(super) fn port_ident(name: &str) -> syn::Ident {
    if is_rust_keyword(name) {
        syn::Ident::new_raw(name, Span::call_site())
    } else {
        syn::Ident::new(name, Span::call_site())
    }
}

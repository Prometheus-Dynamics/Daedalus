use std::{env, fs, path::PathBuf};

use daedalus_wgsl_infer::{InferredSpec, infer_bindings, infer_workgroup_size};
use proc_macro2::Span;

use super::types::Spec;

pub fn infer_spec(spec: &Spec) -> syn::Result<InferredSpec> {
    let manifest = env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".to_string());
    let candidates = vec![
        PathBuf::from(&manifest).join(&spec.src),
        PathBuf::from(&manifest).join("examples").join(&spec.src),
    ];

    let mut last_err: Option<std::io::Error> = None;
    let mut found_path = None;
    let mut src = None;
    for path in candidates {
        match fs::read_to_string(&path) {
            Ok(data) => {
                found_path = Some(path);
                src = Some(data);
                break;
            }
            Err(e) => last_err = Some(e),
        }
    }

    let (_path, src) = match (found_path, src) {
        (Some(p), Some(s)) => (p, s),
        _ => {
            let reason = last_err
                .map(|e| format!("{e}"))
                .unwrap_or_else(|| "file not found".to_string());
            return Err(syn::Error::new(
                Span::call_site(),
                format!("failed to read WGSL source {}: {}", spec.src, reason),
            ));
        }
    };

    let bindings = infer_bindings(&src);
    if bindings.is_empty() {
        return Err(syn::Error::new(
            Span::call_site(),
            format!(
                "no bindings inferred from WGSL {}; add gpu(binding = N) attributes",
                spec.src
            ),
        ));
    }

    let workgroup = infer_workgroup_size(&src);
    Ok(InferredSpec {
        workgroup,
        bindings,
    })
}

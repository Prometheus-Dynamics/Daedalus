//! Registry dumps, as JSON without serde.

use std::fmt::Write as _;

use crate::{Registry, Status};

/// Write `registries` (and everything nested in them) as JSON to `path`, for `accel-matrix
/// --registry`. Nested tables without a `repo` take their parent's.
pub fn dump(path: impl AsRef<std::path::Path>, registries: &[&Registry]) -> std::io::Result<()> {
    if let Some(parent) = path.as_ref().parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, dump_json(registries))
}

/// The JSON [`dump`] writes: `{"ops": [...], "kernels": [...], "exempt": [...]}`.
pub fn dump_json(registries: &[&Registry]) -> String {
    let (mut ops, mut kernels, mut exempt) = (Vec::new(), Vec::new(), Vec::new());
    for registry in registries {
        collect(registry, registry.repo, &mut ops, &mut kernels, &mut exempt);
    }
    format!(
        "{{\"ops\":[{}],\"kernels\":[{}],\"exempt\":[{}]}}\n",
        ops.join(","),
        kernels.join(","),
        exempt.join(",")
    )
}

fn collect(
    registry: &Registry,
    parent_repo: &str,
    ops: &mut Vec<String>,
    kernels: &mut Vec<String>,
    exempt: &mut Vec<String>,
) {
    let repo = if registry.repo.is_empty() {
        parent_repo
    } else {
        registry.repo
    };
    for op in registry.ops {
        ops.push(format!(
            "{{\"repo\":{},\"table\":{},\"id\":{},\"summary\":{},\"scalar\":{},\"per_element\":{},\"candidates\":[{}],\"call_sites\":[{}],\"only\":[{}],\"only_reason\":{}}}",
            json_str(repo),
            json_str(registry.name),
            json_str(op.id),
            json_str(op.summary),
            json_str(op.scalar),
            op.per_element,
            op.candidates
                .iter()
                .map(|(b, exts)| format!(
                    "{{\"backend\":{},\"extensions\":[{}]}}",
                    json_str(b.name()),
                    ext_list(exts)
                ))
                .collect::<Vec<_>>()
                .join(","),
            op.call_sites
                .iter()
                .map(|c| format!(
                    "{{\"path\":{},\"calls\":{},\"forbid\":[{}]}}",
                    json_str(c.path),
                    json_str(c.calls),
                    c.forbid.iter().map(|f| json_str(f)).collect::<Vec<_>>().join(",")
                ))
                .collect::<Vec<_>>()
                .join(","),
            op.only
                .iter()
                .map(|b| json_str(b.name()))
                .collect::<Vec<_>>()
                .join(","),
            json_str(op.only_reason)
        ));
    }
    for k in registry.kernels {
        let (status, reason) = match k.status {
            Status::Implemented => ("implemented", ""),
            Status::Gap(r) => ("gap", r),
            Status::Impossible(r) => ("impossible", r),
            Status::NotDataParallel(r) => ("not_data_parallel", r),
            Status::Deferred(r) => ("deferred", r),
        };
        let mut line = String::new();
        let _ = write!(
            line,
            "{{\"repo\":{},\"op\":{},\"backend\":{},\"extensions\":[{}],\"variant\":{},\"core\":{},\"source\":{},\"test\":{},\"symbol\":{},\"status\":{},\"reason\":{}}}",
            json_str(repo),
            json_str(k.op),
            json_str(k.backend.name()),
            ext_list(k.extensions),
            k.variant.map_or_else(|| "null".into(), json_str),
            k.core,
            json_str(k.source),
            json_str(k.test),
            k.symbol.map_or_else(|| "null".into(), json_str),
            json_str(status),
            json_str(reason)
        );
        kernels.push(line);
    }
    for e in registry.exempt {
        exempt.push(format!(
            "{{\"repo\":{},\"path\":{},\"reason\":{}}}",
            json_str(repo),
            json_str(e.path),
            json_str(e.reason)
        ));
    }
    for child in registry.children {
        collect(child, repo, ops, kernels, exempt);
    }
}

fn ext_list(exts: &[crate::Ext]) -> String {
    exts.iter()
        .map(|e| json_str(e.name()))
        .collect::<Vec<_>>()
        .join(",")
}

/// A JSON string literal.
pub(crate) fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

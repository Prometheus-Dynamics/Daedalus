#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

mapfile -t async_files < <(
    {
        rg --files "$root_dir/crates" "$root_dir/examples/04_async" 2>/dev/null || true
    } | rg '(/async[^/]*\.rs$|/dispatch_async\.rs$|examples/04_async/.*\.rs$)'
)

if ((${#async_files[@]} == 0)); then
    echo "gpu async blocking check skipped: no async-facing Rust files found"
    exit 0
fi

pattern='WgpuBackend::new\(|WgpuBackend::new_with_staging_pool_config\(|\bselect_backend\(|\bdispatch_shader_with_bindings\(|\bdispatch_shader_with_options\(|\.dispatch_auto\(|\.dispatch_bindings\('

if rg -n "$pattern" "${async_files[@]}"; then
    echo "gpu async blocking check failed: async-facing code must use select_backend_async and *_async shader dispatch APIs" >&2
    exit 1
fi

echo "gpu async blocking check passed"

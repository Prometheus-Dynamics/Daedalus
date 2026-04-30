# Changelog

All notable changes to this workspace should be documented in this file.

The format is based on Keep a Changelog and this project follows Semantic Versioning.

## [2.0.0] - 2026-04-30

- Reworked the workspace around the new core/runtime/planner/transport architecture.
- Added the transport crate, capability registries, planner passes, typed adapters, and richer runtime execution paths.
- Expanded macro support for plugins, typed config, adapters, branch payloads, device metadata, and type keys.
- Refreshed FFI packaging across Rust, C/C++, Java, Node, and Python SDK surfaces.
- Moved standalone plugin examples under `examples/plugins` and removed the old root plugin example folder.
- Reorganized runnable examples into the top-level `examples` crate.
- Bumped all workspace crates and example plugin crates to `2.0.0`.

## [1.0.0] - 2026-04-19

- Standardized the workspace layout, docs, CI, linting, and helper scripts.
- Removed the `extensions/` tree and aligned the repo around crates, plugins, docs, testing, and Docker-backed facade validation.
- Centralized more workspace dependencies and documented the intentional `default-features = false` manifest exception in `crates/engine`.
- Added `scripts/check-file-sizes.sh`, `scripts/ci.sh`, and `scripts/repo-clean.sh`.

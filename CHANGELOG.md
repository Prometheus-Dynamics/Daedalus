# Changelog

All notable changes to this workspace should be documented in this file.

The format is based on Keep a Changelog and this project follows Semantic Versioning.

## [Unreleased]

- Document ongoing repo-level changes here before the next release cut.

## [1.0.0] - 2026-04-19

- Standardized the workspace layout, docs, CI, linting, and helper scripts.
- Removed the `extensions/` tree and aligned the repo around crates, plugins, docs, testing, and Docker-backed facade validation.
- Centralized more workspace dependencies and documented the intentional `default-features = false` manifest exception in `crates/engine`.
- Added `scripts/check-file-sizes.sh`, `scripts/ci.sh`, and `scripts/repo-clean.sh`.

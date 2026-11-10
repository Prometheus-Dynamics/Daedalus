#!/usr/bin/env bash
# The API documentation builds with no warning, every relative link in docs/ resolves, and no
# guide sends a reader to a path on dev.
set -euo pipefail
RUSTDOCFLAGS="-D warnings" cargo doc -q --workspace --no-deps
cargo test -q -p daedalus --test docs

#!/usr/bin/env bash
# The API documentation builds with no warning, every relative link in docs/ resolves, and no
# guide sends a reader to a path of the branch this one replaces.
set -euo pipefail
RUSTDOCFLAGS="-D warnings" cargo doc --locked -q --workspace --no-deps
cargo test --locked -q -p daedalus --test docs

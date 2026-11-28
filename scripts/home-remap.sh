# Sourced (scripts/ci.sh, scripts/check-home-remap.sh): maps the cargo home out of everything built.
#
#   source scripts/home-remap.sh && home_remap
#
# A dependency's panic locations and debug info name its source by the path cargo read it from,
# CARGO_HOME/registry/src/... or CARGO_HOME/git/checkouts/..., so a binary carried the builder's
# home (the lander's lane homes, a CI runner's, a developer's, user name included) and its bytes
# depended on who built it and where: the daemon's debug builds held 185 such paths. This adds
# `--remap-path-prefix=<the home>=/cargo` to CARGO_BUILD_RUSTFLAGS, for every profile, so those
# paths read /cargo/... whoever builds; scripts/check-home-remap.sh builds in two homes and cmps.
# cargo's own `trim-paths` profile option would do this, but it is not stable in the pinned toolchain.
#
# CARGO_BUILD_RUSTFLAGS is cargo's build.rustflags: RUSTFLAGS, or a target's rustflags, replace it.
# No shipped build sets those (the cross-target tests and the test step's fixtures do). A remap
# for another home, left by an outer call, is replaced, so a nested call (a step that runs ci.sh)
# builds with the same flags.
home_remap() {
  local home="${CARGO_HOME:-$HOME/.cargo}" word words kept=()
  if [[ $home =~ [[:space:]] ]]; then
    echo "home-remap: the cargo home '$home' holds whitespace, which CARGO_BUILD_RUSTFLAGS can't" >&2
    return 1
  fi
  read -ra words <<< "${CARGO_BUILD_RUSTFLAGS:-}"
  for word in "${words[@]}"; do
    [[ $word == --remap-path-prefix=*=/cargo ]] || kept+=("$word")
  done
  kept+=("--remap-path-prefix=$home=/cargo")
  export CARGO_BUILD_RUSTFLAGS="${kept[*]}"
}

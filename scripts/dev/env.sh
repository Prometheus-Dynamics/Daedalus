# Sourced from inside a checkout: the environment the lander's tools build in.
#
#   source scripts/dev/env.sh
#
# The working area is DAEDALUS_WORK (default /mnt/nvme-wd/daedalus), laid out as scripts/dev/setup.sh
# makes it: wt/ the clones, target/ the build directories (one a checkout, and the gate's lanes
# inside it), cache/ what can be thrown away (the seed, the validation layer, TMPDIR), logs/ the
# gate logs. A missing cache costs time, never a result.
work="${DAEDALUS_WORK:-/mnt/nvme-wd/daedalus}"
checkout="$(basename "$(git rev-parse --show-toplevel 2>/dev/null || pwd)")"
export DAEDALUS_WORK="$work" DAEDALUS_LOGS="$work/logs"
export CARGO_TARGET_DIR="$work/target/$checkout"
export TMPDIR="$work/cache/tmp"
export DAEDALUS_TARGET_SEED="$work/cache/seed"
export DAEDALUS_VVL_DIR="$work/cache/vvl/$("$(git rev-parse --show-toplevel)/scripts/dev/vvl-key.sh")/x86_64"
# The gate's cores: the rest of the machine is other sessions'.
export LAND_CPUS="${LAND_CPUS:-6-18}" CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-13}"
# Pushes and PRs go through gh's credentials.
export GIT_CONFIG_COUNT=2 GIT_CONFIG_KEY_0=credential.helper GIT_CONFIG_VALUE_0='!gh auth git-credential' \
  GIT_CONFIG_KEY_1=url.https://github.com/.insteadOf GIT_CONFIG_VALUE_1=git@github.com:
mkdir -p "$TMPDIR" "$DAEDALUS_LOGS"
unset work checkout

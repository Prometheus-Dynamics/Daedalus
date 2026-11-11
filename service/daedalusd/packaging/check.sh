#!/usr/bin/env bash
# Checks the packaging files against the daemon they package: both units verify, the sample
# config is accepted, and the socket path, config path and plugin directory are the ones the
# daemon and the other files name. It installs nothing and starts nothing.
#
#   service/daedalusd/packaging/check.sh               check the files here
#   service/daedalusd/packaging/check.sh --dir DIR     check the files in DIR, as the self-test does
#   service/daedalusd/packaging/check.sh --self-test   check that each planted breakage fails, and
#                                                      that an unbroken copy passes
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"

fail() {
  echo "packaging: $*" >&2
  exit 1
}

# The files checked: the packaging directory, or the one named.
dir="$here"
self_test=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --self-test) self_test=1 ;;
    --dir)
      [[ $# -ge 2 && -d "$2" ]] || fail "--dir needs a directory"
      dir="$(cd "$2" && pwd)"
      shift
      ;;
    *) fail "unknown argument: $1" ;;
  esac
  shift
done

cd "$root"
command -v systemd-analyze >/dev/null || fail "systemd-analyze is needed to verify the units"

# The daemon under test: the one named, or the one cargo builds.
if [[ -z "${DAEDALUSD:-}" ]]; then
  cargo build -q -p daedalusd
  DAEDALUSD="${CARGO_TARGET_DIR:-$root/target}/debug/daedalusd"
fi
[[ -x "$DAEDALUSD" ]] || fail "no daemon at $DAEDALUSD"
export DAEDALUSD

if [[ $self_test == 1 ]]; then
  # Each planted case is a copy of the packaging files with one thing broken, run through this
  # script with the daemon already built. It must fail with the message that names the break.
  # The unbroken copy must pass, so a check that fails everything is not taken for a check.
  planted="$(mktemp -d)"
  trap 'rm -rf "$planted"' EXIT
  failed=0

  planted_case() { # name, file, sed expression ("" for none), the message it must fail with
    local name="$1" file="$2" expr="$3" expect="$4" case_dir status=0 out
    case_dir="$planted/$name"
    mkdir "$case_dir"
    cp "$dir/daedalusd.service" "$dir/daedalusd.socket" "$dir/daedalusd.toml" \
      "$dir/sysusers.conf" "$dir/tmpfiles.conf" "$case_dir/"
    [[ -z "$expr" ]] || sed -i "$expr" "$case_dir/$file"
    out="$("$here/check.sh" --dir "$case_dir" 2>&1)" || status=$?
    if [[ -z "$expect" ]]; then
      if [[ $status -eq 0 ]]; then echo "  ok   $name: the unbroken copy passes"
      else echo "  FAIL $name: the unbroken copy was refused:"; echo "$out"; failed=1; fi
    elif [[ $status -ne 0 ]] && grep -qF "$expect" <<<"$out"; then
      echo "  ok   $name: fails with \"$expect\""
    else
      echo "  FAIL $name: expected a failure with \"$expect\", got status $status:"
      echo "$out"
      failed=1
    fi
  }

  planted_case unbroken daedalusd.service "" ""
  planted_case unit-missing-key daedalusd.service '/^Restart=on-failure$/d' \
    "the service lacks Restart=on-failure"
  planted_case unit-unknown-key daedalusd.service '/^\[Service\]$/a BogusKey=yes' \
    "systemd-analyze verify complained"
  planted_case config-refused daedalusd.toml 's|^listen = .*|listen = ["tcp:0.0.0.0:7000"]|' \
    "the sample config was refused"
  planted_case socket-mismatch daedalusd.socket 's|^ListenStream=.*|ListenStream=/run/daedalusd/other.sock|' \
    "the sample config does not listen on /run/daedalusd/other.sock"

  if [[ $failed -ne 0 ]]; then
    echo "packaging: self-test failed"
    exit 1
  fi
  echo "packaging: every planted breakage fails, and the unbroken copy passes"
  exit 0
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# 1. Both units verify. The units name /usr/bin/daedalusd, which is not installed here, so a
# copy that names the built daemon is what is verified. Any message about them is a failure.
cp "$dir/daedalusd.service" "$dir/daedalusd.socket" "$work/"
sed -i "s|^ExecStart=/usr/bin/daedalusd |ExecStart=$DAEDALUSD |" "$work/daedalusd.service"
grep -q "^ExecStart=$DAEDALUSD " "$work/daedalusd.service" || fail "the service does not start /usr/bin/daedalusd"
# The tool also loads the units these depend on, and reports what it thinks of the host's own
# units on the way. Only what it says about these two is judged: a line that names one of them,
# by its file or by its unit name.
verified="$(systemd-analyze verify "$work/daedalusd.service" "$work/daedalusd.socket" 2>&1 || true)"
ours="$(grep -E "(^|[^[:alnum:]_.@-])daedalusd\.(service|socket)" <<<"$verified" || true)"
[[ -z "$ours" ]] || fail "systemd-analyze verify complained:
$ours"

# 2. The sample config is accepted, and resolves to the socket the socket unit listens on.
checked="$("$DAEDALUSD" --check-config --config "$dir/daedalusd.toml")" || fail "the sample config was refused"
socket="$(sed -n 's/^ListenStream=//p' "$dir/daedalusd.socket")"
[[ -n "$socket" ]] || fail "the socket unit names no ListenStream"
grep -qx "listen: unix:$socket" <<<"$checked" || fail "the sample config does not listen on $socket:
$checked"

# 3. That socket is the daemon's own default: the constant, and what it resolves to under the
# service's runtime directory.
default="$(sed -n 's/^pub const DEFAULT_SOCKET: &str = "\(.*\)";$/\1/p' service/daedalusd/src/config.rs)"
[[ "$default" == "$socket" ]] || fail "the daemon's default socket is $default, the socket unit listens on $socket"
: >"$work/empty.toml"
runtime="$(sed -n 's/^RuntimeDirectory=//p' "$dir/daedalusd.service")"
resolved="$(RUNTIME_DIRECTORY="/run/$runtime" "$DAEDALUSD" --check-config --config "$work/empty.toml")"
grep -qx "listen: unix:$socket" <<<"$resolved" || fail "with RuntimeDirectory=$runtime the daemon would listen elsewhere:
$resolved"

# 4. The service reads the config the daemon reads by default, as the dedicated user.
config="$(sed -n 's/^pub const DEFAULT_CONFIG: &str = "\(.*\)";$/\1/p' service/daedalusd/src/config.rs)"
grep -qx "ExecStart=/usr/bin/daedalusd --config $config" "$dir/daedalusd.service" || fail "the service does not read $config"
[[ "$config" == /etc/daedalusd/daedalusd.toml ]] || fail "the config is not at /etc/daedalusd/daedalusd.toml"
user="$(sed -n 's/^User=//p' "$dir/daedalusd.service")"
[[ -n "$user" && "$user" != root ]] || fail "the service has no dedicated user"
grep -q "^u $user " "$dir/sysusers.conf" || fail "sysusers.conf does not make the user $user"
[[ "$(sed -n 's/^SocketUser=//p' "$dir/daedalusd.socket")" == "$user" ]] || fail "the socket is not owned by $user"

# 5. The plugin directory is the one the tmpfiles entry makes.
plugins="$(sed -n 's/^plugin_dir: //p' <<<"$checked")"
[[ "$plugins" == /usr/lib/daedalusd/plugins ]] || fail "the plugin directory is $plugins"
grep -q "^d $plugins " "$dir/tmpfiles.conf" || fail "tmpfiles.conf does not make $plugins"
grep -q "^d $(dirname "$config") " "$dir/tmpfiles.conf" || fail "tmpfiles.conf does not make $(dirname "$config")"

# 6. The hardening the service must have, and the failure rule: restarts never stop, and wait
# at most 8 s.
for directive in ProtectSystem=strict NoNewPrivileges=yes PrivateTmp=yes "RuntimeDirectory=$runtime" \
  "Requires=daedalusd.socket" Restart=on-failure RestartMaxDelaySec=8 StartLimitIntervalSec=0; do
  grep -qx "$directive" "$dir/daedalusd.service" || fail "the service lacks $directive"
done

echo "packaging: units verify, the sample config is accepted, and the paths agree ($socket)"

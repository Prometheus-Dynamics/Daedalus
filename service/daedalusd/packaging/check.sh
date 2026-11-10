#!/usr/bin/env bash
# Checks the packaging files against the daemon they package: both units verify, the sample
# config is accepted, and the socket path, config path and plugin directory are the ones the
# daemon and the other files name. It installs nothing and starts nothing.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
cd "$root"

fail() {
  echo "packaging: $*" >&2
  exit 1
}

command -v systemd-analyze >/dev/null || fail "systemd-analyze is needed to verify the units"

# The daemon under test: the one named, or the one cargo builds.
if [[ -z "${DAEDALUSD:-}" ]]; then
  cargo build -q -p daedalusd
  DAEDALUSD="${CARGO_TARGET_DIR:-$root/target}/debug/daedalusd"
fi
[[ -x "$DAEDALUSD" ]] || fail "no daemon at $DAEDALUSD"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# 1. Both units verify. The units name /usr/bin/daedalusd, which is not installed here, so a
# copy that names the built daemon is what is verified. Any message is a failure.
cp "$here/daedalusd.service" "$here/daedalusd.socket" "$work/"
sed -i "s|^ExecStart=/usr/bin/daedalusd |ExecStart=$DAEDALUSD |" "$work/daedalusd.service"
grep -q "^ExecStart=$DAEDALUSD " "$work/daedalusd.service" || fail "the service does not start /usr/bin/daedalusd"
if ! verified="$(systemd-analyze verify "$work/daedalusd.service" "$work/daedalusd.socket" 2>&1)"; then
  fail "systemd-analyze verify failed:
$verified"
fi
[[ -z "$verified" ]] || fail "systemd-analyze verify complained:
$verified"

# 2. The sample config is accepted, and resolves to the socket the socket unit listens on.
checked="$("$DAEDALUSD" --check-config --config "$here/daedalusd.toml")" || fail "the sample config was refused"
socket="$(sed -n 's/^ListenStream=//p' "$here/daedalusd.socket")"
[[ -n "$socket" ]] || fail "the socket unit names no ListenStream"
grep -qx "listen: unix:$socket" <<<"$checked" || fail "the sample config does not listen on $socket:
$checked"

# 3. That socket is the daemon's own default: the constant, and what it resolves to under the
# service's runtime directory.
default="$(sed -n 's/^pub const DEFAULT_SOCKET: &str = "\(.*\)";$/\1/p' service/daedalusd/src/config.rs)"
[[ "$default" == "$socket" ]] || fail "the daemon's default socket is $default, the socket unit listens on $socket"
: >"$work/empty.toml"
runtime="$(sed -n 's/^RuntimeDirectory=//p' "$here/daedalusd.service")"
resolved="$(RUNTIME_DIRECTORY="/run/$runtime" "$DAEDALUSD" --check-config --config "$work/empty.toml")"
grep -qx "listen: unix:$socket" <<<"$resolved" || fail "with RuntimeDirectory=$runtime the daemon would listen elsewhere:
$resolved"

# 4. The service reads the config the daemon reads by default, as the dedicated user.
config="$(sed -n 's/^pub const DEFAULT_CONFIG: &str = "\(.*\)";$/\1/p' service/daedalusd/src/config.rs)"
grep -qx "ExecStart=/usr/bin/daedalusd --config $config" "$here/daedalusd.service" || fail "the service does not read $config"
[[ "$config" == /etc/daedalusd/daedalusd.toml ]] || fail "the config is not at /etc/daedalusd/daedalusd.toml"
user="$(sed -n 's/^User=//p' "$here/daedalusd.service")"
[[ -n "$user" && "$user" != root ]] || fail "the service has no dedicated user"
grep -q "^u $user " "$here/sysusers.conf" || fail "sysusers.conf does not make the user $user"
[[ "$(sed -n 's/^SocketUser=//p' "$here/daedalusd.socket")" == "$user" ]] || fail "the socket is not owned by $user"

# 5. The plugin directory is the one the tmpfiles entry makes.
plugins="$(sed -n 's/^plugin_dir: //p' <<<"$checked")"
[[ "$plugins" == /usr/lib/daedalusd/plugins ]] || fail "the plugin directory is $plugins"
grep -q "^d $plugins " "$here/tmpfiles.conf" || fail "tmpfiles.conf does not make $plugins"
grep -q "^d $(dirname "$config") " "$here/tmpfiles.conf" || fail "tmpfiles.conf does not make $(dirname "$config")"

# 6. The hardening the service must have, and the failure rule: restarts never stop, and wait
# at most 8 s.
for directive in ProtectSystem=strict NoNewPrivileges=yes PrivateTmp=yes "RuntimeDirectory=$runtime" \
  "Requires=daedalusd.socket" Restart=on-failure RestartMaxDelaySec=8 StartLimitIntervalSec=0; do
  grep -qx "$directive" "$here/daedalusd.service" || fail "the service lacks $directive"
done

echo "packaging: units verify, the sample config is accepted, and the paths agree ($socket)"

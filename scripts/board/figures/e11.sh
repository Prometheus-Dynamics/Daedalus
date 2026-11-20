#!/bin/sh
# E11: a device loss caused by the driver on V3D. It runs last, and only with a hardware watchdog
# armed and fed by systemd, so a wedged kernel returns the board by itself. A short ramp first,
# then hangs (4,000,000,000 loops, the default 5 s bound) until the kernel has reset V3D three
# times or a wait fails. Each grind run ends with one `grind: summary:` line.
# Pass, one of two:
#  (a) the driver's lost callback fires, the backend reopens the device, and a known run on it is
#      correct (the reopen time is recorded);
#  (b) after three or more kernel resets no loss is reported, and the known run on the same device
#      is correct every time.
F=$(cd "$(dirname "$0")" && pwd)
. "$F/common.sh"
fig_begin e11

if [ "${BATCH_SKIP_E11:-}" = 1 ]; then
  gate "not run: rehearsal on lavapipe, which has no V3D hang"
  note "not run: rehearsal on lavapipe"
  exit 0
fi
armed=no
for w in /sys/class/watchdog/watchdog*; do
  [ "$(cat "$w/state" 2>/dev/null)" = active ] && armed=yes
done
runtime=$(systemctl show -p RuntimeWatchdogUSec --value 2>/dev/null)
case "$runtime" in "" | 0 | infinity) armed=no ;; esac
if [ "$armed" != yes ]; then
  gate "not run: no armed hardware watchdog fed by systemd (RuntimeWatchdogUSec=$runtime)"
  note "not run: no armed hardware watchdog fed by systemd (RuntimeWatchdogUSec=$runtime)"
  exit 0
fi
start=$(kernel_resets)
if [ "$start" -lt 0 ]; then
  gate "closed: the kernel log is not readable, so the driver's resets cannot be counted"
  note "closed: the kernel log is not readable"
  exit 0
fi
gate "open: the watchdog is armed (RuntimeWatchdogUSec=$runtime) and the kernel log is readable"
note "watchdog armed: $armed (RuntimeWatchdogUSec=$runtime)"
note "v3d resets in the kernel log at the start: $start"

for rounds in 1000000 10000000 100000000; do
  step "e11-ramp-$rounds" env DAEDALUS_GRIND_ROUNDS=$rounds DAEDALUS_GRIND_REPEAT=1 \
    "$B/daedalus-gpu-budgets" grind
done
n=0
while [ "$n" -lt 6 ]; do
  n=$((n + 1))
  step "e11-hang-$n" env DAEDALUS_GRIND_ROUNDS=4000000000 DAEDALUS_GRIND_REPEAT=1 \
    "$B/daedalus-gpu-budgets" grind
  last=$(grep "grind: summary:" "$OUT" | tail -n 1)
  case "$last" in *"wait=finished"*) ;; *) break ;; esac
  case "$last" in *"lost_callback=fired"*) break ;; esac
  now=$(kernel_resets)
  [ $((now - start)) -ge 3 ] && break
done
end=$(kernel_resets)
note "v3d resets in the kernel log: start $start, end $end, during E11 $((end - start))"
dmesg 2>/dev/null | tail -n 200 > "$R/dmesg-e11.txt"

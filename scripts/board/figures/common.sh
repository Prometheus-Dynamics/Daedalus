# Shared by the figure scripts. batch-on-board.sh exports D (the bundle directory), R (the
# results directory), B (the bundle's programs) and SECONDS_MAX, and runs each figure as
# `sh figures/<name>.sh`. A figure writes results/<name>.txt, in blocks a judge can read:
#   == step <name>: <command>
#   <the step's output>
#   == exit <code>
# It also writes results/<name>-gate.txt: its first word is open, closed, skipped or not (for
# "not run"), and the rest says why.
# Busybox ash runs this on the board: no bashisms. Checked with `sh -n` and `bash --posix -n`.

fig_begin() { # figure name
  FIG="$1"
  OUT="$R/$1.txt"
  : > "$OUT"
}

gate() { # the gate's word and its reason
  echo "$*" > "$R/$FIG-gate.txt"
}

note() { # a line of the figure's own, outside any step
  echo "$FIG: $*" >> "$OUT"
}

step() { # step name, command...: bounded, appended to the figure's file
  sname="$1"
  shift
  echo "== step $sname: $*" >> "$OUT"
  "$B/bounded" "$SECONDS_MAX" "$@" >> "$OUT" 2>&1
  code=$?
  echo "== exit $code" >> "$OUT"
}

step_to() { # file, step name, command...: the same, with the step also in a file of its own
  sfile="$1"
  sname="$2"
  shift 2
  echo "== step $sname: $*" > "$sfile"
  "$B/bounded" "$SECONDS_MAX" "$@" >> "$sfile" 2>&1
  code=$?
  echo "== exit $code" >> "$sfile"
  cat "$sfile" >> "$OUT"
}

kernel_resets() { # the V3D resets in the kernel log, or -1 when the log can't be read
  if log=$(dmesg 2>/dev/null); then
    printf '%s\n' "$log" | grep -c "Resetting GPU"
  else
    echo -1
  fi
}

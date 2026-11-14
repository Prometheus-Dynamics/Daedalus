# A board run in one command

`scripts/board/run.sh` builds what a board needs, puts it there, runs it, brings the results
back, takes everything off the board again, and judges. `scripts/board/judge.py` is the judge:
it reads only the files a run left, so a run can be judged again later.

| Command | What it does |
|---|---|
| `scripts/board/run.sh local` | The same bundle on this machine, run from a directory of its own. It proves the run and the judge with no board, and shows a test that needs the source tree. |
| `scripts/board/run.sh luckfox-build` | Only builds what a Luckfox run would send: static armv7 musl test binaries. Needs zig, and no board. |
| `scripts/board/run.sh luckfox --go` | A Luckfox (armv7 Linux, musl), over ssh. |
| `scripts/board/run.sh pico --go` | A Raspberry Pi Pico (RP2040), through a debug probe with `probe-rs`. |

## Before a run on a board

- A board is somebody's. Ask its keeper, and wait for their word: `--go` says you have it.
  Without `--go` the script touches no board.
- Build from NVMe. Set `CARGO_TARGET_DIR` as for any build here.
- For a Linux board, set `BOARD_SSH` to where it is, as `ssh` takes it. The connection is your
  own ssh configuration's: the script passes `BatchMode=yes` and nothing that weakens a host-key
  or login check. If the board asks for a password, or its host key has changed, stop and tell
  its keeper. Type nothing.
- `BOARD_SECONDS` is the longest one test binary may run (300), or how long the Pico's log is
  read (60). `BOARD_PACKAGES` is the packages whose tests are sent: the runner, the CPU backend
  and the host unless it says otherwise.

## What a run does on a board

On a Linux board it makes one directory, `/tmp/daedalus-board-<pid>`, unpacks the test binaries
and `on-board.sh` there, runs each binary once, one at a time, and removes the directory. It
starts and stops no service, changes no file outside its directory, adds no user, group or
permission, and kills only a process it started, by that process's pid. A run that can't
remove its directory says so and stops: remove it by hand.

On a Pico it flashes the firmware, reads the log for the time given, and stops the probe it
started.

## What the judge wants

- Linux: every binary exited 0 and reported a clean result itself, and at least one test ran.
  The line for each binary has its exit code and its counts.
- Pico: each of the three modes (compiled, tunable, loaded) reported at least twice, of 10000
  ticks each, with no failed tick and a plan that loaded. The first report of each mode is left
  out, since the flash cache is cold for it. The stack has to be under the firmware's 2 KiB of
  RAM. The line for each mode has its tick times and stack.

The judges are run against planted runs in the gate (`judge.py --self-test`): a good run
passes, and each bad one fails for its own reason.

## Afterwards

The results are under `$CARGO_TARGET_DIR/board/<board>/results/`, with the revision in
`rev.txt` and the board's `uname` in `uname.txt`. Write the run up as a file here, with the
board, the revision, the judge's lines, and what was not run.

## State

No Luckfox or Pico run has been made: none has been assigned a board. `local` passes on the
development machine, and `luckfox-build` builds 54 static armv7 test binaries there.

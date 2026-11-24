#!/usr/bin/env python3
"""Runs a command under a hard bound on its run time, in a process group of its own.

    bounded.py SECONDS NAME COMMAND [ARGS...]
    bounded.py --step NAME SCRIPT       the bound is SCRIPT's own `# bound: N s` line
    bounded.py --self-test

A gate step or a test binary that hangs would hold the gate forever. So the command starts as the
leader of a new process group. If it has not ended once SECONDS have passed, the whole group gets
TERM, and KILL after a grace, and the run fails with exit 124, naming NAME and its bound. When the
command ends by itself, anything it left running in its group is stopped the same way, and named,
so nothing outlives its step. A signal to this runner (TERM, INT, HUP) stops the group too.

The command writes straight to this runner's own output, a file in the gate: nothing reads a pipe
that a child left behind could hold open. Callers that want the output (scripts/test-parallel.py)
give it a file, never a pipe.

The leader is not reaped until its group is stopped. Its group id stays its pid while it is a
zombie, so no other process can take that id and be signalled by mistake.

DAEDALUS_BOUND_SCALE multiplies every bound, for a machine slower than the one the bounds were
measured on (the hosted CI runners).

The bound is on the step's own run. A step that first has to wait for another step (for the
build directory the `test` step is building in, say), or to build what it runs, carries a
`# wait: N s` line too, and gets DAEDALUS_BOUND_STARTED: a path it creates once that is over. Its
bound counts from then, so a cold or slow build cannot fail a step that has not run yet. The wait
has a bound of its own, N, and a step still waiting then fails by name.

A test binary's bound (scripts/test-parallel.py, `Group.wait_fair`) does not count the time its
threads spent waiting for a CPU: on a machine running more than it has cores for, a test that is
only starved is not slow, and failing it says nothing about the test. The kernel records, for every
thread, how long it ran and how long it waited to run; the time waited, a thread's share of it, is
taken off the clock. A test that spins or sleeps past its bound still fails at it. One that makes
no progress at all fails at OUTER times its bound, whatever the machine was doing.

A step (`--step`) runs with a TMPDIR of its own: an empty directory made in the gate's TMPDIR,
removed when the step ends. A step that leaves anything in it fails, and the names are printed:
tests that leak into a TMPDIR shared by every gate run fill it until unrelated steps fail. A cache
that must outlive one gate run goes in DAEDALUS_GATE_TMPDIR, the gate's own TMPDIR, by name.
"""
import os
import re
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

TIMED_OUT = 124
GRACE = 5.0
# A test binary still running at this many times its bound is stopped, however little CPU it had.
OUTER = 10
# How often a waiting test's threads are looked at.
POLL = 1.0


def scaled(seconds):
    return float(seconds) * float(os.environ.get("DAEDALUS_BOUND_SCALE", "1"))


def members(pgid):
    """The live processes in group `pgid`: what is left to stop. Zombies are done already."""
    found = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as file:
                fields = file.read().rsplit(")", 1)[1].split()
        except (OSError, IndexError):
            continue
        if int(fields[2]) == pgid and fields[0] not in "ZX":
            found.append(int(entry))
    return found


def waited(pgid):
    """How long each thread of group `pgid` has waited for a CPU, in seconds, by thread id. Empty
    where the kernel keeps no such record."""
    found = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as file:
                if int(file.read().rsplit(")", 1)[1].split()[2]) != pgid:
                    continue
            for task in os.listdir(f"/proc/{entry}/task"):
                with open(f"/proc/{entry}/task/{task}/schedstat") as file:
                    found[int(task)] = int(file.read().split()[1]) / 1e9
        except (OSError, IndexError, ValueError):
            continue
    return found


class Group:
    """A command started as the leader of a process group of its own."""

    def __init__(self, argv, **popen):
        self.proc = subprocess.Popen(argv, process_group=0, **popen)
        self.pgid = self.proc.pid
        self.pidfd = os.pidfd_open(self.proc.pid)
        self.lock = threading.Lock()
        self.reaped = False

    def wait(self, timeout):
        """Whether the leader ended within `timeout` seconds. It is left unreaped."""
        ready, _, _ = select.select([self.pidfd], [], [], max(0.0, timeout))
        return bool(ready)

    def wait_fair(self, bound, out=None, outer=OUTER):
        """Whether the leader ended before it had `bound` seconds that it could use: wall time,
        less the time its threads spent waiting for a CPU, or before `outer` times `bound` of
        wall time whatever it waited for. It is left unreaped. When it did not end, `out` (a list)
        gets the seconds of wall time it ran for and the seconds taken off. A process of the
        group that starts and ends between two polls is never seen, so what it waited is not taken
        off: a binary that runs many short children (the drive suites) is bounded by its slowest
        green run under load, which test-slowest.json keeps."""
        start = time.monotonic()
        seen, lost = {}, 0.0
        while True:
            wall = time.monotonic() - start
            left = min(bound - (wall - lost), outer * bound - wall)
            if left <= 0:
                if out is not None:
                    out[:] = [wall, lost]
                return False
            if self.wait(min(POLL, left)):
                return True
            now = waited(self.pgid)
            # A thread seen for the first time has waited what it says since it began.
            more = sum(max(0.0, wait - seen.get(task, 0.0)) for task, wait in now.items())
            seen.update(now)
            # Threads that all waited at once lost that time once, not once each.
            lost += more / max(1, len(now))

    def stop(self):
        """TERM to the whole group, then KILL to what is still there after the grace. Returns
        the processes that were still running when it was called, other than the leader."""
        with self.lock:
            if self.reaped:
                return []
            left = [pid for pid in members(self.pgid) if pid != self.pgid]
            if left or not self.wait(0):
                os.killpg(self.pgid, signal.SIGTERM)
                deadline = time.monotonic() + GRACE
                while members(self.pgid) and time.monotonic() < deadline:
                    time.sleep(0.05)
                if members(self.pgid):
                    os.killpg(self.pgid, signal.SIGKILL)
                    while members(self.pgid):
                        time.sleep(0.01)
            return left

    def reap(self):
        """Stops what is left in the group, then reaps the leader. Returns its exit code and what
        it had left running."""
        left = self.stop()
        with self.lock:
            if not self.reaped:
                self.proc.wait()
                os.close(self.pidfd)
                self.reaped = True
        return self.proc.returncode, left


def run(argv, bound, name, out=sys.stderr, wait=None, **popen):
    """Runs `argv` within `bound` seconds. Returns its exit code, or TIMED_OUT. `wait` is
    (seconds, path): the command may first wait that long, and its bound counts from when it
    creates the path."""
    group = Group(argv, **popen)
    waiting = False
    try:
        ended = False
        if wait is not None:
            deadline = time.monotonic() + wait[0]
            waiting = True
            while not ended and not os.path.exists(wait[1]):
                if time.monotonic() >= deadline:
                    break
                ended = group.wait(0.1)
            else:
                waiting = False
        if not ended and not waiting:
            ended = group.wait(bound)
    finally:
        # Reached by an ending, a timeout, or a signal (raised by the handler as an exception).
        code, left = group.reap()
    if left and ended:
        print(f"{name}: left {len(left)} processes running in its group: stopped them", file=out)
    if not ended and waiting:
        print(f"{name}: still waiting to start at its wait bound of {wait[0]:.0f} s: stopped it",
              file=out)
        return TIMED_OUT
    if not ended:
        print(f"{name}: still running at its bound of {bound:.0f} s: stopped it", file=out)
        return TIMED_OUT
    return code


class Stopped(Exception):
    def __init__(self, signum):
        self.signum = signum


def raise_stopped(signum, _frame):
    raise Stopped(signum)


def bound_of(script, word="bound"):
    """A script's bound, from the `# bound: N s` line it carries beside its code, or its wait
    bound from its `# wait: N s` line."""
    with open(script) as file:
        for line in file:
            match = re.match(rf"# {word}: (\d+) s", line)
            if match:
                return match.group(1)
    return None


def main(argv):
    if argv[:1] == ["--self-test"]:
        return self_test()
    step = argv[:1] == ["--step"] and len(argv) == 3
    if step:
        bound = bound_of(argv[2])
        if bound is None:
            print(f"bounded: {argv[2]} has no `# bound: N s` line: every step the gate runs "
                  "carries its bound", file=sys.stderr)
            return 2
        wait = bound_of(argv[2], "wait")
        argv = [bound, argv[1], argv[2]]
    if len(argv) < 3:
        print(__doc__.split("\n\n")[1], file=sys.stderr)
        return 2
    bound, name, command = scaled(argv[0]), argv[1], argv[2:]
    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, raise_stopped)
    if not step:
        try:
            return run(command, bound, name)
        except Stopped as stopped:
            return 128 + stopped.signum
    gate_tmp = os.environ.get("DAEDALUS_GATE_TMPDIR") or tempfile.gettempdir()
    # A short name: a unix socket's path must stay under 108 bytes.
    own = tempfile.mkdtemp(prefix=f"s-{re.sub(r'[^a-z0-9]', '', name.lower())[:8]}.",
                           dir=gate_tmp)
    env = {**os.environ, "TMPDIR": own, "DAEDALUS_GATE_TMPDIR": gate_tmp}
    env.pop("DAEDALUS_BOUND_STARTED", None)
    started = own + ".started"
    if wait is not None:
        env["DAEDALUS_BOUND_STARTED"] = started
        wait = (scaled(wait), started)
    try:
        code = run(command, bound, name, wait=wait, env=env)
    except Stopped as stopped:
        code = 128 + stopped.signum
    finally:
        left = sorted(os.listdir(own))
        shutil.rmtree(own, ignore_errors=True)
        if os.path.exists(started):
            os.remove(started)
    if left:
        shown = ", ".join(left[:20]) + (f", and {len(left) - 20} more" if len(left) > 20 else "")
        print(f"{name}: left {len(left)} entries in its TMPDIR: {shown}", file=sys.stderr)
        if code == 0:
            code = 1
    return code


def gone(pids):
    """Whether none of the planted pids is still running (a zombie has ended)."""
    for pid in pids:
        try:
            with open(f"/proc/{pid}/stat") as file:
                if file.read().rsplit(")", 1)[1].split()[0] not in "ZX":
                    return False
        except OSError:
            pass
    return True


def write(path, text):
    with open(path, "w") as file:
        file.write(text)
    os.chmod(path, 0o755)


def capture(argv, limit, env=None):
    """Runs `argv` with its output on a pipe and reads it to the end, giving up after `limit`
    seconds rather than blocking on a child that holds the pipe. Returns the exit code, the
    output, the seconds until the pipe ended, and whether it ended."""
    child = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
    chunks = []
    reader = threading.Thread(target=lambda: chunks.append(child.stdout.read()), daemon=True)
    start = time.monotonic()
    reader.start()
    reader.join(limit)
    took = time.monotonic() - start
    ended = not reader.is_alive()
    try:
        code = child.wait(timeout=max(1.0, limit - took))
    except subprocess.TimeoutExpired:
        child.kill()
        code = child.wait()
    return code, b"".join(chunks).decode(errors="replace"), took, ended


def planted(path):
    """The pids a plant wrote, which it started (`$!`)."""
    try:
        with open(path) as file:
            return [int(line) for line in file.read().split()]
    except OSError:
        return []


def self_test():
    import json
    import shutil
    import tempfile

    me = os.path.abspath(__file__)
    failures = []
    started = []

    def check(what, ok):
        print(f"bounded: {what}: {'ok' if ok else 'FAILED'}")
        if not ok:
            failures.append(what)

    with tempfile.TemporaryDirectory() as tmp:
        pids = os.path.join(tmp, "pids")
        try:
            # A sleeper past its bound, with a child of its own and an orphan that holds the output
            # pipe: the run fails at the bound, the reader sees the end, and nothing is left.
            plant = (f"sleep 600 & echo $! >> {pids}; (sleep 600 & echo $! >> {pids}; wait) & "
                     f"echo $! >> {pids}; sleep 600")
            code, out, took, ended = capture(
                [sys.executable, "-I", me, "2", "planted-sleeper", "sh", "-c", plant], 30)
            started += planted(pids)
            check("a sleeper fails at its bound", code == TIMED_OUT)
            check("the failure names the step and its bound",
                  "planted-sleeper: still running at its bound of 2 s" in out)
            check("its output ends within the bound and the grace", ended and took < 2 + GRACE + 3)
            check("nothing it started is left running", gone(planted(pids)))

            # A command that ends but leaves an orphan holding the output pipe: the reader still
            # sees the end, the run keeps the command's own exit code, and the orphan is stopped
            # and named.
            if os.path.exists(pids):
                os.remove(pids)
            plant = f"(sleep 600 & echo $! >> {pids}; wait) & echo $! >> {pids}; exit 3"
            code, out, took, ended = capture(
                [sys.executable, "-I", me, "60", "planted-orphan", "sh", "-c", plant], 30)
            started += planted(pids)
            check("an orphan's run keeps the command's exit code", code == 3)
            check("the orphan is named", "planted-orphan: left" in out)
            check("the output ends without waiting on the orphan", ended and took < GRACE + 3)
            check("the orphan is stopped", gone(planted(pids)))

            # A TERM to the runner stops its group: how a gate that is stopped stops its steps.
            if os.path.exists(pids):
                os.remove(pids)
            runner = subprocess.Popen(
                [sys.executable, "-I", me, "600", "planted-stopped", "sh", "-c",
                 f"sleep 600 & echo $! >> {pids}; wait"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            deadline = time.monotonic() + 30
            while not planted(pids) and time.monotonic() < deadline:
                time.sleep(0.02)
            runner.send_signal(signal.SIGTERM)
            try:
                code = runner.wait(timeout=GRACE + 10)
            except subprocess.TimeoutExpired:
                runner.kill()
                code = runner.wait()
            started += planted(pids)
            check("a TERM to the runner stops its group",
                  code == 128 + signal.SIGTERM and gone(planted(pids)))

            # The gate's own runners on a planted tree: a step and a test binary that sleep past
            # their bounds and leave an orphan on the output pipe go red, named, and stop.
            scripts = os.path.dirname(me)
            tree = os.path.join(tmp, "tree")
            os.makedirs(os.path.join(tree, "scripts", "ci.d"))
            for name in ("ci.sh", "bounded.py", "test-parallel.py", "fixture-locks.sh", "seed-target.py"):
                shutil.copy(os.path.join(scripts, name), os.path.join(tree, "scripts", name))
            plant = (f"sleep 600 & echo $! >> {pids}; (sleep 600 & echo $! >> {pids}; wait) & "
                     f"echo $! >> {pids}; sleep 600")
            write(os.path.join(tree, "scripts", "ci.d", "planted.sh"),
                  f"#!/usr/bin/env bash\n# bound: 2 s (planted)\n{plant}\n")
            env = {**os.environ, "CARGO_TARGET_DIR": os.path.join(tmp, "target"),
                   "VK_DRIVER_FILES": "planted", "VK_ICD_FILENAMES": "planted"}
            env.pop("DAEDALUS_TARGET_SEED", None)
            env.pop("DAEDALUS_BOUND_SCALE", None)
            if os.path.exists(pids):
                os.remove(pids)
            code, out, took, ended = capture(
                ["bash", os.path.join(tree, "scripts", "ci.sh"), "planted"], 30, env=env)
            started += planted(pids)
            check("a gate step past its bound fails, named",
                  code != 0 and "planted: still running at its bound of 2 s" in out)
            check("the step's output ends within the bound and the grace",
                  ended and took < 2 + GRACE + 3)
            check("nothing the step started is left running", gone(planted(pids)))

            # Two test binaries: one sleeps past its bound (600 s for a binary with no time on
            # record, scaled to 2 s here), one passes and leaves an orphan behind.
            artifacts = []
            for name, body in (("sleeper", plant),
                               ("orphan", f"(sleep 600 & echo $! >> {pids}; wait) & "
                                          f"echo $! >> {pids}; echo 'test result: ok.'")):
                binary = os.path.join(tree, name)
                write(binary, f"#!/bin/sh\n{body}\n")
                artifacts.append(json.dumps({
                    "reason": "compiler-artifact", "executable": binary,
                    "profile": {"test": True}, "manifest_path": os.path.join(tree, "Cargo.toml"),
                    "package_id": f"path+file://{tree}#planted@0.0.0",
                    "target": {"kind": ["test"], "name": name}}))
            cargo = os.path.join(tree, "cargo")
            write(cargo, "#!/bin/sh\ncat <<'EOF'\n" + "\n".join(artifacts) + "\nEOF\n")
            # The sleeper's committed slowest green run, 400 s, bounds it at 1200 s (4 s scaled),
            # where its build directory has no record of it (600 s, 2 s scaled).
            write(os.path.join(tree, "scripts", "test-slowest.json"),
                  json.dumps({"planted test sleeper": 400.0}))
            if os.path.exists(pids):
                os.remove(pids)
            code, out, took, ended = capture(
                [sys.executable, "-I", os.path.join(tree, "scripts", "test-parallel.py")], 30,
                env={**env, "CARGO": cargo, "DAEDALUS_BOUND_SCALE": str(2 / 600)})
            started += planted(pids)
            check("a test binary past its bound fails, named",
                  code == 1 and "planted test sleeper: TIMED OUT" in out
                  and "planted test sleeper: still running at its bound of 4 s" in out)
            check("a binary's orphan is stopped and named, and the binary still passes",
                  "planted test orphan: ok" in out and "(left 2 running: stopped)" in out)
            check("the run's output ends within the bound and the grace",
                  ended and took < 4 + GRACE + 3)
            check("nothing the binaries started is left running", gone(planted(pids)))

            # A step that leaves a file in its TMPDIR fails, naming it; a clean step passes. Both
            # TMPDIRs are removed, and a step's TMPDIR is not the gate's.
            gate_tmp = os.path.join(tmp, "gate-tmp")
            os.makedirs(gate_tmp)
            step_env = {**os.environ, "TMPDIR": gate_tmp}
            step_env.pop("DAEDALUS_GATE_TMPDIR", None)
            for name, body in (("leaky", 'touch "$TMPDIR/planted-leftover"; mkdir "$TMPDIR/d"'),
                               ("clean", 'f="$TMPDIR/x"; touch "$f"; rm "$f"; '
                                         'test "$TMPDIR" != "$DAEDALUS_GATE_TMPDIR"')):
                write(os.path.join(tmp, f"{name}.sh"),
                      f"#!/usr/bin/env bash\n# bound: 60 s (planted)\n{body}\n")
                os.chmod(os.path.join(tmp, f"{name}.sh"), 0o755)
            code, out, _, _ = capture(
                [sys.executable, "-I", me, "--step", "leaky", os.path.join(tmp, "leaky.sh")], 30,
                env=step_env)
            check("a step that leaves files in its TMPDIR fails, naming them",
                  code == 1 and "leaky: left 2 entries in its TMPDIR: d, planted-leftover" in out)
            code, out, _, _ = capture(
                [sys.executable, "-I", me, "--step", "clean", os.path.join(tmp, "clean.sh")], 30,
                env=step_env)
            check("a step that cleans up passes, in a TMPDIR of its own", code == 0)
            check("no step's TMPDIR is left in the gate's", os.listdir(gate_tmp) == [])

            # A step that waits for another one: its bound counts from when it says it started.
            signal_start = 'touch "$DAEDALUS_BOUND_STARTED"'
            for name, body in (("waited", f"sleep 3; {signal_start}; sleep 1"),
                               ("slow", f"{signal_start}; sleep 600"),
                               ("stuck", "sleep 600")):
                write(os.path.join(tmp, f"{name}.sh"),
                      f"#!/usr/bin/env bash\n# bound: 2 s (planted)\n# wait: 4 s (planted)\n"
                      f"{body}\n")
            step = lambda name: capture(
                [sys.executable, "-I", me, "--step", name, os.path.join(tmp, f"{name}.sh")], 30,
                env=step_env)
            code, out, _, _ = step("waited")
            check("a step that waited longer than its bound, then ran within it, passes",
                  code == 0 and out == "")
            code, out, took, _ = step("slow")
            check("a step past its bound once it has started fails, named",
                  code == TIMED_OUT and "slow: still running at its bound of 2 s" in out
                  and took < 2 + GRACE + 3)
            code, out, took, _ = step("stuck")
            check("a step still waiting at its wait bound fails, named",
                  code == TIMED_OUT
                  and "stuck: still waiting to start at its wait bound of 4 s" in out
                  and took < 4 + GRACE + 3)
            check("no step's start mark is left in the gate's", os.listdir(gate_tmp) == [])

            # A test binary's bound doesn't count time spent waiting for a CPU. One CPU is shared
            # with spinners that are not the test's: a worker that needs 1 s of CPU takes longer
            # than its 2 s bound to get it, and passes; alone on the CPU and spinning for ever it
            # fails at its bound; starved and spinning for ever it fails at three times it (OUTER
            # times, for a test binary).
            cpu = min(os.sched_getaffinity(0))
            pin = lambda: os.sched_setaffinity(0, {cpu})
            spin = [sys.executable, "-I", "-c", "while True: pass"]
            work = [sys.executable, "-I", "-c",
                    "import time\nwhile time.process_time() < 1: pass"]

            def fair(argv, bound, others, outer=OUTER):
                crowd = [subprocess.Popen(spin, preexec_fn=pin) for _ in range(others)]
                group, ran_for = Group(argv, preexec_fn=pin), []
                begin = time.monotonic()
                try:
                    ended = group.wait_fair(bound, ran_for, outer)
                    took = time.monotonic() - begin
                finally:
                    group.reap()
                    for other in crowd:
                        other.kill()
                        other.wait()
                return ended, took, ran_for

            ended, took, _ = fair(work, 2, 4)
            check("a starved test that needs less CPU than its bound passes, however long it took",
                  ended and took > 2)
            # The machine may be busy on this CPU too, so what is checked is the time the spinner
            # could use, not the time on the wall.
            ended, took, ran_for = fair(spin, 2, 0)
            check("a test that spins past its bound fails at it",
                  not ended and ran_for[0] - ran_for[1] < 2 + 2 * POLL and took < OUTER * 2)
            ended, took, ran_for = fair(spin, 1, 20, outer=3)
            check("a starved test that never ends fails at a multiple of its bound",
                  not ended and 3 <= took < 3 + 6 and ran_for[1] > 1)

            # A command that ends within its bound runs as it would alone.
            code, out, _, ended = capture(
                [sys.executable, "-I", me, "60", "planted-quick", "sh", "-c", "echo out"], 30)
            check("a quick command passes through", code == 0 and ended and out == "out\n")
        finally:
            # Whatever a failed check left behind: only the pids the plants wrote, by number.
            for pid in started:
                if not gone([pid]):
                    try:
                        os.kill(pid, signal.SIGKILL)
                    except OSError:
                        pass
    if failures:
        print(f"bounded: {len(failures)} checks failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

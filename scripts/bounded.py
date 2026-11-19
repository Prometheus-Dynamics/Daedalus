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
"""
import os
import re
import select
import signal
import subprocess
import sys
import threading
import time

TIMED_OUT = 124
GRACE = 5.0


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


def run(argv, bound, name, out=sys.stderr, **popen):
    """Runs `argv` within `bound` seconds. Returns its exit code, or TIMED_OUT."""
    group = Group(argv, **popen)
    try:
        ended = group.wait(bound)
    finally:
        # Reached by an ending, a timeout, or a signal (raised by the handler as an exception).
        code, left = group.reap()
    if left and ended:
        print(f"{name}: left {len(left)} processes running in its group: stopped them", file=out)
    if not ended:
        print(f"{name}: still running at its bound of {bound:.0f} s: stopped it", file=out)
        return TIMED_OUT
    return code


class Stopped(Exception):
    def __init__(self, signum):
        self.signum = signum


def raise_stopped(signum, _frame):
    raise Stopped(signum)


def bound_of(script):
    """A script's bound, from the `# bound: N s` line it carries beside its code."""
    with open(script) as file:
        for line in file:
            match = re.match(r"# bound: (\d+) s", line)
            if match:
                return match.group(1)
    return None


def main(argv):
    if argv[:1] == ["--self-test"]:
        return self_test()
    if argv[:1] == ["--step"] and len(argv) == 3:
        bound = bound_of(argv[2])
        if bound is None:
            print(f"bounded: {argv[2]} has no `# bound: N s` line: every step the gate runs "
                  "carries its bound", file=sys.stderr)
            return 2
        argv = [bound, argv[1], argv[2]]
    if len(argv) < 3:
        print(__doc__.split("\n\n")[1], file=sys.stderr)
        return 2
    bound, name, command = scaled(argv[0]), argv[1], argv[2:]
    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, raise_stopped)
    try:
        return run(command, bound, name)
    except Stopped as stopped:
        return 128 + stopped.signum


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
            for name in ("ci.sh", "bounded.py", "test-parallel.py"):
                shutil.copy(os.path.join(scripts, name), os.path.join(tree, "scripts", name))
            plant = (f"sleep 600 & echo $! >> {pids}; (sleep 600 & echo $! >> {pids}; wait) & "
                     f"echo $! >> {pids}; sleep 600")
            write(os.path.join(tree, "scripts", "ci.d", "planted.sh"),
                  f"#!/usr/bin/env bash\n# bound: 2 s (planted)\n{plant}\n")
            env = {**os.environ, "CARGO_TARGET_DIR": os.path.join(tmp, "target"),
                   "VK_DRIVER_FILES": "planted", "VK_ICD_FILENAMES": "planted"}
            env.pop("DAEDALUS_TARGET_SEED", None)
            env.pop("DAEDALUS_BOUND_SCALE", None)
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
            os.remove(pids)
            code, out, took, ended = capture(
                [sys.executable, "-I", os.path.join(tree, "scripts", "test-parallel.py")], 30,
                env={**env, "CARGO": cargo, "DAEDALUS_BOUND_SCALE": str(2 / 600)})
            started += planted(pids)
            check("a test binary past its bound fails, named",
                  code == 1 and "planted test sleeper: TIMED OUT" in out
                  and "planted test sleeper: still running at its bound of 2 s" in out)
            check("a binary's orphan is stopped and named, and the binary still passes",
                  "planted test orphan: ok" in out and "(left 2 running: stopped)" in out)
            check("the run's output ends within the bound and the grace",
                  ended and took < 2 + GRACE + 3)
            check("nothing the binaries started is left running", gone(planted(pids)))

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

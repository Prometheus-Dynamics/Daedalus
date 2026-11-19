#!/usr/bin/env python3
"""Runs the workspace's test binaries side by side: what `cargo test --workspace --all-targets` runs,
binary for binary, with each binary run as cargo runs it.

    test-parallel.py [CARGO_TEST_ARGS...]

cargo test runs one test binary at a time, so a binary whose tests wait on a child process or a
socket holds the whole run. Here every binary cargo would run is built first (`--no-run`), then
they run at once, up to one per core, the slowest first by the times of the last run. Each binary
is still one process, so its tests share what they share under cargo test: a fixture built once,
a lock on state of the process. Every binary runs, and the step fails if any binary fails; each
failing binary's output is printed whole.

A binary is run as cargo runs it: in its package's directory, with cargo's variables for that
package, and the target's library directories on the library path.

With DAEDALUS_TEST_PACKAGES set to package names, only those packages' binaries run, from the
same build: scripts/check.sh runs a change's packages this way. The gate unsets it.

Each binary runs under a bound: three times the slowest it has passed in, as the times file in the
build directory records it, and never under FLOOR; a binary with no passing run on record gets
UNKNOWN, three times the slowest any binary has passed in at a gate (192 s, the wgpu-author compile
tests under three gates at once). Each runs in a process group of its own (scripts/bounded.py): past
its bound the group is stopped and the binary fails, named, and what a binary leaves running when it
ends is stopped and named. Its output goes to a file, so a child left holding it never blocks the
run. A TERM to this runner stops every binary's group.

Binaries that link trybuild run one after another: trybuild builds every crate's compile tests in
one shared directory under the same binary names, so two at once overwrite each other's builds.
"""
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bounded import TIMED_OUT, Group, scaled  # noqa: E402

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TARGET = os.environ.get("CARGO_TARGET_DIR") or os.path.join(ROOT, "target")
TIMES = os.path.join(TARGET, "test-parallel-times.json")
CARGO = os.environ.get("CARGO", "cargo")
FLOOR = 60.0
UNKNOWN = 600.0


def binaries(args):
    """Each test binary cargo would run, with its package's manifest."""
    out = subprocess.run(
        [CARGO, "test", *args, "--no-run", "--message-format=json-render-diagnostics"],
        cwd=ROOT, stdout=subprocess.PIPE, check=True, text=True,
    ).stdout
    found = {}
    for line in out.splitlines():
        if not line.startswith("{"):
            continue
        message = json.loads(line)
        if message.get("reason") != "compiler-artifact" or not message.get("executable"):
            continue
        if not message["profile"]["test"]:
            continue
        found[message["executable"]] = message
    return list(found.values())


def package_env(message, sysroot_lib):
    manifest = message["manifest_path"]
    name, version = package_of(message["package_id"], manifest)
    env = dict(os.environ)
    env.update({
        "CARGO": CARGO,
        "CARGO_MANIFEST_DIR": os.path.dirname(manifest),
        "CARGO_MANIFEST_PATH": manifest,
        "CARGO_PKG_NAME": name,
        "CARGO_PKG_VERSION": version,
        "CARGO_CRATE_NAME": message["target"]["name"].replace("-", "_"),
    })
    libs = [os.path.join(TARGET, "debug", "deps"), os.path.join(TARGET, "debug"), sysroot_lib]
    if env.get("LD_LIBRARY_PATH"):
        libs.append(env["LD_LIBRARY_PATH"])
    env["LD_LIBRARY_PATH"] = os.pathsep.join(libs)
    return env


def package_of(package_id, manifest):
    """The package's name and version, from its id: `path+file:///dir#name@version`, or
    `path+file:///dir#version` when the name is the directory's."""
    tail = package_id.rsplit("#", 1)[-1]
    if "@" in tail:
        name, version = tail.split("@", 1)
    else:
        name, version = os.path.basename(os.path.dirname(manifest)), tail
    return name, version


def shares_trybuild(message):
    """Whether the binary links trybuild, by its symbols: wherever in the test the use sits."""
    with open(message["executable"], "rb") as file:
        return b"trybuild" in file.read()


def label(message):
    kinds = message["target"]["kind"]
    kind = "unittests" if any(k in ("lib", "rlib", "proc-macro", "cdylib", "staticlib") for k in kinds) else kinds[0]
    name, _ = package_of(message["package_id"], message["manifest_path"])
    return f"{name} {kind} {message['target']['name']}"


def main():
    args = ["--locked", "--workspace", "--all-targets", *sys.argv[1:]]
    found = binaries(args)
    rustc = lambda *words: subprocess.run(["rustc", *words], capture_output=True, text=True,
                                          check=True).stdout.strip()
    host = next(l.split()[1] for l in rustc("-vV").splitlines() if l.startswith("host:"))
    # A proc-macro's test binary links the toolchain's libstd as a shared library, as cargo knows.
    sysroot_lib = os.path.join(rustc("--print", "sysroot"), "lib", "rustlib", host, "lib")
    try:
        with open(TIMES) as file:
            last = json.load(file)
    except (OSError, ValueError):
        last = {}
    # Each binary's last time, and the slowest it has passed in. A file from before keeps one
    # number, which is both.
    last = {name: seconds if isinstance(seconds, list) else [seconds, seconds]
            for name, seconds in last.items()}
    built = len(found)
    # A binary renamed or merged away leaves its time behind: only the binaries built now keep one.
    current = {label(m) for m in found}
    last = {name: seconds for name, seconds in last.items() if name in current}
    only = os.environ.get("DAEDALUS_TEST_PACKAGES")
    if only is not None:
        wanted = set(only.split())
        found = [m for m in found if package_of(m["package_id"], m["manifest_path"])[0] in wanted]
        print(f"  {len(found)} of {built} test binaries: those of DAEDALUS_TEST_PACKAGES")
    shared = [m for m in found if shares_trybuild(m)]
    jobs = [[m] for m in found if m not in shared] + ([shared] if shared else [])
    jobs.sort(key=lambda job: -sum(last.get(label(m), [0.0])[0] for m in job))

    def bound(name):
        if last.get(name, [0.0, None])[1] is None:
            return scaled(UNKNOWN)
        return scaled(max(FLOOR, 3 * last[name][1]))

    groups, groups_lock = set(), threading.Lock()

    def stop_all(signum, _frame):
        with groups_lock:
            running = list(groups)
        for group in running:
            group.stop()
        os._exit(128 + signum)

    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, stop_all)

    def run(job):
        ran = []
        for message in job:
            name = label(message)
            start = time.monotonic()
            with tempfile.TemporaryFile() as out:
                group = Group([message["executable"]],
                              cwd=os.path.dirname(message["manifest_path"]),
                              env=package_env(message, sysroot_lib), stdout=out,
                              stderr=subprocess.STDOUT)
                with groups_lock:
                    groups.add(group)
                ended = group.wait(bound(name))
                code, left = group.reap()
                with groups_lock:
                    groups.discard(group)
                out.seek(0)
                output = out.read().decode(errors="replace")
            if not ended:
                left = []
            if left:
                output += f"\n{name}: left {len(left)} processes running in its group: stopped them\n"
            if not ended:
                code = TIMED_OUT
                output += f"\n{name}: still running at its bound of {bound(name):.0f} s: stopped it\n"
            ran.append((message, code, output, left, time.monotonic() - start))
        return ran

    workers = max(1, len(os.sched_getaffinity(0)))
    failed, times = [], {}
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for ran in pool.map(run, jobs):
            for message, code, output, left, seconds in ran:
                name = label(message)
                slowest = last.get(name, [0.0, None])[1]
                times[name] = [round(seconds, 2),
                               max(slowest or 0.0, round(seconds, 2)) if code == 0 else slowest]
                summary = [l for l in output.splitlines() if l.startswith("test result:")]
                status = ("ok" if code == 0 else "TIMED OUT" if code == TIMED_OUT
                          else f"FAILED (exit {code})")
                stray = f" (left {len(left)} running: stopped)" if left else ""
                print(f"  {seconds:6.1f} s  {name}: {status} {summary[-1][12:] if summary else ''}"
                      f"{stray}")
                if code != 0:
                    failed.append((name, output))
    try:
        with open(TIMES, "w") as file:
            json.dump({**last, **times}, file, indent=0, sort_keys=True)
    except OSError:
        pass
    print(f"  {len(found)} test binaries in {time.monotonic() - started:.0f} s, {workers} at a time,"
          f" {len(shared)} sharing trybuild's build one after another")
    for name, output in failed:
        print(f"\n---- {name} ----\n{output}", end="")
    if failed:
        print(f"\ntest: {len(failed)} of {len(found)} binaries failed: "
              + ", ".join(name for name, _ in failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

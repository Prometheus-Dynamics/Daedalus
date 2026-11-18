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
"""
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TARGET = os.environ.get("CARGO_TARGET_DIR") or os.path.join(ROOT, "target")
TIMES = os.path.join(TARGET, "test-parallel-times.json")
CARGO = os.environ.get("CARGO", "cargo")


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
    found.sort(key=lambda m: -last.get(label(m), 0.0))

    def run(message):
        start = time.monotonic()
        done = subprocess.run(
            [message["executable"]], cwd=os.path.dirname(message["manifest_path"]),
            env=package_env(message, sysroot_lib), stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, errors="replace",
        )
        return message, done, time.monotonic() - start

    workers = max(1, len(os.sched_getaffinity(0)))
    failed, times = [], {}
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for message, done, seconds in pool.map(run, found):
            name = label(message)
            times[name] = round(seconds, 2)
            summary = [l for l in done.stdout.splitlines() if l.startswith("test result:")]
            status = "ok" if done.returncode == 0 else f"FAILED (exit {done.returncode})"
            print(f"  {seconds:6.1f} s  {name}: {status} {summary[-1][12:] if summary else ''}")
            if done.returncode != 0:
                failed.append((name, done.stdout))
    try:
        with open(TIMES, "w") as file:
            json.dump(times, file, indent=0, sort_keys=True)
    except OSError:
        pass
    print(f"  {len(found)} test binaries in {time.monotonic() - started:.0f} s, {workers} at a time")
    for name, output in failed:
        print(f"\n---- {name} ----\n{output}", end="")
    if failed:
        print(f"\ntest: {len(failed)} of {len(found)} binaries failed: "
              + ", ".join(name for name, _ in failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

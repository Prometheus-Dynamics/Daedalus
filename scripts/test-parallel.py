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

Each binary's line gives its wall time and its CPU seconds (its own and those of the children it
reaped), and the last line their sum: the run's share of the step's CPU budget.

A binary is run as cargo runs it: in its package's directory, with cargo's variables for that
package, and the target's library directories on the library path.

With DAEDALUS_TEST_PACKAGES set to package names, only those packages' binaries run, from the
same build: scripts/check.sh runs a change's packages this way, and the gate those whose tests
did not pass before on the same inputs (scripts/lane-reuse.py). With DAEDALUS_TEST_PASSED set to a
file, the names of the packages whose every binary passed are written to it, failed run or not,
but none when a test built a fixture FIXTURES doesn't name (below): which test did isn't known.
With DAEDALUS_TEST_VISIBLE set to a JSON file of package names to paths of the checkout, each of
those packages' binaries runs under bwrap seeing only those paths of the checkout, the rest of the
machine as it is (`sandboxed`). With DAEDALUS_TEST_ARTIFACTS set to a file, cargo's artifact
messages for the build are written to it (`scripts/lane-reuse.py reads`). With DAEDALUS_TEST_PLAN
set to a file too (the gate, scripts/ci.d/test.sh), each package's tests are keyed by that build
(`scripts/lane-reuse.py plan`, into the file), and only those that didn't pass before on their key
run, each narrowly keyed one's seeing only what its key holds of the checkout (the file's .visible).

Each binary runs under a bound: three times the slowest it has passed in, and never under FLOOR.
The bound is on time the binary could use: the time its threads spent waiting for a CPU on a busy
machine is not counted (scripts/bounded.py, `wait_fair`), and a binary still running at ten times
its bound is stopped whatever it was waiting for.
The slowest is the larger of two records: the times file in the build directory, and SLOWEST, the
slowest green run of each binary in gates on fresh slots under load, committed with the tree. A
build directory's own record is too short to bound a binary by alone: a warm one that has seen one
quick pass stopped a binary at 83 s that passes in 67 s cold. A binary with no passing run on
either record gets
UNKNOWN, three times the slowest any binary has passed in at a gate (192 s, the wgpu-author compile
tests under three gates at once). Each runs in a process group of its own (scripts/bounded.py): past
its bound the group is stopped and the binary fails, named, and what a binary leaves running when it
ends is stopped and named. Its output goes to a file, so a child left holding it never blocks the
run. A TERM to this runner stops every binary's group.

Some tests build a fixture with cargo, each into a directory of its own under the test target
directory. Those builds are made here first, each with the command its tests use, before any
binary runs: a test's own build then finds everything fresh, and a fixture that has to be rebuilt
after a change is not timed against a binary's bound. A fixture that fails to build fails the step,
named. After the run, a build directory under the test target directory that FIXTURES doesn't name
fails the step too, so a test that starts building a fixture of its own is added here. One there
before the run (a restored cache's, of a fixture since renamed) is removed first.

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
from bounded import OUTER, TIMED_OUT, Group, scaled  # noqa: E402

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TARGET = os.environ.get("CARGO_TARGET_DIR") or os.path.join(ROOT, "target")
TIMES = os.path.join(TARGET, "test-parallel-times.json")
CARGO = os.environ.get("CARGO", "cargo")
# The slowest green run of each binary in cold and loaded gates (`scripts/ci.sh` in a fresh slot).
SLOWEST = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test-slowest.json")
FLOOR = 60.0
UNKNOWN = 600.0
# CARGO_TARGET_TMPDIR, where the tests build their fixtures.
TEST_TMP = os.path.join(TARGET, "tmp")
# Each fixture the tests build: the directory under TEST_TMP, the arguments after `cargo build -q`
# (run from the workspace root), and the compiler flags added to RUSTFLAGS. They are the commands
# in the tests: testing/crates/drive-conformance/src/run.rs and the tests' support files
# (core/loader/tests/support.rs builds the baseline and the old fixture, service/daedalus-ctl/tests/ctl.rs
# the daemon, backends/process/tests/support.rs and service/daedalusd/tests/workers.rs the worker).
FIXTURES = [
    ("dylib-fixture", ["--manifest-path", "testing/fixtures/dylib/Cargo.toml"], None),
    ("dylib-fixture-baseline", ["--manifest-path", "testing/fixtures/dylib/Cargo.toml"],
     "--cfg daedalus_abi_baseline"),
    ("dylib-old-fixture", ["--manifest-path", "testing/fixtures/dylib-old/Cargo.toml"], None),
    ("worker-fixture", ["-p", "daedalus-worker-fixture"], None),
    ("ctl-daemon", ["--manifest-path", "Cargo.toml", "-p", "daedalusd"], None),
]
# Build directories under TEST_TMP that other steps make, beside this one: not the tests'.
NOT_TESTS = {"dylib-fixture-budgets"}  # the budgets step, testing/crates/budget-harness/check.py


def build_fixtures():
    """Builds every fixture in FIXTURES, side by side. The names of those that failed, with output."""
    def build(fixture):
        name, args, flags = fixture
        env = dict(os.environ)
        if flags:
            env["RUSTFLAGS"] = (env.get("RUSTFLAGS", "") + " " + flags).strip()
        done = subprocess.run([CARGO, "build", "-q", *args, "--target-dir",
                               os.path.join(TEST_TMP, name)], cwd=ROOT, env=env,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        return name, done.returncode, done.stdout
    with ThreadPoolExecutor(max_workers=len(FIXTURES)) as pool:
        return [(name, output) for name, code, output in pool.map(build, FIXTURES) if code != 0]


def unlisted_fixtures():
    """Build directories under TEST_TMP that FIXTURES doesn't name: cargo marks its own."""
    named = {name for name, _, _ in FIXTURES} | NOT_TESTS
    try:
        entries = os.listdir(TEST_TMP)
    except OSError:
        return []
    return sorted(e for e in entries if e not in named
                  and os.path.exists(os.path.join(TEST_TMP, e, "CACHEDIR.TAG")))


def binaries(args):
    """Each test binary cargo would run, with its package's manifest."""
    out = subprocess.run(
        [CARGO, "test", *args, "--no-run", "--message-format=json-render-diagnostics"],
        cwd=ROOT, stdout=subprocess.PIPE, check=True, text=True,
    ).stdout
    kept = os.environ.get("DAEDALUS_TEST_ARTIFACTS")
    if kept:
        # What every unit was built from (scripts/lane-reuse.py reads).
        with open(kept, "w") as file:
            file.write(out)
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


def sandboxed(seen):
    """The command that runs a binary seeing only `seen` of the checkout: the checkout is an empty
    directory with those paths bound back in, and the build directory if it is inside. `seen` is
    a list of paths (a directory, ending in /, with all it holds), or {"bind": paths, "make":
    directories}, the directories made empty first (scripts/lane-reuse.py plan). Nothing else
    changes: the same processes, devices and the rest of the file system."""
    if seen is None:
        return []
    argv = ["bwrap", "--dev-bind", "/", "/", "--tmpfs", ROOT]
    if isinstance(seen, dict):
        for path in seen["make"]:
            argv += ["--dir", os.path.join(ROOT, path.rstrip("/"))]
        seen = seen["bind"]
    inside = [os.path.join(ROOT, path.rstrip("/")) for path in seen]
    if os.path.realpath(TARGET).startswith(os.path.realpath(ROOT) + os.sep):
        inside.append(TARGET)
    for path in inside:
        if os.path.exists(path):
            argv += ["--bind", path, path]
    return argv + ["--"]


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
    visible = os.environ.get("DAEDALUS_TEST_VISIBLE")
    plan = os.environ.get("DAEDALUS_TEST_PLAN")
    if plan:
        # Each package keyed by what was just built: only those that didn't pass before on it run.
        subprocess.run([sys.executable, "-I", os.path.join(ROOT, "scripts", "lane-reuse.py"), "plan",
                        plan, os.environ["DAEDALUS_TEST_ARTIFACTS"], "--", *sys.argv[1:]], check=True)
        with open(plan) as file:
            only = " ".join(l.split("\t")[0] for l in file if l.rstrip("\n").endswith("\trun"))
        visible = plan + ".visible"
    if only is not None:
        wanted = set(only.split())
        found = [m for m in found if package_of(m["package_id"], m["manifest_path"])[0] in wanted]
        print(f"  {len(found)} of {built} test binaries: those of DAEDALUS_TEST_PACKAGES")
    # In the gate, a narrowly keyed package's binaries see only the paths of the checkout its
    # reuse key holds, so a pass recorded under that key read nothing the key doesn't.
    if visible:
        with open(visible) as file:
            visible = json.load(file)
    else:
        visible = {}
    shared = [m for m in found if shares_trybuild(m)]
    jobs = [[m] for m in found if m not in shared] + ([shared] if shared else [])
    jobs.sort(key=lambda job: -sum(last.get(label(m), [0.0])[0] for m in job))

    try:
        with open(SLOWEST) as file:
            committed = json.load(file)
    except (OSError, ValueError):
        committed = {}

    def bound(name):
        slowest = max(last.get(name, [0.0, None])[1] or 0.0, committed.get(name, 0.0))
        if not slowest:
            return scaled(UNKNOWN)
        return scaled(max(FLOOR, 3 * slowest))

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
            seen = visible.get(package_of(message["package_id"], message["manifest_path"])[0])
            with tempfile.TemporaryFile() as out:
                group = Group(sandboxed(seen) + [message["executable"]],
                              cwd=os.path.dirname(message["manifest_path"]),
                              env=package_env(message, sysroot_lib), stdout=out,
                              stderr=subprocess.STDOUT)
                with groups_lock:
                    groups.add(group)
                ran_for = []
                ended = group.wait_fair(bound(name), ran_for)
                code, left = group.reap()
                cpu = group.cpu
                with groups_lock:
                    groups.discard(group)
                out.seek(0)
                output = out.read().decode(errors="replace")
            if seen is not None and code != 0:
                output += (f"\n{name}: ran seeing only the files of the checkout its package's "
                           "reuse key holds (scripts/lane-reuse.py runtime): a test that reads "
                           "another, or a file it was compiled from, fails here. One that must is "
                           "listed in WHOLE_TREE there.\n")
            if not ended:
                left = []
            if left:
                output += f"\n{name}: left {len(left)} processes running in its group: stopped them\n"
            if not ended:
                code = TIMED_OUT
                wall, lost = ran_for
                why = (f"after {wall:.0f} s, {OUTER} times its bound"
                       if wall >= OUTER * bound(name) else
                       f"after {wall:.0f} s, {lost:.0f} s of them spent waiting for a CPU"
                       if lost >= 1 else "")
                output += (f"\n{name}: still running at its bound of {bound(name):.0f} s"
                           f"{' ' + why if why else ''}: stopped it\n")
            ran.append((message, code, output, left, time.monotonic() - start, cpu))
        return ran

    # A build directory here that FIXTURES doesn't name, from before this run, belongs to no test
    # of this tree: a restored CI cache keeps those of fixtures since renamed. It goes, so one
    # found after the run is one a test of this tree built.
    import shutil
    for stale in unlisted_fixtures():
        shutil.rmtree(os.path.join(TEST_TMP, stale), ignore_errors=True)
        print(f"  removed {stale} from {TEST_TMP}: a build directory no test here makes")
    started = time.monotonic()
    # With no binary to run (every package passed before on what was built), no test reads one.
    broken = build_fixtures() if found else []
    for name, output in broken:
        print(f"\n---- fixture {name} ----\n{output}", end="")
    if broken:
        print("\ntest: the fixtures did not build: " + ", ".join(name for name, _ in broken))
        return 1
    if found:
        print(f"  {len(FIXTURES)} fixtures built in {time.monotonic() - started:.0f} s")

    workers = max(1, len(os.sched_getaffinity(0)))
    failed, times, spent = [], {}, 0.0
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for ran in pool.map(run, jobs):
            for message, code, output, left, seconds, cpu in ran:
                name = label(message)
                slowest = last.get(name, [0.0, None])[1]
                times[name] = [round(seconds, 2),
                               max(slowest or 0.0, round(seconds, 2)) if code == 0 else slowest]
                summary = [l for l in output.splitlines() if l.startswith("test result:")]
                status = ("ok" if code == 0 else "TIMED OUT" if code == TIMED_OUT
                          else f"FAILED (exit {code})")
                stray = f" (left {len(left)} running: stopped)" if left else ""
                spent += cpu
                print(f"  {seconds:6.1f} s {cpu:6.1f} cpu-s  {name}: {status} {summary[-1][12:] if summary else ''}"
                      f"{stray}")
                if code != 0:
                    failed.append((name, output))
    try:
        with open(TIMES, "w") as file:
            json.dump({**last, **times}, file, indent=0, sort_keys=True)
    except OSError:
        pass
    print(f"  {len(found)} test binaries in {time.monotonic() - started:.0f} s and {spent:.0f} CPU-s,"
          f" {workers} at a time,"
          f" {len(shared)} sharing trybuild's build one after another")
    for name, output in failed:
        print(f"\n---- {name} ----\n{output}", end="")
    # A fixture no test is named for fails the run, and which test built it isn't known: no
    # package is recorded as passed, or the next gate would reuse them all and never see it.
    unlisted = unlisted_fixtures()
    passed_file = os.environ.get("DAEDALUS_TEST_PASSED")
    if passed_file:
        names = set() if unlisted else set(only.split()) if only is not None else {
            package_of(m["package_id"], m["manifest_path"])[0] for m in found}
        names -= {name.split(" ", 1)[0] for name, _ in failed}
        with open(passed_file, "w") as file:
            file.write("".join(f"{name}\n" for name in sorted(names)))
    if unlisted:
        print("\ntest: a test built a fixture that scripts/test-parallel.py FIXTURES doesn't build"
              " first: " + ", ".join(unlisted))
        return 1
    if failed:
        print(f"\ntest: {len(failed)} of {len(found)} binaries failed: "
              + ", ".join(name for name, _ in failed))
        return 1
    return 0


def self_test():
    """This runner in a planted tree, with a cargo that lists one test binary and builds nothing.
    A binary that passes but leaves a fixture FIXTURES doesn't name fails the run and records no
    package; a stale one from before the run is removed and the run passes. A narrowly keyed
    package's binary that reads a file of the checkout its key doesn't hold fails, says why, and
    records nothing; the same read passes unsandboxed, and passes when the key holds the file. Held
as the gate holds it (files bound into directories made empty), a file the key doesn't bind, as
one it was compiled from, can't be read."""
    import shutil

    work = tempfile.mkdtemp(prefix="daedalus-test-parallel.")
    failures = []
    try:
        for name in ("scripts", "bin"):
            os.makedirs(os.path.join(work, name))
        # This script, as it is (a mutant of it too), and the bounds it imports.
        shutil.copy2(os.path.abspath(__file__), os.path.join(work, "scripts", "test-parallel.py"))
        shutil.copy2(os.path.join(ROOT, "scripts", "bounded.py"), os.path.join(work, "scripts", "bounded.py"))
        test = os.path.join(work, "bin", "t")
        outside = os.path.join(work, "outside.txt")
        artifact = json.dumps({
            "reason": "compiler-artifact", "executable": test, "profile": {"test": True},
            "target": {"kind": ["test"], "name": "t"},
            "package_id": f"path+file://{work}#planted@0.1.0",
            "manifest_path": os.path.join(work, "Cargo.toml")})
        scripts = {
            "cargo": f"#!/bin/sh\ncase \" $* \" in *' --no-run '*) echo '{artifact}';; esac\n",
            "bin/t": '#!/bin/sh\n[ -z "$PLANT_ROGUE" ] || { mkdir -p "$CARGO_TARGET_DIR/tmp/rogue" &&'
                     ' touch "$CARGO_TARGET_DIR/tmp/rogue/CACHEDIR.TAG"; }\n'
                     '[ -z "$PLANT_READ" ] || cat "$PLANT_READ" > /dev/null\n',
            "outside.txt": "read by the planted test\n",
        }
        for name, text in scripts.items():
            with open(os.path.join(work, name), "w") as file:
                file.write(text)
            os.chmod(os.path.join(work, name), 0o755)
        sandbox = shutil.which("bwrap") is not None
        cases = [
            # (case, settings, the stale fixture, the paths the key holds, exit, recorded)
            ("an unlisted fixture", {"PLANT_ROGUE": "1"}, False, None, 1, ""),
            ("a clean pass", {}, False, None, 0, "planted\n"),
            ("a stale fixture from before the run", {}, True, None, 0, "planted\n"),
            ("a read outside, unsandboxed", {"PLANT_READ": outside}, False, None, 0, "planted\n"),
        ]
        if sandbox:
            cases += [
                ("a narrow key's test reading outside it", {"PLANT_READ": outside}, False,
                 ["bin/"], 1, ""),
                ("a narrow key's test reading what it holds", {"PLANT_READ": outside}, False,
                 ["bin/", "outside.txt"], 0, "planted\n"),
                # As the gate holds it: the files its key holds, in directories made empty. A file
                # it was compiled from is keyed by its binary and not bound: reading it fails.
                ("a narrow key's test reading a file it was compiled from", {"PLANT_READ": outside},
                 False, {"bind": ["bin/t"], "make": ["bin/"]}, 1, ""),
                ("a narrow key's test reading a file its key holds", {"PLANT_READ": outside},
                 False, {"bind": ["bin/t", "outside.txt"], "make": ["bin/"]}, 0, "planted\n"),
            ]
        else:
            failures.append("no bwrap: a narrow key's tests can't be held to it")
        for case, settings, stale, seen, code, recorded in cases:
            target, passed = os.path.join(work, "target"), os.path.join(work, "passed")
            shutil.rmtree(target, ignore_errors=True)
            if stale:
                os.makedirs(os.path.join(target, "tmp", "stale-fixture"))
                open(os.path.join(target, "tmp", "stale-fixture", "CACHEDIR.TAG"), "w").close()
            if os.path.exists(passed):
                os.remove(passed)
            env = {k: v for k, v in os.environ.items()
                   if k not in ("DAEDALUS_TEST_PACKAGES", "DAEDALUS_TEST_VISIBLE", "DAEDALUS_TEST_ARTIFACTS")}
            env.update(CARGO=os.path.join(work, "cargo"), CARGO_TARGET_DIR=target,
                       DAEDALUS_TEST_PASSED=passed, **settings)
            if seen is not None:
                with open(os.path.join(work, "visible.json"), "w") as file:
                    json.dump({"planted": seen}, file)
                env["DAEDALUS_TEST_VISIBLE"] = os.path.join(work, "visible.json")
            done = subprocess.run([sys.executable, "-I", os.path.join(work, "scripts", "test-parallel.py")],
                                  env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            got = open(passed).read() if os.path.exists(passed) else ""
            if os.path.exists(os.path.join(target, "tmp", "stale-fixture")):
                failures.append(f"{case}: the stale build directory is still there")
            if done.returncode != code or got != recorded:
                failures.append(f"{case}: exit {done.returncode} (want {code}), recorded "
                                f"{got.split() or 'nothing'} (want {recorded.split() or 'nothing'})"
                                f"\n{done.stdout}")
            elif code and seen is not None and "ran seeing only" not in done.stdout:
                failures.append(f"{case}: the failure doesn't say the test ran held to its key")
    finally:
        shutil.rmtree(work, ignore_errors=True)
    for failure in failures:
        print(f"test-parallel self-test: {failure}", file=sys.stderr)
    if not failures:
        print("test-parallel self-test: an unlisted fixture fails the run and records nothing, a "
              "stale one is removed; a narrow key's test that reads outside it fails, named, and "
              "records nothing, while the same read passes unsandboxed and when the key holds it")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(self_test() if sys.argv[1:] == ["--self-test"] else main())

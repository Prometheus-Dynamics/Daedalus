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
import contextlib
import io
import json
import os
import re
import shutil
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


def fixture_cargo(directory):
    """A cargo for a narrowly keyed package's tests, written into `directory`: the fixtures were
    built before their keys (scripts/lane-reuse.py holds each by its outputs), so a test's own
    `cargo build` of one into its directory under TEST_TMP is answered as fresh, and the fixture's
    sources stay out of what the test sees. Any other cargo run is the real one."""
    import shlex
    path = os.path.join(directory, "cargo")
    built = " | ".join(shlex.quote(os.path.join(TEST_TMP, name)) for name, _, _ in FIXTURES)
    with open(path, "w") as file:
        file.write(f"""#!/bin/sh
# scripts/test-parallel.py fixture_cargo: a build of a fixture built before the keys is fresh.
target= previous=
for argument in "$@"; do [ "$previous" = --target-dir ] && target=$argument; previous=$argument; done
if [ "$1" = build ]; then
  case "$target" in
    {built}) exit 0 ;;
  esac
fi
exec {shlex.quote(CARGO)} "$@"
""")
    os.chmod(path, 0o755)
    return path


def fixtures_failed(broken):
    for name, output in broken:
        print(f"\n---- fixture {name} ----\n{output}", end="")
    print("\ntest: the fixtures did not build: " + ", ".join(name for name, _ in broken))
    return 1


def unlisted_fixtures():
    """Build directories under TEST_TMP that FIXTURES doesn't name: cargo marks its own."""
    named = {name for name, _, _ in FIXTURES} | NOT_TESTS
    try:
        entries = os.listdir(TEST_TMP)
    except OSError:
        return []
    return sorted(e for e in entries if e not in named
                  and os.path.exists(os.path.join(TEST_TMP, e, "CACHEDIR.TAG")))


class Phases:
    """The step's phases, each in wall seconds and CPU seconds: this process's and the children it
    waited for (cargo's build, lane-reuse.py's keys, the binaries)."""

    def __init__(self):
        self.done, self.mark = [], self.now()

    @staticmethod
    def now():
        spent = os.times()
        return time.monotonic(), spent[0] + spent[1] + spent[2] + spent[3]

    def end(self, name):
        wall, cpu = self.now()
        self.done.append((name, wall - self.mark[0], cpu - self.mark[1]))
        self.mark = (wall, cpu)

    def line(self):
        return ", ".join(f"{name} {wall:.1f} s {cpu:.1f} CPU-s" for name, wall, cpu in self.done)


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


def sandboxed(seen, stages=None):
    """The command that runs a binary seeing only `seen` of the checkout: the checkout is an empty
    directory with those paths in it, and the build directory if it is inside. `seen` is a list of
    paths (a directory, ending in /, with all it holds), or {"bind": paths, "make": directories},
    the directories made empty first (scripts/lane-reuse.py plan). Nothing else changes: the same
    processes, devices and the rest of the file system. Nothing can be written into the checkout but
into its live directories and the build directory.

    With `stages` (a Stages), the checkout is one read-only mount: a directory holding the files as
    hard links, laid out once for every binary that sees the same paths. One bind per file costs
    bwrap seconds a binary under load: 8-10 s for the 1602 of a package keyed by its sources, at
    load 90. Directories are bound live on top either way (their files may appear as the test
    runs), then the build directory, then the files it sees empty."""
    if seen is None:
        return []
    make, blank = [], []
    if isinstance(seen, dict):
        make = seen["make"]
        blank = seen.get("blank", [])
        seen = seen["bind"]
    paths = [path for path in seen if os.path.exists(os.path.join(ROOT, path.rstrip("/")))]
    files = [path for path in paths if not os.path.isdir(os.path.join(ROOT, path))]
    live = [path for path in paths if path not in files]
    target = os.path.realpath(TARGET)
    inside = target.startswith(os.path.realpath(ROOT) + os.sep)
    # Where the mounts on top go: made in the stage, which is read-only once mounted.
    points = make + live + ([os.path.relpath(target, os.path.realpath(ROOT))] if inside else [])
    stage = stages.stage(files, points, blank) if stages is not None else None
    argv = ["bwrap", "--dev-bind", "/", "/"]
    if stage:
        argv += ["--ro-bind", stage, ROOT]
    else:
        argv += ["--tmpfs", ROOT]
        for path in make:
            argv += ["--dir", os.path.join(ROOT, path.rstrip("/"))]
        for path in files:
            argv += ["--ro-bind", os.path.join(ROOT, path), os.path.join(ROOT, path)]
    for path in live:
        argv += ["--bind", os.path.join(ROOT, path.rstrip("/")), os.path.join(ROOT, path.rstrip("/"))]
    if inside:
        argv += ["--bind", TARGET, TARGET]
    # Files that are there, empty (scripts/lane-reuse.py blanks): in the stage, empty read-only
    # files of its own (one mount for them all: a trybuild package sees about a thousand, one bind
    # each cost bwrap 15-20 s a binary); bound one by one, one empty read-only file bound on each.
    # Not /dev/null: a bind is mounted nodev, where a device can't even be opened to be read. The
    # empty directory is then read-only too, once every mount is on it.
    if not stage:
        for path in blank:
            argv += ["--ro-bind", stages.empty(), os.path.join(ROOT, path)]
        argv += ["--remount-ro", ROOT]
    return argv + ["--"]


def stage_home():
    """Where the stages go: on the checkout's file system, where its files can be hard-linked. The
    build directory when it is there (CI's, inside the checkout); else beside the checkout, in its
    parent, when that is there and can be written (a build directory of its own mount); else the
    build directory, where every set is then bound one by one, and the run says so. Never inside
    the checkout, which steps beside this one scan."""
    device = os.stat(ROOT).st_dev
    for home in (TARGET, os.path.dirname(ROOT)):
        try:
            if os.stat(home).st_dev == device and os.access(home, os.W_OK):
                return home
        except OSError:
            continue
    return TARGET


STAGE = re.compile(r"\.daedalus-stages\.(\d+)\.")


def stage_dir():
    """The directory this run's stages go in: the build directory, or beside the checkout one
    shared by the checkouts there (.daedalus-stages), each run's named by its PID. A run's stages
    whose PID is gone (stopped by a KILL) are removed here, and named."""
    home = stage_home()
    if home != TARGET:
        home = os.path.join(home, ".daedalus-stages")
        os.makedirs(home, exist_ok=True)
    for name in sorted(os.listdir(home)):
        found = STAGE.match(name)
        if not found:
            continue
        try:
            os.kill(int(found.group(1)), 0)
        except ProcessLookupError:
            shutil.rmtree(os.path.join(home, name), ignore_errors=True)
            print(f"  removed {os.path.join(home, name)}: stages left by PID {found.group(1)}, "
                  "which is gone")
        except PermissionError:
            pass
    return home


def empty_file(directory):
    """An empty read-only regular file made in `directory`, for the files a key sees empty."""
    path = os.path.join(directory, "empty")
    if not os.path.exists(path):
        open(path, "x").close()
        os.chmod(path, 0o444)
    return path


class Stages:
    """The directories `sandboxed` mounts: each set of files of the checkout as hard links under
    `under`, made once and shared. A file that can't be linked (`under` on another file system)
    is never copied: that set's files are bound one by one, and the run says so."""

    def __init__(self, under):
        self.under, self.made, self.lock = under, {}, threading.Lock()

    def empty(self):
        with self.lock:
            return empty_file(self.under)

    def stage(self, files, points, blank):
        key = json.dumps([sorted(files), sorted(points), sorted(blank)])
        # One lock for each set: sets are laid out side by side, a set once.
        with self.lock:
            entry = self.made.setdefault(key, [threading.Lock(), None, False])
        with entry[0]:
            if not entry[2]:
                entry[1], entry[2] = self.lay_out(files, points, blank), True
            return entry[1]

    def lay_out(self, files, points, blank):
        stage = tempfile.mkdtemp(dir=self.under)
        for path in points:
            os.makedirs(os.path.join(stage, path.rstrip("/")), exist_ok=True)
        try:
            for path in files:
                into = os.path.join(stage, path)
                os.makedirs(os.path.dirname(into), exist_ok=True)
                os.link(os.path.join(ROOT, path), into)
        except OSError as error:
            print(f"  test-parallel: can't lay out a key's files as hard links in {self.under} "
                  f"({error}): binding them one by one, seconds a binary", file=sys.stderr)
            shutil.rmtree(stage, ignore_errors=True)
            return None
        for path in blank:
            into = os.path.join(stage, path)
            os.makedirs(os.path.dirname(into), exist_ok=True)
            # Made here, never a link to the checkout's file: empty and read-only.
            open(into, "x").close()
            os.chmod(into, 0o444)
        if not staged_as_keyed(stage, files, blank):
            print(f"  test-parallel: the files laid out in {stage} are not the key's: binding them "
                  "one by one", file=sys.stderr)
            shutil.rmtree(stage, ignore_errors=True)
            return None
        return stage


def staged_as_keyed(stage, files, blank):
    """Whether `stage` holds exactly the key's files, each the checkout's own (the same inode, so
    the same content), and the files it sees empty: each a regular file of its own (one link, not
    the checkout's inode), read-only and empty."""
    import stat
    found = set()
    for directory, _, names in os.walk(stage):
        found.update(os.path.relpath(os.path.join(directory, name), stage) for name in names)
    if found != set(files) | set(blank):
        return False
    for path in blank:
        mark = os.lstat(os.path.join(stage, path))
        source = os.path.join(ROOT, path)
        if (not stat.S_ISREG(mark.st_mode) or mark.st_size != 0 or mark.st_nlink != 1 or
                stat.S_IMODE(mark.st_mode) != 0o444 or
                (os.path.exists(source) and os.path.samefile(os.path.join(stage, path), source))):
            return False
    return all(os.path.samefile(os.path.join(stage, path), os.path.join(ROOT, path)) for path in files)


def shares_trybuild(message):
    """Whether the binary links trybuild, by its symbols: wherever in the test the use sits."""
    with open(message["executable"], "rb") as file:
        return b"trybuild" in file.read()


def reaching_trybuild(meta):
    """The packages, of cargo's metadata `meta`, whose test binaries can link trybuild: trybuild
    is in what their tests build (their dependencies of every kind, then those's own, but not
    their dev-dependencies). Only their binaries are read for it (`shares_trybuild`): every test
    binary is 3.5 GiB, read in 15 s at load 124 to find 7."""
    names = {p["id"]: p["name"] for p in meta["packages"]}
    edges = {n["id"]: [(d["pkg"], {k["kind"] for k in d["dep_kinds"]}) for d in n["deps"]]
             for n in meta["resolve"]["nodes"]}
    reaching = set()
    for package in edges:
        seen, todo = set(), [d for d, _ in edges[package]]
        while todo:
            current = todo.pop()
            if current not in seen:
                seen.add(current)
                todo.extend(d for d, kinds in edges.get(current, []) if kinds != {"dev"})
        if any(names.get(i) == "trybuild" for i in seen):
            reaching.add(package)
    return reaching


def trybuild_candidates(found, cargo=None):
    """The binaries of `found` that may link trybuild, by cargo metadata (`reaching_trybuild`). If
    cargo metadata fails, every binary, and the run says so: none is ever left unread."""
    done = subprocess.run([cargo or CARGO, "metadata", "--locked", "--format-version", "1"], cwd=ROOT,
                          capture_output=True, text=True)
    try:
        reaching = reaching_trybuild(json.loads(done.stdout)) if done.returncode == 0 else None
    except (ValueError, KeyError):
        reaching = None
    if reaching is None:
        print(f"  test: cargo metadata failed (exit {done.returncode}): every binary is read for "
              f"trybuild\n{done.stderr.strip()}")
        return list(found)
    return [m for m in found if m["package_id"] in reaching]


def label(message):
    kinds = message["target"]["kind"]
    kind = "unittests" if any(k in ("lib", "rlib", "proc-macro", "cdylib", "staticlib") for k in kinds) else kinds[0]
    name, _ = package_of(message["package_id"], message["manifest_path"])
    return f"{name} {kind} {message['target']['name']}"


def main():
    args = ["--locked", "--workspace", "--all-targets", *sys.argv[1:]]
    phases = Phases()
    found = binaries(args)
    phases.end("build")
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
    broken = None
    if plan:
        # The fixtures first: a package whose tests build one is keyed by what it built.
        started = time.monotonic()
        broken = build_fixtures()
        if broken:
            return fixtures_failed(broken)
        print(f"  {len(FIXTURES)} fixtures built in {time.monotonic() - started:.0f} s, before the keys")
        phases.end("fixtures")
        # Each package keyed by what was just built: only those that didn't pass before on it run.
        subprocess.run([sys.executable, "-I", os.path.join(ROOT, "scripts", "lane-reuse.py"), "plan",
                        plan, os.environ["DAEDALUS_TEST_ARTIFACTS"], "--", *sys.argv[1:]], check=True)
        phases.end("keys")
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
    import atexit
    shims = tempfile.mkdtemp(prefix="daedalus-cargo.")
    # Gone however the run ends.
    atexit.register(shutil.rmtree, shims, True)
    held_cargo = fixture_cargo(shims)
    stages, made = None, [shims]
    if visible:
        # On the checkout's file system, for hard links: gone however the run ends, a red or a stop.
        made.append(tempfile.mkdtemp(prefix=f".daedalus-stages.{os.getpid()}.", dir=stage_dir()))
        atexit.register(shutil.rmtree, made[-1], True)
        stages = Stages(made[-1])
    shared = [m for m in trybuild_candidates(found) if shares_trybuild(m)]
    jobs = [[m] for m in found if m not in shared] + ([shared] if shared else [])
    jobs.sort(key=lambda job: -sum(last.get(label(m), [0.0])[0] for m in job))
    if stages is not None:
        # Every set of files the binaries will see, laid out side by side before any runs: not
        # time a binary's bound counts, nor a wait the first binary of each package sits out.
        started = time.monotonic()
        sets = {json.dumps(seen, sort_keys=True): seen for seen in (
            visible.get(package_of(m["package_id"], m["manifest_path"])[0]) for m in found)
            if seen is not None}
        with ThreadPoolExecutor(max_workers=max(1, len(sets))) as pool:
            list(pool.map(lambda seen: sandboxed(seen, stages), sets.values()))
        alone = sum(1 for entry in stages.made.values() if entry[1] is None)
        print(f"  {len(sets)} sets of files: {len(sets) - alone} laid out, {alone} bound one by one"
              f"{' (no hard links into ' + stages.under + ')' if alone else ''}, in "
              f"{time.monotonic() - started:.0f} s, before the run")
        phases.end("layout")
        if alone and os.environ.get("DAEDALUS_TEST_STAGES"):
            # For the gate's last line (scripts/gate.sh): a set bound one by one costs seconds a
            # binary, and is said there too.
            with open(os.environ["DAEDALUS_TEST_STAGES"], "w") as file:
                file.write(f"{alone}-of-{len(sets)}-bound-one-by-one\n")
        if alone and os.environ.get("CI") == "true":
            # On CI the build directory is inside the checkout: a set it can't lay out is a bug.
            print(f"test: {alone} sets of files bound one by one on CI, where the build directory "
                  "is on the checkout's file system", file=sys.stderr)
            return 1

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
        for directory in made:
            shutil.rmtree(directory, ignore_errors=True)
        os._exit(128 + signum)

    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, stop_all)

    def run(job):
        ran = []
        for message in job:
            name = label(message)
            start = time.monotonic()
            seen = visible.get(package_of(message["package_id"], message["manifest_path"])[0])
            env = package_env(message, sysroot_lib)
            if seen is not None:
                env["CARGO"] = held_cargo
            with tempfile.TemporaryFile() as out:
                group = Group(sandboxed(seen, stages) + [message["executable"]],
                              cwd=os.path.dirname(message["manifest_path"]),
                              env=env, stdout=out,
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
    for stale in unlisted_fixtures():
        shutil.rmtree(os.path.join(TEST_TMP, stale), ignore_errors=True)
        print(f"  removed {stale} from {TEST_TMP}: a build directory no test here makes")
    started = time.monotonic()
    # With no binary to run (every package passed before on what was built), no test reads one.
    if broken is None and found:
        broken = build_fixtures()
        if broken:
            return fixtures_failed(broken)
        print(f"  {len(FIXTURES)} fixtures built in {time.monotonic() - started:.0f} s")
        phases.end("fixtures")

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
    phases.end("run")
    print(f"  phases: {phases.line()}")
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


def trybuild_plants():
    """Which binaries are read for trybuild: a package that reaches it only through a helper it
    builds is read, one that has it only as a helper's dev-dependency is not, nor one with no path
    to it; with cargo metadata failing, every binary is read, said."""
    failures = []
    node = lambda i, *deps: {"id": i, "deps": [{"pkg": d, "dep_kinds": [{"kind": k}]} for d, k in deps]}
    meta = {"packages": [{"id": i, "name": i} for i in ("direct", "helper", "through", "dev-only",
                                                         "tested", "none", "trybuild")],
            "resolve": {"nodes": [node("direct", ("trybuild", "dev")), node("helper", ("trybuild", None)),
                                  node("through", ("helper", "dev")), node("tested", ("trybuild", "dev")),
                                  node("dev-only", ("tested", "dev")), node("none"), node("trybuild")]}}
    got = reaching_trybuild(meta)
    want = {"direct", "helper", "through", "tested"}
    if got != want:
        failures.append(f"trybuild reached by {sorted(got)}, not {sorted(want)}")
    found = [{"package_id": "direct"}, {"package_id": "none"}]
    said = io.StringIO()
    with contextlib.redirect_stdout(said):
        read = trybuild_candidates(found, cargo="false")
    if read != found or "every binary is read for trybuild" not in said.getvalue():
        failures.append(f"with cargo metadata failing, read {read}, said {said.getvalue()!r}")
    return failures


def stage_plants(work):
    """Stages, planted: on another file system its files are bound one by one, said, never copied;
    a stage that holds a file the key doesn't, or a copy of one it does, is not the key's."""
    failures = []
    global ROOT
    root, ROOT = ROOT, work
    try:
        elsewhere = tempfile.mkdtemp(dir="/dev/shm") if os.path.isdir("/dev/shm") else None
        if elsewhere is None or os.stat(elsewhere).st_dev == os.stat(work).st_dev:
            failures.append("no other file system to lay a key's files out on")
        else:
            import contextlib
            import io
            said = io.StringIO()
            with contextlib.redirect_stderr(said):
                stage = Stages(elsewhere).stage(["outside.txt"], [], [])
            if stage is not None or "one by one" not in said.getvalue() or os.listdir(elsewhere):
                failures.append(f"on another file system: staged {stage}, said {said.getvalue()!r}")
        if elsewhere:
            shutil.rmtree(elsewhere, ignore_errors=True)
        under = tempfile.mkdtemp(dir=work)
        stages = Stages(under)
        stage = stages.stage(["outside.txt", "bin/t"], ["bin/"], ["empty.txt"])
        if stage and (stages.stage(["bin/t", "outside.txt"], ["bin/"], ["empty.txt"]) != stage or
                      len(os.listdir(under)) != 1):
            failures.append("a set of files asked for again is laid out again")
        if not stage or not staged_as_keyed(stage, ["outside.txt", "bin/t"], ["empty.txt"]):
            failures.append(f"a key's files laid out: {stage}, not the key's")
        else:
            open(os.path.join(stage, "extra.txt"), "w").close()
            if staged_as_keyed(stage, ["outside.txt", "bin/t"], ["empty.txt"]):
                failures.append("a stage with a file the key doesn't hold passes as the key's")
            os.remove(os.path.join(stage, "extra.txt"))
            # A file it sees empty that isn't: with content, or a link to the checkout's file.
            blank = os.path.join(stage, "empty.txt")
            os.chmod(blank, 0o644)
            with open(blank, "w") as file:
                file.write("planted\n")
            os.chmod(blank, 0o444)
            if staged_as_keyed(stage, ["outside.txt", "bin/t"], ["empty.txt"]):
                failures.append("a stage whose empty file has content passes as the key's")
            os.remove(blank)
            os.link(os.path.join(work, "outside.txt"), blank)
            if staged_as_keyed(stage, ["outside.txt", "bin/t"], ["outside.txt"]):
                failures.append("a stage whose empty file is a link to the checkout's passes as the key's")
            os.remove(blank)
            # Linked to a file outside the checkout too, or not a regular file at all.
            other = os.path.join(work, "planted-other")
            open(other, "x").close()
            os.link(other, blank)
            os.chmod(blank, 0o444)
            if staged_as_keyed(stage, ["outside.txt", "bin/t"], ["empty.txt"]):
                failures.append("a stage whose empty file has another link passes as the key's")
            os.remove(blank)
            os.mkfifo(blank, 0o444)
            if staged_as_keyed(stage, ["outside.txt", "bin/t"], ["empty.txt"]):
                failures.append("a stage whose empty file is a pipe passes as the key's")
            os.remove(blank)
            open(blank, "x").close()
            os.chmod(blank, 0o444)
            # The one bound on each when the files are bound one by one: empty and read-only.
            import stat
            mark = os.stat(stages.empty())
            if mark.st_size or stat.S_IMODE(mark.st_mode) != 0o444 or not stat.S_ISREG(mark.st_mode):
                failures.append(f"the empty file bound one by one: {oct(mark.st_mode)}, {mark.st_size} bytes")
            os.remove(os.path.join(stage, "outside.txt"))
            shutil.copy2(os.path.join(work, "outside.txt"), os.path.join(stage, "outside.txt"))
            if staged_as_keyed(stage, ["outside.txt", "bin/t"], ["empty.txt"]):
                failures.append("a stage with a copy of a key's file passes as the key's")
    finally:
        ROOT = root
    return failures


def self_test():
    """This runner in a planted tree, with a cargo that lists one test binary and builds nothing.
    A binary that passes but leaves a fixture FIXTURES doesn't name fails the run and records no
    package; a stale one from before the run is removed and the run passes. A narrowly keyed
    package's binary that reads a file of the checkout its key doesn't hold fails, says why, and
    records nothing; the same read passes unsandboxed, and passes when the key holds the file. Held
as the gate holds it (files bound into directories made empty), a file the key doesn't bind, as
one it was compiled from, can't be read."""
    # The planted checkout, in a parent of its own: made read-only, there is nowhere beside it.
    parent = tempfile.mkdtemp(prefix="daedalus-test-parallel.")
    work = os.path.join(parent, "checkout")
    os.makedirs(work)
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
        # A copy of the binary as a second package's, for a run holding two packages to two keys.
        second = artifact.replace("#planted@", "#planted-second@").replace(
            f'"{test}"', f'"{test}2"').replace('"name": "t"', '"name": "t2"')
        scripts = {
            "cargo": f"#!/bin/sh\ncase \" $* \" in *' --no-run '*) echo '{artifact}'; [ -z \"$PLANT_TWO\" ] || echo '{second}';; *) echo \"$*\" >> {work}/target/cargo.log;; esac\n",
            "bin/t": '#!/bin/sh\n[ -z "$PLANT_ROGUE" ] || { mkdir -p "$CARGO_TARGET_DIR/tmp/rogue" &&'
                     ' touch "$CARGO_TARGET_DIR/tmp/rogue/CACHEDIR.TAG"; }\n'
                     '[ -z "$PLANT_FIXTURE" ] || { "$CARGO" build -q --manifest-path planted-fixture'
                     ' --target-dir "$CARGO_TARGET_DIR/tmp/dylib-fixture" && "$CARGO" planted-other; } || exit 1\n'
                     '[ -z "$PLANT_READ" ] || cat "$PLANT_READ" > /dev/null || exit 1\n'
                     '[ -z "$PLANT_BLANK" ] || { [ "$(wc -c < "$PLANT_BLANK")" = 0 ] &&'
                     ' ! (echo x > "$PLANT_BLANK") 2> /dev/null && [ "$(wc -c < "$PLANT_BLANK")" = 0 ] &&'
                     ' [ -f "$PLANT_BLANK" ]; } || exit 1\n'
                     '[ -z "$PLANT_WRITE" ] || { ! (echo changed > outside.txt) 2> /dev/null &&'
                     ' ! (echo new > made-by-test) 2> /dev/null; } || exit 1\n'
                     '[ -z "$PLANT_MOUNTS" ] || [ "$(awk -v r="$PWD" \'$5 == r || index($5, r "/") == 1\''
                     ' /proc/self/mountinfo | wc -l)" -le "$PLANT_MOUNTS" ]\n',
            "outside.txt": "read by the planted test\n",
        }
        scripts["bin/t2"] = scripts["bin/t"]
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
            ("a fixture's build, unsandboxed", {"PLANT_FIXTURE": "1"}, False, None, 0, "planted\n"),
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
                # A file it sees empty (a member a trybuild package doesn't build): an empty,
                # read-only regular file of the stage's own. It reads 0 bytes and can't be written.
                ("a narrow key's test reading a file it sees empty", {"PLANT_BLANK": outside},
                 False, {"bind": ["bin/t"], "make": ["bin/"], "blank": ["outside.txt"]}, 0, "planted\n"),
                # The same with the files bound one by one: one empty read-only file on each.
                ("a narrow key's test reading a file it sees empty, its files bound one by one",
                 {"PLANT_BLANK": outside, "PLANT_ELSEWHERE": "1"},
                 False, {"bind": ["bin/t"], "make": ["bin/"], "blank": ["outside.txt"]}, 0, "planted\n"),
                ("the same file bound whole", {"PLANT_BLANK": outside},
                 False, {"bind": ["bin/t", "outside.txt"], "make": ["bin/"]}, 1, ""),
                # A fixture built before the keys: the test's own build of it is answered as fresh,
                # with none of its sources in sight; any other cargo run is the real one.
                ("a narrow key's test building a fixture", {"PLANT_FIXTURE": "1"},
                 False, {"bind": ["bin/t", "cargo"], "make": ["bin/"]}, 0, "planted\n"),
                # The files are one mount, however many: the checkout's directory and the build
                # directory inside it, not a bind for each file.
                ("a narrow key's files, one mount", {"PLANT_MOUNTS": "2", "PLANT_LEFTOVER": "1"},
                 False, {"bind": ["bin/t", "cargo", "outside.txt"], "make": ["bin/"]}, 0, "planted\n"),
                # A held test can't write into the checkout: its files are the checkout's own, by
                # hard links, so a write would change the source.
                ("a narrow key's test writing into the checkout", {"PLANT_WRITE": "1"},
                 False, {"bind": ["bin/t", "outside.txt"], "make": ["bin/"]}, 0, "planted\n"),
                # The build directory on another file system: the files are laid out beside the
                # checkout instead, one mount (the build directory isn't inside), and gone after.
                ("a narrow key's files with the build directory elsewhere, laid out beside",
                 {"PLANT_BESIDE": "1", "PLANT_MOUNTS": "1"},
                 False, {"bind": ["bin/t", "cargo", "outside.txt"], "make": ["bin/"]}, 0, "planted\n"),
                # The same with nowhere beside it either: no hard links, so the files are bound
                # one by one, said, and still can't be written.
                # On CI, where the build directory is inside the checkout, that is a bug: red.
                ("a set bound one by one on CI", {"PLANT_ELSEWHERE": "1", "CI": "true"},
                 False, {"bind": ["bin/t", "outside.txt"], "make": ["bin/"]}, 1, ""),
                ("a narrow key's test writing into the checkout, its files bound one by one",
                 {"PLANT_WRITE": "1", "PLANT_ELSEWHERE": "1"},
                 False, {"bind": ["bin/t", "outside.txt"], "make": ["bin/"]}, 0, "planted\n"),
                # Two packages held to two keys in one run: each sees its own files, whichever
                # was laid out first.
                # Both hold both binaries, so either one's files alone would run both.
                ("two packages, each held to its own key", {"PLANT_READ": outside, "PLANT_TWO": "1"},
                 False, {"planted": {"bind": ["bin/t", "bin/t2", "outside.txt"], "make": ["bin/"]},
                         "planted-second": {"bind": ["bin/t", "bin/t2"], "make": ["bin/"]}},
                 1, "planted\n"),
            ]
        else:
            failures.append("no bwrap: a narrow key's tests can't be held to it")
        for case, settings, stale, seen, code, recorded in cases:
            target, passed = os.path.join(work, "target"), os.path.join(work, "passed")
            shutil.rmtree(target, ignore_errors=True)
            os.makedirs(target)
            if "PLANT_ELSEWHERE" in settings or "PLANT_BESIDE" in settings:
                target = tempfile.mkdtemp(dir="/dev/shm")
            if "PLANT_ELSEWHERE" in settings:
                os.chmod(parent, 0o555)
            if stale:
                os.makedirs(os.path.join(target, "tmp", "stale-fixture"))
                open(os.path.join(target, "tmp", "stale-fixture", "CACHEDIR.TAG"), "w").close()
            if os.path.exists(passed):
                os.remove(passed)
            # In the build directory, which a held test sees.
            log = os.path.join(target, "cargo.log")
            env = {k: v for k, v in os.environ.items()
                   if k not in ("DAEDALUS_TEST_PACKAGES", "DAEDALUS_TEST_VISIBLE", "DAEDALUS_TEST_ARTIFACTS",
                                "DAEDALUS_TEST_STAGES", "CI")}
            stages_status = os.path.join(work, "stages.status")
            if os.path.exists(stages_status):
                os.remove(stages_status)
            env.update(CARGO=os.path.join(work, "cargo"), CARGO_TARGET_DIR=target,
                       DAEDALUS_TEST_PASSED=passed, DAEDALUS_TEST_STAGES=stages_status, **settings)
            # Stages a run stopped by a KILL left, and a live run's, from before this one.
            dead = os.path.join(target, ".daedalus-stages.4194999.planted")
            live = os.path.join(target, f".daedalus-stages.{os.getpid()}.planted")
            if "PLANT_LEFTOVER" in settings:
                os.makedirs(dead)
                os.makedirs(live)
            if seen is not None:
                with open(os.path.join(work, "visible.json"), "w") as file:
                    json.dump(seen if "PLANT_TWO" in settings else {"planted": seen}, file)
                env["DAEDALUS_TEST_VISIBLE"] = os.path.join(work, "visible.json")
            try:
                done = subprocess.run([sys.executable, "-I", os.path.join(work, "scripts", "test-parallel.py")],
                                      env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            finally:
                os.chmod(parent, 0o755)
            got = open(passed).read() if os.path.exists(passed) else ""
            if os.path.exists(os.path.join(target, "tmp", "stale-fixture")):
                failures.append(f"{case}: the stale build directory is still there")
            if done.returncode != code or got != recorded:
                failures.append(f"{case}: exit {done.returncode} (want {code}), recorded "
                                f"{got.split() or 'nothing'} (want {recorded.split() or 'nothing'})"
                                f"\n{done.stdout}")
            elif code and seen is not None and "CI" not in settings and "ran seeing only" not in done.stdout:
                failures.append(f"{case}: the failure doesn't say the test ran held to its key")
            if any(name.startswith(".daedalus-stages.") and name != os.path.basename(live)
                   for name in os.listdir(target)):
                failures.append(f"{case}: a key's files laid out for the run are still there")
            if "PLANT_TWO" in settings and "2 sets of files: 2 laid out" not in done.stdout:
                failures.append(f"{case}: the two sets of files weren't laid out before the run")
            if "PLANT_TWO" in settings and not re.search(
                    r"phases: build [\d.]+ s [\d.]+ CPU-s, (.+, )?layout [\d.]+ s [\d.]+ CPU-s, .*run [\d.]+ s "
                    r"[\d.]+ CPU-s$", done.stdout, re.M):
                failures.append(f"{case}: the phases (build, layout, run) aren't said, each in s and CPU-s")
            if "PLANT_LEFTOVER" in settings:
                if os.path.exists(dead) or "stages left by PID 4194999, which is gone" not in done.stdout:
                    failures.append(f"{case}: a dead run's stages were left, or not named")
                if not os.path.exists(live):
                    failures.append(f"{case}: a live run's stages were removed")
                shutil.rmtree(live, ignore_errors=True)
            if "CI" in settings and "bound one by one on CI" not in done.stdout:
                failures.append(f"{case}: a set bound one by one on CI didn't fail the run, named")
            if "PLANT_BESIDE" in settings:
                shutil.rmtree(target, ignore_errors=True)
                if "1 laid out, 0 bound one by one" not in done.stdout or os.listdir(
                        os.path.join(parent, ".daedalus-stages")) or os.path.exists(stages_status):
                    failures.append(f"{case}: the files weren't laid out beside the checkout, or "
                                    "were left there")
            if "PLANT_ELSEWHERE" in settings:
                shutil.rmtree(target, ignore_errors=True)
                said = open(stages_status).read() if os.path.exists(stages_status) else ""
                if "0 laid out, 1 bound one by one (no hard links into" not in done.stdout or (
                        "CI" not in settings and said != "1-of-1-bound-one-by-one\n"):
                    failures.append(f"{case}: binding the files one by one wasn't said on the "
                                    "summary line")
            if "PLANT_WRITE" in settings and (open(outside).read() != scripts["outside.txt"] or
                                              os.path.exists(os.path.join(work, "made-by-test"))):
                failures.append(f"{case}: the test's writes reached the checkout")
                with open(outside, "w") as file:
                    file.write(scripts["outside.txt"])
            if "PLANT_FIXTURE" in settings:
                ran = open(log).read() if os.path.exists(log) else ""
                # Held to its key, the build was answered and only the other run reached cargo.
                want = ("planted-fixture" not in ran) if seen is not None else ("planted-fixture" in ran)
                if not want or "planted-other" not in ran:
                    failures.append(f"{case}: cargo ran {ran.splitlines()}")
        failures += stage_plants(work)
        failures += trybuild_plants()
    finally:
        os.chmod(parent, 0o755)
        shutil.rmtree(parent, ignore_errors=True)
    for failure in failures:
        print(f"test-parallel self-test: {failure}", file=sys.stderr)
    if not failures:
        print("test-parallel self-test: an unlisted fixture fails the run and records nothing, a "
              "stale one is removed; a narrow key's test that reads outside it fails, named, and "
              "records nothing, while the same read passes unsandboxed and when the key holds it; "
              "held to its key, a test's build of a fixture is answered as fresh, its files are one "
              "read-only mount laid out once (by hard links, never copies) and gone after the run, its "
              "blanks /dev/null on top, and two packages in one run see their own keys")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(self_test() if sys.argv[1:] == ["--self-test"] else main())

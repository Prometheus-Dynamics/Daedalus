#!/usr/bin/env python3
"""A tools self-test that passed before on exactly these inputs is not run again.

    tools-reuse.py plan STATE        every stamped self-test's key, into STATE
    tools-reuse.py run STATE NAME    `NAME: passed before on these inputs`, or the self-test run,
                                     and on a pass its key recorded
    tools-reuse.py --self-test

A self-test's key (`inputs`), each part collected so:
  - the parts every reuse key holds (scripts/lane-reuse.py `common`): the judges, cargo's config,
    the toolchain, the environment, the programs a step may start (python3 and bash among them)
    and the kernel, and the checkout's files outside the packages;
  - this script, which holds the commands, and the name;
  - the files the self-test reads, by content (`STAMPS`), and the programs it starts that
    `common` doesn't hold;
  - for a self-test that reads the tree (its package graph, which files exist, which packages are
    keyed by the whole tree): every manifest and the lock file by content, the names of the files
    of the checkout but the notes (lane-reuse.py NOTE), and each member's whole-tree verdict
    (lane-reuse.py `leaves`).
A comment in a package's source changes none of these. The keys are taken once, before any
self-test runs (`plan`), so a file changed while one runs is caught on the next run. A failed or
stopped self-test records nothing; a record is read only under its own key and the judges it was
written under. On CI, or with GATE_LANE_REUSE=0, every self-test runs.
"""
import importlib.util
import json
import os
import shutil
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_spec = importlib.util.spec_from_file_location("lane_reuse", os.path.join(ROOT, "scripts", "lane-reuse.py"))
lr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lr)

# name: (command, files it reads, programs common() doesn't hold, reads the tree). The self-tests
# under a second or two run every time (scripts/ci.d/tools.sh).
STAMPS = {
    # Planted sleepers under its own runner, and children held to a CPU quota by a systemd scope:
    # python3 and the kernel are in common(), systemd-run isn't.
    "bounded": (["python3", "-I", "scripts/bounded.py", "--self-test"],
                ["scripts/bounded.py"], ["systemd-run"], False),
    # Copies itself and bounded.py and runs planted tests under bwrap.
    "test-parallel": (["python3", "-I", "scripts/test-parallel.py", "--self-test"],
                      ["scripts/test-parallel.py", "scripts/bounded.py"], ["bwrap"], False),
    # A planted gate around its own gate.sh and ci.sh.
    "gate-self-test": (["scripts/gate-self-test.sh"],
                       ["scripts/gate-self-test.sh", "scripts/gate.sh", "scripts/ci.sh"], [], False),
    # A planted repository with the real budgets workflow, and git.
    "release-check": (["scripts/release-check.sh", "--self-test"],
                      ["scripts/release-check.sh", ".github/workflows/bench.yml"], ["git"], False),
    # Keys the tree's packages with digests planted over the files: the files' contents don't
    # move its result, the package graph and which files exist do.
    "step-reuse": (["python3", "-I", "scripts/step-reuse.py", "--self-test"],
                   ["scripts/step-reuse.py", "scripts/lane-reuse.py"], [], True),
    # Keys a copy of the tree with planted builds (rustc, in common()); picks its packages by
    # their whole-tree verdicts.
    "lane-reuse": (["python3", "-I", "scripts/lane-reuse.py", "--self-test"],
                   ["scripts/lane-reuse.py"], [], True),
    # The budget table's key over the measured packages' closure, by cargo metadata.
    "record": (["python3", "-I", "testing/crates/budget-harness/record.py", "--self-test"],
               ["testing/crates/budget-harness/record.py", "testing/crates/budget-harness/check.py"],
               [], True),
}


def tree(keys):
    """What a self-test that reads the tree depends on: the manifests and the lock file, which
    files exist, and which members are keyed by the whole tree."""
    manifests = [(n, keys.file(n)) for n in keys.listed if n == "Cargo.toml" or n.endswith("/Cargo.toml")]
    verdicts = sorted((n, bool(keys.leaves(n)), bool(keys.sources(n))) for n in keys.by_name)
    # The notes (lane-reuse.py NOTE: docs, .md files, .github/) are read by no build or test outside
    # a whole-tree package, and which packages are whole-tree is keyed below: a new note moves none
    # of these self-tests' results. A self-test that reads a note names it in STAMPS (release-check
    # reads .github/workflows/bench.yml, by that path).
    names = [n for n in keys.listed if not lr.NOTE.search(n)]
    return [("manifests", lr.digest(json.dumps(manifests).encode())),
            ("Cargo.lock", keys.file("Cargo.lock")),
            ("the files' names", lr.digest("\0".join(names).encode())),
            ("whole-tree verdicts", lr.digest(json.dumps(verdicts).encode()))]


def inputs(keys, name, stamps=None):
    command, files, programs, reads_tree = (stamps or STAMPS)[name]
    found = keys.common()
    found.append(("self-test", name))
    found.append(("scripts/tools-reuse.py", lr.file_digest(os.path.abspath(__file__))))
    found.append(("command", " ".join(command)))
    found.extend((n, keys.file(n)) for n in files)
    for program in programs:
        path = shutil.which(program) or ""
        found.append((f"program {program}", lr.file_digest(os.path.realpath(path)) if path else "absent"))
    if reads_tree:
        found.extend(tree(keys))
    return found


def key(keys, name, stamps=None):
    return lr.digest(json.dumps(inputs(keys, name, stamps)).encode())


def off():
    if os.environ.get("CI"):
        return "on CI"
    if os.environ.get("GATE_LANE_REUSE") == "0":
        return "GATE_LANE_REUSE=0"
    return None


def records():
    return os.environ.get("TOOLS_REUSE_RECORDS") or lr.records()


def record_path(name, stamp):
    return os.path.join(records(), f"tools-{name}-{stamp}")


def plan(state, stamps=None):
    keys = lr.Keys()
    with open(state, "w") as file:
        for name in sorted(stamps or STAMPS):
            file.write(f"{name}\t{key(keys, name, stamps)}\n")
    return 0


def run(state, name, stamps=None):
    stamps = stamps or STAMPS
    with open(state) as file:
        stamp = dict(line.rstrip("\n").split("\t") for line in file).get(name)
    if stamp is None or name not in stamps:
        print(f"tools-reuse.py: {name} has no key in {state}", file=sys.stderr)
        return 1
    path = record_path(name, stamp)
    if off() is None:
        try:
            with open(path) as file:
                if file.read() == f"{lr.FORMAT} {lr.judges_hash()}\n":
                    print(f"{name}: passed before on these inputs ({stamp[:12]})")
                    return 0
        except OSError:
            pass
    code = subprocess.run(stamps[name][0], cwd=ROOT).returncode
    if code == 0 and off() is None:
        os.makedirs(records(), exist_ok=True)
        with open(path + ".tmp", "w") as file:
            file.write(f"{lr.FORMAT} {lr.judges_hash()}\n")
        os.replace(path + ".tmp", path)
    return code


def self_test():
    """For each stamped self-test: each of its files, changed, moves its key; a comment in a
    package's source doesn't. A program it starts, and for one that reads the tree a manifest, a new
    file and a package turning whole-tree, move it. A planted self-test that fails records nothing,
    one that passes is reused only under its key and the judges, and on CI it runs."""
    import tempfile

    failures = []
    keys = lr.Keys()
    held = {n for _, files, _, _ in STAMPS.values() for n in files}
    leaf = "backends/cpu-author/src/lib.rs"
    assert leaf in keys.listed and leaf not in held, f"{leaf}: pick another leaf"
    real_digest = lr.file_digest
    overlay = {}
    lr.file_digest = lambda path: overlay.get(os.path.abspath(path)) or real_digest(path)
    keys.reread()
    once = keys.common()
    # The sources' verdicts are read from the files, which no plant changes: scanned once.
    scanned = {}

    def key_with(name, path=None, verdict=None, listed=None):
        overlay.clear()
        if path:
            overlay[os.path.abspath(os.path.join(ROOT, path))] = "changed by the tools-reuse self-test"
        saved = keys.listed
        # Each declared file is in the key by itself, beside any part of common() that holds it.
        keys.digests, keys.cache = {}, {"common": once}
        keys.cache.update(scanned)
        if verdict:
            keys.cache[("leaves", verdict)] = "planted"
        if listed:
            keys.listed = sorted(keys.listed + [listed])
        try:
            return key(keys, name)
        finally:
            scanned.update((k, v) for k, v in keys.cache.items() if k[0] in ("leaves", "sources") and k[1] != verdict)
            keys.listed = saved
            overlay.clear()

    narrow = next(n for n in sorted(keys.by_name) if not keys.leaves(n))
    manifest = next(n for n in keys.listed if n.endswith("/Cargo.toml"))
    for name, (_, files, programs, reads_tree) in STAMPS.items():
        base = key_with(name)
        cases = [(f"its file {n}", dict(path=n), True) for n in files]
        cases += [(f"program {p}", dict(path=os.path.realpath(shutil.which(p) or "/absent")), True)
                  for p in programs]
        cases += [(f"a comment in {leaf}", dict(path=leaf), False),
                  ("a script no self-test runs (scripts/dev/land.sh)", dict(path="scripts/dev/land.sh"), False)]
        cases += [(f"a manifest ({manifest})", dict(path=manifest), reads_tree),
                  ("a new file", dict(listed="backends/cpu-author/src/planted.rs"), reads_tree),
                  ("a new file at the top", dict(listed="planted.txt"), reads_tree),
                  # A note: none reads it but by name (release-check: bench.yml, by its path).
                  ("a new note under .github", dict(listed=".github/planted.yml"), False),
                  ("a new workflow", dict(listed=".github/workflows/planted.yml"), False),
                  ("a new note under docs", dict(listed="docs/planted.md"), False),
                  (f"{narrow} turned whole-tree", dict(verdict=narrow), reads_tree)]
        for label, plant, moves in cases:
            moved = key_with(name, **plant) != base
            if moved != moves:
                failures.append(f"{name}, {label}: the key {'moved' if moved else 'stayed'}")
            else:
                print(f"    {name}, {label}: {'moved' if moved else 'stayed'}")
    lr.file_digest = real_digest
    keys.reread()

    with tempfile.TemporaryDirectory() as work:
        mark = os.path.join(work, "ran")
        planted = {"planted": (["sh", "-c", f'echo >> {mark}; exit "${{PLANT_FAIL:-0}}"'], [], [], False)}
        state = os.path.join(work, "state")
        saved = dict(os.environ)
        os.environ["TOOLS_REUSE_RECORDS"] = os.path.join(work, "records")
        for variable in ("CI", "GATE_LANE_REUSE", "PLANT_FAIL"):
            os.environ.pop(variable, None)
        try:
            plan(state, planted)
            for label, env, want in [("a red self-test", {"PLANT_FAIL": "1"}, (1, 1)),
                                     ("after it, a green one runs", {}, (0, 2)),
                                     ("after that, it is reused", {}, (0, 2)),
                                     ("on CI it runs", {"CI": "1"}, (0, 3)),
                                     ("with a judge changed it runs", {"PLANT_JUDGE": "1"}, (0, 4))]:
                if "PLANT_JUDGE" in env:
                    for name in os.listdir(os.environ["TOOLS_REUSE_RECORDS"]):
                        with open(os.path.join(os.environ["TOOLS_REUSE_RECORDS"], name), "w") as file:
                            file.write(f"{lr.FORMAT} other-judges\n")
                os.environ.update(env)
                code = run(state, "planted", planted)
                for variable in env:
                    os.environ.pop(variable)
                with open(mark) as file:
                    got = (code, len(file.readlines()))
                if got != want:
                    failures.append(f"{label}: exit {got[0]}, ran {got[1]} times (want {want})")
                else:
                    print(f"    {label}: {'failed, nothing recorded' if want[0] else 'passed'}, ran {want[1]} times")
        finally:
            os.environ.clear()
            os.environ.update(saved)
    for failure in failures:
        print(f"tools-reuse self-test: {failure}", file=sys.stderr)
    if failures:
        return 1
    print("tools-reuse self-test passed")
    return 0


def main(argv):
    if len(argv) == 2 and argv[0] == "plan":
        return plan(argv[1])
    if len(argv) == 3 and argv[0] == "run":
        return run(argv[1], argv[2])
    if argv == ["--self-test"]:
        return self_test()
    print(__doc__.split("\n\n")[0], file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

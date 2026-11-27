#!/usr/bin/env python3
"""A gate step outside the test lane that passed before on exactly these inputs is not run again.

    step-reuse.py begin STEP PACKAGE...            `reused KEY` (and the step's Gate-Lanes token
                                                   written), or `run KEY`
    step-reuse.py end STEP KEY ARTIFACTS PACKAGE...  what the step compiled stayed inside its key
                                                   (rustc's dep-info): then the key is recorded
    step-reuse.py --self-test

A step's key (`inputs`), each part collected so:
  - the parts every reuse key holds (scripts/lane-reuse.py `common`): the judges on every step's
    path (scripts/gate.sh, ci.sh, bounded.py, and these scripts), cargo's config, the toolchain
    (`rustc -vV`, `cargo -V`, `cc --version`), the environment a build reads (CARGO_*, RUST*,
    TARGET_*, ...), the programs a step may start, and the checkout's files outside the packages
    (its root, .cargo/, the runners);
  - the step's own script, scripts/ci.d/STEP.sh;
  - the packages it builds: the given ones and everything they depend on, without
    dev-dependencies (cargo metadata): a member's or an in-tree path package's every file, a
    registry or git package's Cargo.lock entry.
The key holds the packages' files, not only what a build reads; what a build did read is checked
after each run against rustc's dep-info and what each build script said it read: a file of the
checkout outside the key fails the step, named, and nothing is recorded. The step calls `end`
last, so a failed or stopped step records nothing; a record is read only under its own key, and
only while the judges are the ones it was written under. On CI, or with GATE_LANE_REUSE=0,
nothing is reused.
"""
import importlib.util
import json
import os
import subprocess
import sys
import tempfile

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_spec = importlib.util.spec_from_file_location("lane_reuse", os.path.join(ROOT, "scripts", "lane-reuse.py"))
lr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lr)


def closure(keys, names, tests=False):
    """The packages a build of `names` builds: theirs and their dependencies', without
    dev-dependencies; with `tests`, what their test binaries build (their own dev-dependencies too)."""
    if tests:
        return set().union(*(keys.built(n) for n in names))
    seen, todo = set(), [keys.by_name[n] for n in names]
    while todo:
        current = todo.pop()
        if current not in seen:
            seen.add(current)
            todo.extend(keys.deps.get(current, []))
    return seen


def home_of(keys, package_id):
    """The package's directory in the checkout, or None for one from a registry or git."""
    package = keys.packages[package_id]
    if package_id in keys.members:
        return keys.dirs[package_id]
    if package["source"] is None:
        home = os.path.relpath(os.path.dirname(package["manifest_path"]), keys.root)
        return None if home.startswith("..") else home + "/"
    return None


def tree_digest(path):
    """Every file under `path`, by its path there and its contents: a directory a step reads from
    outside the checkout (the validation layer's)."""
    found = []
    for here, dirs, files in os.walk(path):
        dirs.sort()
        for name in sorted(files):
            full = os.path.join(here, name)
            found.append((os.path.relpath(full, path), lr.file_digest(full)))
    return lr.digest(json.dumps(found).encode())


def inputs(keys, step, names, tests=False, extra=()):
    """The key's inputs. `extra` is (label, value) pairs the step adds: its own scripts and what
    it reads from outside the checkout."""
    found = keys.common()
    script = f"scripts/ci.d/{step}.sh"
    found.append(("step", step))
    found.append((script, keys.file(script)))
    # This script judges every step it keys; the test lane's judges don't include it.
    found.append(("scripts/step-reuse.py", keys.file("scripts/step-reuse.py")))
    found.extend(extra)
    for package_id in sorted(closure(keys, names, tests), key=lambda i: keys.packages[i]["id"]):
        package = keys.packages[package_id]
        entry = keys.lock.get((package["name"], package["version"]), "absent")
        found.append((f"Cargo.lock {package['name']} {package['version']}", lr.digest(entry.encode())))
        home = home_of(keys, package_id)
        if home:
            found.extend(keys.files(home))
    return found


def key(keys, step, names, tests=False, extra=()):
    return lr.digest(json.dumps(inputs(keys, step, names, tests, extra)).encode())


def visible(keys, names, tests=False):
    """The paths of the checkout the key holds: all a step's builds may compile from and its tests
    may read (scripts/test-parallel.py `sandboxed`)."""
    found = set(keys.root_files()) | {".cargo/"}
    for package_id in closure(keys, names, tests):
        home = home_of(keys, package_id)
        if home:
            found.add(home)
    return found


def outside(keys, names, artifacts, tests=False):
    """Each (package, file) a unit of the step compiled from, or a build script said it read, that
    is a file of the checkout its key doesn't hold. With `tests`, only the units of the packages
    the key holds: the artifacts are a whole workspace's build."""
    held = closure(keys, names, tests)
    target = os.path.realpath(os.environ.get("CARGO_TARGET_DIR") or os.path.join(keys.root, "target"))
    root = os.path.realpath(keys.root) + os.sep
    allowed = visible(keys, names, tests)
    dirs = tuple(a for a in allowed if a.endswith("/"))
    found = []
    for message in artifacts:
        reason = message.get("reason")
        if reason not in ("compiler-artifact", "build-script-executed"):
            continue
        package_id = message["package_id"]
        if tests and package_id not in held:
            continue
        if tests and reason == "compiler-artifact" and not used(keys, names, message):
            continue
        name = keys.packages[package_id]["name"] if package_id in keys.packages else package_id
        if reason == "compiler-artifact":
            if "custom-build" in message["target"]["kind"]:
                continue
            paths = [os.path.join(keys.root, p) for d in lr.dep_info_files(message) for p in lr.dep_info(d)]
        else:
            home = os.path.dirname(keys.packages[package_id]["manifest_path"])
            paths = [os.path.join(home, p) for p in lr.build_script_reads(message)]
        for path in paths:
            real = os.path.realpath(path)
            if not real.startswith(root) or real.startswith(target + os.sep):
                continue
            rel = real[len(root):]
            if rel not in allowed and not rel.startswith(dirs):
                found.append((name, rel))
    return sorted(set(found))


LIBRARY = {"lib", "rlib", "dylib", "cdylib", "staticlib", "proc-macro"}


def used(keys, names, message):
    """Whether a unit of a workspace build is one a run of `names`' tests uses: a library of a
    package they build, or a test unit of one of them. Another target's unit (a dependency's own
    tests, a bin, an example) is built by the workspace, not for these tests."""
    library = bool(LIBRARY & set(message["target"]["kind"])) and not message["profile"]["test"]
    tested = message["package_id"] in {keys.by_name[n] for n in names}
    return library or (tested and message["profile"]["test"])


def off():
    if os.environ.get("CI"):
        return "on CI"
    if os.environ.get("GATE_LANE_REUSE") == "0":
        return "GATE_LANE_REUSE=0"
    return None


def records():
    return os.environ.get("STEP_REUSE_RECORDS") or lr.records()


def record_path(step_key):
    return os.path.join(records(), f"step-{step_key}")


def passed_before(step_key):
    try:
        with open(record_path(step_key)) as file:
            return file.read() == f"{lr.FORMAT} {lr.judges_hash()}\n"
    except OSError:
        return False


def reused(step, step_key):
    """Whether the step passed before under its key; if so its Gate-Lanes token is written."""
    if off() is not None or not passed_before(step_key):
        return False
    state = os.environ.get("GATE_LANE_STATE_DIR")
    if state:
        with open(os.path.join(state, f"{step}.status"), "w") as file:
            file.write(f"passed(reused:{step_key[:12]})\n")
    print(f"{step}: passed before on these inputs ({step_key[:12]}): not run again", file=sys.stderr)
    return True


def record(step_key):
    """The step passed under its key: called last, so a failed or stopped step records nothing."""
    if off() is None:
        # The key the step began under: a file changed while it ran is caught on the next run.
        os.makedirs(records(), exist_ok=True)
        path = record_path(step_key)
        with open(path + ".tmp", "w") as file:
            file.write(f"{lr.FORMAT} {lr.judges_hash()}\n")
        os.replace(path + ".tmp", path)


def begin(step, names):
    step_key = key(lr.Keys(), step, names)
    why = off()
    if reused(step, step_key):
        print(f"reused {step_key}")
    else:
        print(f"run {step_key}")
        if why:
            print(f"{step}: nothing reused ({why})", file=sys.stderr)
    return 0


def end(step, step_key, artifacts_path, names):
    keys = lr.Keys()
    with open(artifacts_path) as file:
        artifacts = [json.loads(line) for line in file if line.startswith("{")]
    found = outside(keys, names, artifacts)
    for name, path in found:
        print(f"step-reuse: {step}: {name} compiles from {path}, which the step's key doesn't hold: "
              f"add the package that holds it to the step's packages", file=sys.stderr)
    if found:
        return 1
    record(step_key)
    return 0


def self_test():
    """Each kind of input changed, each both ways: the key moves for what the step reads and stays
    for what it doesn't; a unit compiled from outside the key is named; a planted step that fails
    records nothing, and one that passes is reused only under its key."""
    failures = []
    step, names = "nostd", ["daedalus-abi"]
    keys = lr.Keys()
    held = closure(keys, names)
    inside = next(n for n in keys.listed if n.startswith(keys.dirs[keys.by_name["daedalus-abi"]])
                  and n.endswith(".rs"))
    apart = next(n for n in keys.listed if n.endswith(".rs") and not any(
        n.startswith(keys.dirs[i]) for i in held if i in keys.members) and "/" in n
        and any(n.startswith(keys.dirs[i]) for i in keys.members))
    real_digest = lr.file_digest
    overlay = {}
    lr.file_digest = lambda path: overlay.get(os.path.abspath(path)) or real_digest(path)

    # common() (the toolchain, the environment, the judges, the files outside the packages) costs
    # most of a key: it is computed once, and again only for the plants that change what it reads.
    keys.reread()
    once = keys.common()

    def key_with(make, path=None, env=None, fresh=False):
        saved = dict(os.environ)
        overlay.clear()
        if path:
            overlay[os.path.abspath(os.path.join(ROOT, path))] = "changed by the step-reuse self-test"
        os.environ.update(env or {})
        try:
            if fresh:
                keys.reread()
            else:
                keys.digests, keys.cache = {}, {"common": once}
            return make()
        finally:
            os.environ.clear()
            os.environ.update(saved)
            overlay.clear()

    def plants(make, cases):
        base = key_with(make)
        for case in cases:
            label, path, env, moves = case[:4]
            # A plant of what common() reads (a judge, a script beside it, the environment) is
            # keyed in full, so a plant that should stay can't stay only because it was cached.
            moved = key_with(make, path, env, fresh=len(case) > 4) != base
            if moved != moves:
                failures.append(f"{label}: the key {'moved' if moved else 'stayed'}")
            else:
                print(f"    {label}: {'moved' if moved else 'stayed'}")

    common = [
        ("a judge on its path (scripts/bounded.py)", "scripts/bounded.py", None, True, "fresh"),
        ("a script on no step's path (scripts/dev/land.sh)", "scripts/dev/land.sh", None, False, "fresh"),
        ("an environment it reads (RUSTFLAGS)", None, {"RUSTFLAGS": "-Cdebug-assertions"}, True, "fresh"),
        ("an environment it doesn't (DAEDALUS_WORK)", None, {"DAEDALUS_WORK": "/elsewhere"}, False, "fresh"),
        # A step with a wait bound is handed a new path for its start mark on every run (bounded.py).
        ("its start mark, new each run (DAEDALUS_BOUND_STARTED)", None,
         {"DAEDALUS_BOUND_STARTED": "/tmp/s-planted.started"}, False, "fresh"),
    ]
    print("  nostd, a step that builds:")
    plants(lambda: key(keys, step, names), [
        (f"a source of the step's closure ({inside})", inside, None, True),
        (f"a source outside it ({apart})", apart, None, False),
        ("the step's script", f"scripts/ci.d/{step}.sh", None, True),
        ("another step's script", "scripts/ci.d/wasm.sh", None, False),
    ] + common)

    # A step that runs tests under a directory outside the checkout: vvl-wgpu, as scripts/vvl-run.py
    # keys it, with a planted layer directory.
    tested = ["daedalus-vk-sys", "daedalus-wgpu"]
    run_held = closure(keys, tested, tests=True)
    wgpu = next(n for n in keys.listed if n.startswith(keys.dirs[keys.by_name["daedalus-wgpu"]])
                and n.endswith(".rs"))
    beyond = next(n for n in keys.listed if n.endswith(".rs") and any(
        n.startswith(keys.dirs[i]) for i in keys.members) and not any(
        n.startswith(keys.dirs[i]) for i in run_held if i in keys.members))
    with tempfile.TemporaryDirectory() as layers:
        layer = os.path.join(layers, "layer")
        os.makedirs(layer)
        for name in ("libVkLayer_khronos_validation.so", "VkLayer_khronos_validation.json"):
            with open(os.path.join(layer, name), "w") as file:
                file.write(name)
        with open(os.path.join(layers, "beside"), "w") as file:
            file.write("beside the layer")
        make = lambda: key(keys, "vvl-wgpu", tested, tests=True, extra=[
            ("scripts/vvl-run.py", keys.file("scripts/vvl-run.py")),
            ("the validation layer", tree_digest(layer))])
        print("  vvl-wgpu, a step that runs tests:")
        plants(make, [
            (f"a source of its tests' closure ({wgpu})", wgpu, None, True),
            (f"a source outside it ({beyond})", beyond, None, False),
            ("the step's script", "scripts/ci.d/vvl-wgpu.sh", None, True),
            ("another step's script", "scripts/ci.d/vvl.sh", None, False),
            ("its runner (scripts/vvl-run.py)", "scripts/vvl-run.py", None, True),
            ("a file of the layer", os.path.join(layer, "VkLayer_khronos_validation.json"), None, True),
            ("a file beside the layer", os.path.join(layers, "beside"), None, False),
        ] + common)
    lr.file_digest = real_digest
    keys.reread()

    with tempfile.TemporaryDirectory() as work:
        # What a unit compiled from, by its dep-info: inside the key passes, outside is named.
        def unit(sources):
            depfile = os.path.join(work, "daedalus_abi-1.d")
            with open(depfile, "w") as file:
                file.write(f"{work}/libdaedalus_abi-1.rmeta: {' '.join(sources)}\n")
            return [{"reason": "compiler-artifact", "package_id": keys.by_name["daedalus-abi"],
                     "target": {"kind": ["lib"], "name": "daedalus_abi"},
                     "filenames": [f"{work}/libdaedalus_abi-1.rmeta"]}]
        for label, sources, want in [("its own sources", [inside], []),
                                     ("a script", [inside, "scripts/check.sh"],
                                      [("daedalus-abi", "scripts/check.sh")])]:
            got = outside(keys, names, unit(sources))
            if got != want:
                failures.append(f"compiling from {label}: named {got}, not {want}")
            else:
                print(f"    compiling from {label}: {'named' if want else 'passed'}")

        # A planted step, run as the gate runs it: red records nothing; green is reused after.
        artifacts = os.path.join(work, "artifacts")
        with open(artifacts, "w") as file:
            file.write(json.dumps(unit([inside])[0]) + "\n")
        plant = os.path.join(work, "step.sh")
        with open(plant, "w") as file:
            file.write(f"""set -euo pipefail
read -r how key < <(python3 -I {__file__} begin {step} {' '.join(names)})
[[ $how == reused ]] && {{ echo reused; exit 0; }}
[[ -z ${{PLANT_FAIL:-}} ]]
python3 -I {__file__} end {step} "$key" {artifacts} {' '.join(names)}
echo ran
""")
        env = {k: v for k, v in os.environ.items() if k not in ("CI", "GATE_LANE_REUSE", "GATE_LANE_STATE_DIR")}
        env["STEP_REUSE_RECORDS"] = os.path.join(work, "records")
        for label, extra, want in [("a red step", {"PLANT_FAIL": "1"}, (1, "")),
                                   ("after it, a green one runs", {}, (0, "ran")),
                                   ("after that, it is reused", {}, (0, "reused")),
                                   ("on CI it runs", {"CI": "1"}, (0, "ran")),
                                   ("with a judge changed it runs", {"PLANT_JUDGE": "1"}, (0, "ran"))]:
            if "PLANT_JUDGE" in extra:
                # A record written under other judges is not read.
                for name in os.listdir(env["STEP_REUSE_RECORDS"]):
                    with open(os.path.join(env["STEP_REUSE_RECORDS"], name), "w") as file:
                        file.write(f"{lr.FORMAT} other-judges\n")
            done = subprocess.run(["bash", plant], env={**env, **extra}, capture_output=True, text=True)
            got = (done.returncode, done.stdout.strip())
            if got != want:
                failures.append(f"{label}: exit {got[0]}, said {got[1]!r} (want {want})\n{done.stderr}")
            else:
                print(f"    {label}: {want[1] or 'failed, nothing recorded'}")
    for failure in failures:
        print(f"step-reuse self-test: {failure}", file=sys.stderr)
    if failures:
        return 1
    print("step-reuse self-test passed")
    return 0


def main(argv):
    if len(argv) >= 3 and argv[0] == "begin":
        return begin(argv[1], argv[2:])
    if len(argv) >= 5 and argv[0] == "end":
        return end(argv[1], argv[2], argv[3], argv[4:])
    if argv == ["--self-test"]:
        return self_test()
    print(__doc__.split("\n\n")[0], file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

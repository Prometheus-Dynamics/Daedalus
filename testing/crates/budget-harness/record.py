#!/usr/bin/env python3
"""The budget table's measurements, kept by what they were measured from.

    record.py --self-test   every traced input, changed, changes the key; a leaf outside doesn't

A budget measures the binaries that ship: the release harness (fat LTO), the firmware, and the
plugin fixture it loads. They are the same bytes from the same inputs, whatever the checkout's
path, so a measurement made once holds until an input changes. The key is the content of:

- every file of every package the measured binaries are built from, by any dependency that is
  not a dev-dependency (`cargo metadata`), and the fixture's own packages;
- the lock files and manifests at the roots, and `.cargo/config.toml`;
- this file and `check.py`, which say how the measuring is done;
- the toolchain (`rustc -vV`, `cargo -V`), valgrind, readelf and glibc, and the CPU (its model and
  flags: glibc picks its string functions by them, and callgrind counts what runs);
- every `CARGO_*` and `RUST*` variable of the environment, but the build directory, the job count
  and whether it is incremental, which the measured builds set themselves.

A file of a package outside that closure, or one in no package, changes nothing measured.
"""
import functools
import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
FIXTURE = os.path.join("testing", "fixtures", "dylib", "Cargo.toml")
MEASURED = ("daedalus-budget-harness", "daedalus-chip-blink")
METHOD = ("testing/crates/budget-harness/check.py", "testing/crates/budget-harness/record.py")
ROOT_FILES = ("Cargo.toml", "Cargo.lock", ".cargo/config.toml", "rust-toolchain", "rust-toolchain.toml",
              "testing/fixtures/dylib/Cargo.lock")
SET_BY_THE_BUILD = {"CARGO_TARGET_DIR", "CARGO_BUILD_JOBS", "CARGO_INCREMENTAL", "DAEDALUS_TARGET_SEED"}
KEPT = 8


@functools.cache
def closure_dirs(manifest, roots):
    """The directories, relative to ROOT, of the path packages `roots` are built from."""
    args = ["cargo", "metadata", "--format-version", "1", "--locked"]
    if manifest:
        args += ["--manifest-path", os.path.join(ROOT, manifest)]
    meta = json.loads(subprocess.run(args, cwd=ROOT, check=True, capture_output=True, text=True).stdout)
    by_id = {p["id"]: p for p in meta["packages"]}
    nodes = {n["id"]: n for n in meta["resolve"]["nodes"]}
    todo = [p["id"] for p in meta["packages"] if p["name"] in roots]
    seen = set()
    while todo:
        pid = todo.pop()
        if pid in seen:
            continue
        seen.add(pid)
        for dep in nodes[pid]["deps"]:
            if any(k["kind"] in (None, "build") for k in dep["dep_kinds"]):
                todo.append(dep["pkg"])
    return sorted(os.path.relpath(os.path.dirname(by_id[pid]["manifest_path"]), ROOT)
                  for pid in seen if by_id[pid]["source"] is None)


def fixture_files():
    """What the plugin fixture is built from: its packages' files, and the root manifest and lock
    they take their workspace fields from."""
    dirs = closure_dirs(FIXTURE, ("daedalus-dylib-fixture",))
    return files_of(dirs, {}) + [p for p in ("Cargo.toml", "Cargo.lock") if os.path.exists(os.path.join(ROOT, p))]


def tool(*args):
    try:
        done = subprocess.run(args, capture_output=True, text=True)
        return (done.stdout + done.stderr).strip().splitlines()[0] if done.returncode == 0 else "none"
    except (OSError, IndexError):
        return "none"


def cpu():
    try:
        with open("/proc/cpuinfo") as file:
            lines = file.read().splitlines()
    except OSError:
        return platform.processor()
    keep = [l for l in lines if l.split(":")[0].strip() in ("model name", "flags")]
    return "\n".join(dict.fromkeys(keep))


def facts(env):
    return {
        "rustc": subprocess.run(["rustc", "-vV"], capture_output=True, text=True, check=True).stdout,
        "cargo": tool("cargo", "-V"),
        "valgrind": tool("valgrind", "--version"),
        "readelf": tool("readelf", "--version"),
        "glibc": tool("ldd", "--version"),
        "cpu": cpu(),
        # The plugin fixture is built under it (check.py), and its path is in the library.
        "tmp": tempfile.gettempdir(),
        "env": "\n".join(f"{k}={v}" for k, v in sorted(env.items())
                         if (k.startswith("CARGO_") or k.startswith("RUST")) and k not in SET_BY_THE_BUILD),
    }


def files_of(dirs, overlay):
    """Every file git sees under `dirs` (tracked, or new and not ignored), and the overlay's."""
    listed = subprocess.run(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard", "--", *dirs],
                            cwd=ROOT, check=True, capture_output=True, text=True).stdout.split("\0")
    inside = lambda path: any(path == d or path.startswith(d.rstrip("/") + "/") for d in dirs)
    return sorted({p for p in listed if p} | {p for p in overlay if inside(p)})


def key(overlay=None, env=None, said=None):
    """The key of the measurements this checkout would make. `overlay` stands in for files
    ({path: bytes}), `env` for the environment, `said` for the facts: the self-test plants in them."""
    overlay = overlay or {}
    dirs = sorted(set(closure_dirs(None, MEASURED)) | set(closure_dirs(FIXTURE, ("daedalus-dylib-fixture",))))
    digest = hashlib.sha256()

    def add(path):
        if path in overlay:
            data = overlay[path]
        else:
            try:
                with open(os.path.join(ROOT, path), "rb") as file:
                    data = file.read()
            except (FileNotFoundError, IsADirectoryError):
                return
        digest.update(f"{path}\0{len(data)}\0".encode())
        digest.update(data)

    for path in files_of(dirs, overlay):
        add(path)
    for path in (*ROOT_FILES, *METHOD):
        add(path)
    for name, value in sorted((said or facts(os.environ if env is None else env)).items()):
        digest.update(f"{name}\0{value}\0".encode())
    return digest.hexdigest()


def load(path, wanted):
    try:
        with open(path) as file:
            return json.load(file).get(wanted)
    except (OSError, ValueError, AttributeError):
        return None


def save(path, wanted, values):
    try:
        with open(path) as file:
            kept = json.load(file)
    except (OSError, ValueError):
        kept = {}
    kept.pop(wanted, None)
    kept[wanted] = values
    kept = dict(list(kept.items())[-KEPT:])
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path + ".new", "w") as file:
        json.dump(kept, file, indent=0, sort_keys=False)
    os.replace(path + ".new", path)


def self_test():
    said = facts(os.environ)
    base = key(said=said)
    harness_dirs = closure_dirs(None, MEASURED)
    fixture_dirs = closure_dirs(FIXTURE, ("daedalus-dylib-fixture",))
    inside = sorted(set(harness_dirs) | set(fixture_dirs))
    meta = json.loads(subprocess.run(["cargo", "metadata", "--format-version", "1", "--no-deps", "--locked"],
                                     cwd=ROOT, check=True, capture_output=True, text=True).stdout)
    leaves = sorted(os.path.relpath(os.path.dirname(p["manifest_path"]), ROOT) for p in meta["packages"])
    leaf = next(d for d in leaves if d not in inside and not any(d.startswith(i + "/") for i in inside))
    some = lambda d: next(p for p in files_of([d], {}) if p.endswith(".rs"))
    core = next(d for d in harness_dirs if d.startswith("core/"))
    read = lambda path: open(os.path.join(ROOT, path), "rb").read()
    must_change = {
        f"a source of {core}": dict(overlay={some(core): read(some(core)) + b"\n"}),
        f"a new file in {core}": dict(overlay={f"{core}/src/planted.rs": b"fn planted() {}\n"}),
        "the firmware's source": dict(overlay={some("examples/chip_blink"): read(some("examples/chip_blink")) + b" "}),
        "the plugin fixture": dict(overlay={some("testing/fixtures/dylib"): read(some("testing/fixtures/dylib")) + b" "}),
        "Cargo.lock": dict(overlay={"Cargo.lock": read("Cargo.lock") + b"\n"}),
        "the release profile": dict(overlay={"Cargo.toml": read("Cargo.toml").replace(b'lto = "fat"', b'lto = "thin"', 1)}),
        "the measuring": dict(overlay={METHOD[0]: read(METHOD[0]) + b"\n"}),
        "the toolchain": dict(said={**said, "rustc": said["rustc"] + "planted"}),
        "valgrind": dict(said={**said, "valgrind": "valgrind-0.0"}),
        "the CPU": dict(said={**said, "cpu": said["cpu"] + " planted"}),
        "RUSTFLAGS": dict(said=facts({**os.environ, "RUSTFLAGS": "-C opt-level=1"})),
    }
    must_stay = {
        f"a source of {leaf}, outside": dict(overlay={some(leaf): read(some(leaf)) + b"\n"}),
        "a doc": dict(overlay={"docs/testing.md": read("docs/testing.md") + b"\n"}),
        "the build directory and job count": dict(said=facts({**os.environ, "CARGO_TARGET_DIR": "/elsewhere",
                                                              "CARGO_BUILD_JOBS": "3"})),
    }
    failed = 0
    for name, plant in must_change.items():
        plant.setdefault("said", said)
        same = key(**plant) == base
        failed += same
        print(f"  {'FAIL' if same else 'ok  '} {name} changes the key")
    for name, plant in must_stay.items():
        plant.setdefault("said", said)
        same = key(**plant) == base
        failed += not same
        print(f"  {'ok  ' if same else 'FAIL'} {name} leaves it")
    print(f"record: {len(harness_dirs)} packages measured, {len(fixture_dirs)} in the fixture;"
          f" {'every planted change was seen' if not failed else f'{failed} wrong'}")
    return 1 if failed else 0


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        sys.exit(self_test())
    print(key())

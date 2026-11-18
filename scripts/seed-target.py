#!/usr/bin/env python3
"""A seed of dependency builds, to start a new checkout's build directory from.

    seed-target.py save SEED   SEED becomes this checkout's build directory less the workspace's
                               own builds: the dependencies only
    seed-target.py use SEED    a build directory that doesn't exist yet starts as a copy of SEED
    seed-target.py --self-test

The workspace's own builds never go in a seed. Cargo would take them as fresh in another checkout,
and they carry the paths of the checkout that built them (`env!("CARGO_MANIFEST_DIR")`). The
dependencies' builds carry no checkout's paths, and what their build scripts wrote names the build
directory they ran in, which cargo maps onto the directory it finds them in. Cargo still checks
every seeded build against the source, so an old seed only means more to build.

Some builds have a build directory of their own inside this one: trybuild's (`tests/trybuild`, one
project per crate named `<crate>-tests`) and the fixtures tests build (`tmp/<name>`). Their
dependency builds go in the seed the same way. The rest of `tests` and `tmp` is ours, and so are
trybuild's projects, whose manifests name this checkout's paths.

The build directory is CARGO_TARGET_DIR, or `target` in this checkout. Copies share their blocks
where the file system can (btrfs, XFS); a seed is replaced whole, under a lock beside it.
"""
import fcntl
import json
import os
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# Whole: what is built from the workspace, or belongs to one checkout.
ONLY_OURS = ["doc", "test-parallel-times.json", ".daedalus-checkout"]
# Ours, except the build directories inside them.
HOLDING_BUILDS = ["tests", "tmp"]


def target():
    return os.path.abspath(os.environ.get("CARGO_TARGET_DIR") or os.path.join(ROOT, "target"))


def members():
    meta = json.loads(subprocess.run(
        ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"],
        cwd=ROOT, check=True, capture_output=True, text=True).stdout)
    return {p["name"] for p in meta["packages"]}


def unit_of(name):
    """The unit hash in a build file's name (`libname-HASH.rlib`, `name-HASH`), or None."""
    stem = (name[3:] if name.startswith("lib") else name).split(".", 1)[0]
    return stem.rsplit("-", 1)[1] if "-" in stem else None


def remove(path):
    if os.path.isdir(path) and not os.path.islink(path):
        shutil.rmtree(path)
    elif os.path.lexists(path):
        os.remove(path)


def strip(directory, ours):
    """Removes from a build directory every build of the packages named in `ours`."""
    for name in ONLY_OURS:
        remove(os.path.join(directory, name))
    for holder in HOLDING_BUILDS:
        holder = os.path.join(directory, holder)
        if not os.path.isdir(holder):
            continue
        for name in os.listdir(holder):
            inner = os.path.join(holder, name)
            # Cargo writes `.rustc_info.json` at the top of every build directory.
            if not os.path.exists(os.path.join(inner, ".rustc_info.json")):
                remove(inner)
                continue
            for project in os.listdir(inner):
                # A trybuild project: its manifest names the checkout's paths.
                if os.path.exists(os.path.join(inner, project, "Cargo.toml")):
                    remove(os.path.join(inner, project))
    profiles = []
    for root, dirs, _ in os.walk(directory):
        if ".fingerprint" in dirs:
            profiles.append(root)
            dirs[:] = []
    for profile in profiles:
        fingerprints = os.path.join(profile, ".fingerprint")
        units = set()
        for name in os.listdir(fingerprints):
            package, _, unit = name.rpartition("-")
            # `<crate>-tests` is the project trybuild generates for one of ours.
            if package in ours or package.removesuffix("-tests") in ours:
                units.add(unit)
                remove(os.path.join(fingerprints, name))
        for sub in ("deps", "build", "examples"):
            path = os.path.join(profile, sub)
            if not os.path.isdir(path):
                continue
            for name in os.listdir(path):
                unit = unit_of(name)
                # An example's file with no hash is cargo's copy of one of ours.
                if unit in units or (sub == "examples" and unit is None):
                    remove(os.path.join(path, name))
        for name in os.listdir(profile):
            path = os.path.join(profile, name)
            # The incremental state is only ever ours; a file at the top is cargo's copy of one
            # of our binaries or libraries.
            if name == "incremental" or (os.path.isfile(path) and name != ".cargo-lock"):
                remove(path)


def copy(source, destination):
    subprocess.run(["cp", "-a", "--reflink=auto", source, destination], check=True)


def save(seed):
    seed = os.path.abspath(seed)
    source = target()
    if not os.path.isdir(source):
        print(f"seed: no build directory at {source}", file=sys.stderr)
        return 1
    ours = members()
    with open(seed + ".lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        fresh = tempfile.mkdtemp(prefix=os.path.basename(seed) + ".", dir=os.path.dirname(seed))
        os.rmdir(fresh)
        copy(source, fresh)
        strip(fresh, ours)
        old = seed + ".old"
        remove(old)
        if os.path.exists(seed):
            os.rename(seed, old)
        os.rename(fresh, seed)
        remove(old)
    print(f"seed: {seed} holds the dependency builds of {source}")
    return 0


def use(seed):
    seed = os.path.abspath(seed)
    destination = target()
    if os.path.exists(destination) or not os.path.isdir(seed):
        return 0
    with open(seed + ".lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_SH)
        parent = os.path.dirname(destination)
        os.makedirs(parent, exist_ok=True)
        fresh = tempfile.mkdtemp(prefix=os.path.basename(destination) + ".", dir=parent)
        os.rmdir(fresh)
        copy(seed, fresh)
    try:
        # Whoever renames first wins; a lane that loses leaves the other's directory.
        os.rename(fresh, destination)
        print(f"seed: {destination} starts from {seed}")
    except OSError:
        remove(fresh)
    return 0


def self_test():
    failed = 0
    with tempfile.TemporaryDirectory() as top:
        def put(*parts):
            path = os.path.join(top, *parts)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            open(path, "w").close()
            return path
        keep = [put("debug/.fingerprint/serde-aaa/lib-serde"), put("debug/deps/libserde-aaa.rlib"),
                put("debug/build/ring-bbb/output"), put("debug/.fingerprint/ring-bbb/x"),
                put("thumbv6m-none-eabi/chip/deps/libdefmt-ccc.rlib"),
                put("thumbv6m-none-eabi/chip/.fingerprint/defmt-ccc/x"),
                put("debug/examples/dep_example-ddd"), put("debug/.cargo-lock")]
        gone = [put("debug/.fingerprint/daedalus-wire-eee/lib"), put("debug/deps/libdaedalus_wire-eee.rlib"),
                put("debug/deps/ui-eee"), put("debug/deps/daedalus_wire-eee.d"),
                put("debug/build/daedalus-wire-eee/output"), put("debug/examples/generate.d"),
                put("debug/incremental/daedalus_wire-x/s"), put("debug/daedalusd"),
                put("thumbv6m-none-eabi/chip/.fingerprint/daedalus-wire-fff/x"),
                put("thumbv6m-none-eabi/chip/deps/libdaedalus_wire-fff.rlib"),
                put("tmp/x"), put("doc/x"), put(".daedalus-checkout"),
                put("test-parallel-times.json")]
        # The build directories inside ours: a dependency's builds stay, a project and its builds go.
        tb = "tests/trybuild/"
        fx = "tmp/dylib-fixture/"
        keep += [put(tb + ".rustc_info.json"), put(fx + ".rustc_info.json"),
                 put(fx + "debug/.fingerprint/serde-kkk/x"), put(fx + "debug/deps/libserde-kkk.rlib"),
                 put(tb + "debug/.fingerprint/syn-ggg/lib-syn"), put(tb + "debug/deps/libsyn-ggg.rlib"),
                 put(tb + "x86_64-unknown-linux-gnu/debug/.fingerprint/serde-hhh/x"),
                 put(tb + "x86_64-unknown-linux-gnu/debug/deps/libserde-hhh.rlib")]
        gone += [put(tb + "daedalus-wire/Cargo.toml"), put(tb + "daedalus-wire/main.rs"),
                 put(tb + "x86_64-unknown-linux-gnu/debug/.fingerprint/daedalus-wire-tests-iii/x"),
                 put(tb + "x86_64-unknown-linux-gnu/debug/deps/daedalus_wire_tests-iii"),
                 put(tb + "x86_64-unknown-linux-gnu/debug/daedalus-wire-tests"),
                 put(tb + "debug/.fingerprint/daedalus-wire-jjj/x"),
                 put(tb + "debug/deps/libdaedalus_wire-jjj.rlib"),
                 put(fx + "debug/.fingerprint/daedalus-wire-lll/x"), put(fx + "debug/libdaedalus_wire.so"),
                 put("tmp/daedalusd-run/daedalus.sock"), put("tests/other/x")]
        # The unit `eee` of ours shares its directory with `aaa`, a dependency's.
        strip(top, {"daedalus-wire"})
        for path in keep:
            if not os.path.exists(path):
                print(f"seed self-test: a dependency's {os.path.relpath(path, top)} was removed", file=sys.stderr)
                failed = 1
        for path in gone:
            if os.path.exists(path):
                print(f"seed self-test: our {os.path.relpath(path, top)} was kept", file=sys.stderr)
                failed = 1
    if not failed:
        print("seed self-test: only the dependencies' builds were kept")
    return failed


def main(args):
    if args == ["--self-test"]:
        return self_test()
    if len(args) == 2 and args[0] == "save":
        return save(args[1])
    if len(args) == 2 and args[0] == "use":
        return use(args[1])
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

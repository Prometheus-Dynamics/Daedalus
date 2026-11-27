#!/usr/bin/env python3
"""Runs packages' test binaries on lavapipe under the pinned Khronos validation layer.

    vvl-run.py --step NAME [--threads N] PACKAGE[:lib]...

The layer is the one scripts/vvl-build.sh builds into $DAEDALUS_VVL_DIR, refused unless its hash
is scripts/vvl.pin's. The binaries are the ones the `test` step built (scripts/test-parallel.py's
own `cargo test --locked --workspace --all-targets`, a no-op once that step has run); building a
package on its own would unify features differently and build wgpu again. `:lib` takes only the
package's unit tests. Each binary runs as test-parallel.py runs it.

First the planted call (vk-sys, DAEDALUS_PLANT_VALIDATION_ERROR=1), which the layer has to name,
so a layer that isn't loaded can't pass. Then every binary, and any validation line fails.

With no layer it prints "NAME: not run" and why, and fails: a gate that couldn't validate hasn't
passed. GitHub's runners have no pinned layer, so there (CI=true) the same line is printed and the
step passes; the lander's gate is the merge bar.

A step that passed before on exactly these inputs is not run again (scripts/step-reuse.py): its key
holds what the packages' test binaries build (and the planted call's, vk-sys's), this script, the
pin, the step's own script, and every file of the layer. Each binary then runs under bwrap seeing
only the paths of the checkout that key holds, and what each unit compiled from must be inside
it (rustc's dep-info): a read outside fails the step, named, and nothing is recorded.
"""
import argparse
import hashlib
import json
import importlib.util
import os
import re
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)
_spec = importlib.util.spec_from_file_location("test_parallel", os.path.join(HERE, "test-parallel.py"))
tp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tp)
_spec = importlib.util.spec_from_file_location("step_reuse", os.path.join(HERE, "step-reuse.py"))
sr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sr)

MESSAGES = re.compile(r"Validation (Error|Warning|Performance Warning)|VUID-|SYNC-HAZARD|UNASSIGNED-")
PLANTED = "VUID-VkBufferCreateInfo-size-00912"
WORKSPACE = ["--locked", "--workspace", "--all-targets"]


def pin():
    values = {}
    with open(os.path.join(HERE, "vvl.pin")) as file:
        for line in file:
            key, sep, value = line.strip().partition("=")
            if sep and not key.startswith("#"):
                values[key] = value.strip('"')
    return values


def not_run(step, why):
    print(f"{step}: not run: {why}", file=sys.stderr)
    if os.environ.get("CI") == "true":
        print(f"{step}: on a CI runner with no pinned layer; the lander's gate runs this step")
        sys.exit(0)
    sys.exit(1)


def fail(step, why, output=None):
    if output:
        with open(output, errors="replace") as file:
            sys.stderr.write(file.read()[-20000:])
    print(f"{step}: {why}", file=sys.stderr)
    sys.exit(1)


def layer_env(step, pinned):
    layer = os.environ.get("DAEDALUS_VVL_DIR", "")
    build = "scripts/vvl-build.sh --arch x86_64 --out DIR builds the layer"
    if not layer:
        not_run(step, f"DAEDALUS_VVL_DIR is not set ({build})")
    lib = os.path.join(layer, "libVkLayer_khronos_validation.so")
    if not (os.path.isfile(lib) and os.path.isfile(os.path.join(layer, "VkLayer_khronos_validation.json"))):
        not_run(step, f"no layer in {layer} ({build})")
    with open(lib, "rb") as file:
        have = hashlib.sha256(file.read()).hexdigest()
    want = pinned.get("VVL_SHA256_X86_64", "")
    if have != want:
        fail(step, f"the layer in {layer} has sha256 {have}, not the pinned {want or '(none)'}")
    icd = subprocess.run([os.path.join(HERE, "lavapipe-icd.sh")], capture_output=True, text=True)
    if icd.returncode != 0 or not icd.stdout.strip():
        not_run(step, "no lavapipe driver is installed (mesa-vulkan-drivers)")
    icd = icd.stdout.strip()
    return {
        "VK_DRIVER_FILES": icd, "VK_ICD_FILENAMES": icd, "VK_LAYER_PATH": layer,
        "VK_INSTANCE_LAYERS": "VK_LAYER_KHRONOS_validation", "VK_LOADER_LAYERS_DISABLE": "~implicit~",
        "VK_KHRONOS_VALIDATION_DEBUG_ACTION": "VK_DBG_LAYER_ACTION_LOG_MSG",
        "VK_KHRONOS_VALIDATION_REPORT_FLAGS": "error,warn",
        "DAEDALUS_GPU_ADAPTERS": "software", "DAEDALUS_GPU_REQUIRED": "1",
    }


def wanted(found, spec):
    package, _, only = spec.partition(":")
    picked = []
    for message in found:
        name, _ = tp.package_of(message["package_id"], message["manifest_path"])
        if name != package:
            continue
        if only == "lib" and "lib" not in message["target"]["kind"]:
            continue
        picked.append(message)
    return picked


def run(message, args, extra, log, output, sysroot_lib, held=None):
    """Runs one binary; with `held` (package name to the paths of the checkout its key holds), it
    sees only those."""
    env = tp.package_env(message, sysroot_lib)
    env.update(extra)
    env["VK_KHRONOS_VALIDATION_LOG_FILENAME"] = log
    name, _ = tp.package_of(message["package_id"], message["manifest_path"])
    prefix = tp.sandboxed(held[name]) if held is not None else []
    with open(output, "a") as out:
        out.write(f"== {tp.label(message)}\n")
        out.flush()
        return subprocess.run([*prefix, message["executable"], *args], cwd=os.path.dirname(message["manifest_path"]),
                              env=env, stdout=out, stderr=subprocess.STDOUT).returncode


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", required=True)
    parser.add_argument("--threads", type=int)
    parser.add_argument("packages", nargs="+")
    opts = parser.parse_args()
    step = opts.step
    pinned = pin()
    extra = layer_env(step, pinned)

    # The key: what the binaries build and the planted call's, and what the step runs them with.
    keys = sr.lr.Keys()
    names = sorted({spec.partition(":")[0] for spec in opts.packages} | {"daedalus-vk-sys"})
    step_key = sr.key(keys, step, names, tests=True, extra=[
        ("scripts/vvl-run.py", keys.file("scripts/vvl-run.py")),
        ("scripts/vvl.pin", keys.file("scripts/vvl.pin")),
        ("the validation layer", sr.tree_digest(extra["VK_LAYER_PATH"]))])
    reuse = sr.off() is None and shutil.which("bwrap") is not None
    if reuse and sr.reused(step, step_key):
        return 0
    held = {n: sorted(sr.visible(keys, [n], tests=True)) for n in names} if reuse else None
    artifacts = tempfile.NamedTemporaryFile(prefix="vvl-artifacts.", delete=False).name
    os.environ["DAEDALUS_TEST_ARTIFACTS"] = artifacts

    # This waits for the `test` step's build, when that step is still building. The step's bound
    # counts from here: see scripts/bounded.py.
    try:
        found = tp.binaries(WORKSPACE)
        with open(artifacts) as file:
            built = [json.loads(line) for line in file if line.startswith("{")]
    finally:
        os.remove(artifacts)
    started = os.environ.get("DAEDALUS_BOUND_STARTED")
    if started:
        open(started, "w").close()
    rustc = lambda *words: subprocess.run(["rustc", *words], capture_output=True, text=True,
                                          check=True).stdout.strip()
    host = next(l.split()[1] for l in rustc("-vV").splitlines() if l.startswith("host:"))
    sysroot_lib = os.path.join(rustc("--print", "sysroot"), "lib", "rustlib", host, "lib")

    with tempfile.TemporaryDirectory() as work:
        # The planted call: the layer is loaded and its messages reach the log.
        planted = wanted(found, "daedalus-vk-sys:lib")
        if len(planted) != 1:
            fail(step, f"no single vk-sys test binary was built ({len(planted)} found)")
        log, out = os.path.join(work, "planted.log"), os.path.join(work, "planted.out")
        if run(planted[0], ["planted"], {**extra, "DAEDALUS_PLANT_VALIDATION_ERROR": "1"}, log, out,
               sysroot_lib, held) != 0:
            fail(step, "the planted run failed", out)
        if not os.path.exists(log) or PLANTED not in open(log, errors="replace").read():
            fail(step, f"the layer didn't name the planted call ({PLANTED})")

        args = [f"--test-threads={opts.threads}"] if opts.threads else []
        binaries = [m for spec in opts.packages for m in wanted(found, spec)]
        for spec in opts.packages:
            if not wanted(found, spec):
                fail(step, f"no test binary of {spec} was built")
        log, out = os.path.join(work, "tests.log"), os.path.join(work, "tests.out")
        for message in binaries:
            if run(message, args, extra, log, out, sysroot_lib, held) != 0:
                fail(step, f"{tp.label(message)} failed under the layer" + (
                     ", seeing only the paths of the checkout its key holds (scripts/step-reuse.py): "
                     "a test that reads another fails here" if held is not None else ""), out)
        lines = []
        for path in (log, out):
            if os.path.exists(path):
                with open(path, errors="replace") as file:
                    lines += [line.rstrip("\n") for line in file if MESSAGES.search(line)]
        if lines:
            print("\n".join(lines[:20]), file=sys.stderr)
            fail(step, f"the layer reported {len(lines)} validation lines")
        passed = 0
        with open(out, errors="replace") as file:
            for line in file:
                if line.startswith("test result: ok."):
                    passed += int(line.split()[3])
        if passed == 0:
            fail(step, "no test ran")
    if reuse:
        for name, path in sr.outside(keys, names, built, tests=True):
            fail(step, f"{name} compiles from {path}, which the step's key doesn't hold")
        sr.record(step_key)
    print(f"{step}: {passed} tests of {', '.join(opts.packages)} ({len(binaries)} binaries) under "
          f"{pinned.get('VVL_TAG')}, the planted call named, no validation line")
    return 0


if __name__ == "__main__":
    sys.exit(main())

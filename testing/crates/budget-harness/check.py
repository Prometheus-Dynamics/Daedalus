#!/usr/bin/env python3
"""Measures the rows of testing/budgets/budgets.toml and fails on one above its limit.

    check.py             measure and check every row this machine can, then the planted rows
    check.py --self-test only the planted rows: each must fail
    check.py --fresh     measure again, whatever was measured before (or BUDGETS_FRESH=1)

What is measured is kept in the build directory under a key of everything it was measured from
(record.py), and used again while the key is the same: the binaries are the same bytes then, so
are their counts. The limits are checked every time.

A row whose tool is missing is reported as not run. With BUDGETS_REQUIRE_ALL=1, as in the
budgets workflow, a row that could not be run is a failure.
"""
import os
import re
import shutil
import subprocess
import sys
import tempfile
import importlib.util
import tomllib
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
TARGET = os.environ.get("CARGO_TARGET_DIR") or os.path.join(ROOT, "target")
HARNESS = os.path.join(TARGET, "release", "daedalus-budget-harness")
# The fixture's dependencies are outside its workspace, so cargo hashes their absolute paths into
# its symbols: built in the checkout, the library would differ by checkout. The budgets build it from
# a copy at a path named by the key instead, the same for every checkout with these inputs.
FIXTURE_DIR = os.path.join(TARGET, "tmp", "dylib-fixture-budgets")
COPIES = os.path.join(tempfile.gettempdir(), "daedalus-budgets")
RECORD = os.path.join(TARGET, "budgets-record.json")
_spec = importlib.util.spec_from_file_location("record", os.path.join(os.path.dirname(__file__), "record.py"))
record = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(record)
REQUIRE_ALL = os.environ.get("BUDGETS_REQUIRE_ALL") == "1"
LOW, HIGH = 1000, 3000


# Instruction counts and firmware sizes are the code's: never an incremental build, whatever the
# caller's environment says.
CODE_ENV = {**os.environ, "CARGO_INCREMENTAL": "0"}


def run(args, **kwargs):
    return subprocess.run(args, cwd=ROOT, check=True, text=True, **kwargs)


def fixture_copy(key):
    """The fixture's sources at COPIES/<key>: made once, whole, by a rename, and never changed."""
    copy = os.path.join(COPIES, key[:16])
    if not os.path.isdir(copy):
        making = f"{copy}.{os.getpid()}"
        for path in record.fixture_files():
            os.makedirs(os.path.dirname(os.path.join(making, path)), exist_ok=True)
            shutil.copy2(os.path.join(ROOT, path), os.path.join(making, path))
        try:
            os.rename(making, copy)
        except OSError:  # another checkout with the same inputs made it first
            shutil.rmtree(making, ignore_errors=True)
    # The newest few are kept.
    for old in sorted(os.scandir(COPIES), key=lambda e: e.stat().st_mtime, reverse=True)[8:]:
        if old.is_dir() and "." not in old.name:
            shutil.rmtree(old.path, ignore_errors=True)
    return copy


def build(key):
    run(["cargo", "build", "-q", "--release", "-p", "daedalus-budget-harness"], env=CODE_ENV)
    manifest = os.path.join(fixture_copy(key), "testing", "fixtures", "dylib", "Cargo.toml")
    run(["cargo", "build", "-q", "--manifest-path", manifest, "--target-dir", FIXTURE_DIR])
    return os.path.join(FIXTURE_DIR, "debug", "libdaedalus_dylib_fixture.so")


def collected(command, n, fixture):
    """The instructions callgrind counts for one run of the harness, `{n}` iterations."""
    words = [w.replace("{n}", str(n)).replace("{fixture}", fixture) for w in command.split()]
    out = subprocess.run(
        ["valgrind", "--tool=callgrind", "--callgrind-out-file=/dev/null", HARNESS, *words],
        cwd=ROOT, text=True, capture_output=True,
    )
    found = re.search(r"Collected : (\d+)", out.stderr)
    if out.returncode != 0 or not found:
        raise RuntimeError(f"callgrind failed on `{command}`:\n{out.stderr[-2000:]}")
    return int(found.group(1))


def instructions_of(commands, fixture):
    """Ir per iteration of each command: the difference between two runs, over the difference in
    their counts. The runs are independent and callgrind's counts don't depend on load, so they
    run side by side, one valgrind per core."""
    commands = list(dict.fromkeys(commands))
    jobs = [(command, n) for command in commands for n in (LOW, HIGH)]
    with ThreadPoolExecutor(max_workers=max(1, min(len(jobs), len(os.sched_getaffinity(0))))) as pool:
        counts = dict(zip(jobs, pool.map(lambda job: collected(*job, fixture), jobs)))
    return {c: (counts[(c, HIGH)] - counts[(c, LOW)]) // (HIGH - LOW) for c in commands}


def allocations(fixture, planted=False):
    rows = {}
    words = [["allocations"] + (["planted"] if planted else [])]
    if not planted:
        words.append(["plugin", "0", fixture, "allocations"])
    for args in words:
        out = run([HARNESS, *args], capture_output=True).stdout
        for line in out.splitlines():
            name, value = line.split()
            rows[name] = int(value)
    return rows


def sections(path):
    """(flash, ram) of an ELF file, by the convention in budgets.toml."""
    out = run(["readelf", "-SW", path], capture_output=True).stdout
    flash = ram = 0
    for line in out.splitlines():
        fields = re.sub(r"^\s*\[\s*\d+\]\s*", "", line).split()
        if len(fields) < 5 or not fields[0].startswith("."):
            continue
        name, size = fields[0], int(fields[4], 16)
        if name in (".vector_table", ".text", ".rodata"):
            flash += size
        elif name == ".data":
            flash += size
            ram += size
        elif name in (".bss", ".uninit"):
            ram += size
    return flash, ram


def firmware(row, target):
    """(flash, ram) of the row's binary built for `target`."""
    run(["cargo", "build", "-q", "--target", target, "--profile", "chip",
         "-p", "daedalus-chip-blink", "--bins"], env=CODE_ENV)
    return sections(os.path.join(TARGET, target, "chip", row["binary"]))


def measure(rows, wanted, self_test, have_valgrind, values, key):
    """Fills `values` with what this run needs and it doesn't hold yet. True when it built."""
    elf = [(row, target) for row in rows if row["tool"] == "elf" and not self_test
           for target in row["targets"]]
    missing_ir = [c for c in wanted if have_valgrind and c not in values.get("ir", {})]
    missing_elf = [(row, t) for row, t in elf if f"{row['binary']} {t}" not in values.get("elf", {})]
    if not (missing_ir or missing_elf or "planted_alloc" not in values
            or (not self_test and "alloc" not in values)):
        return False
    fixture = build(key)
    if missing_ir and have_valgrind:
        values.setdefault("ir", {}).update(instructions_of(missing_ir, fixture))
    if not self_test and "alloc" not in values:
        values["alloc"] = allocations(fixture)
    if "planted_alloc" not in values:
        values["planted_alloc"] = allocations(fixture, planted=True)["alloc_fused_tick"]
    for row, target in missing_elf:
        flash, ram = firmware(row, target)
        values.setdefault("elf", {})[f"{row['binary']} {target}"] = [flash, ram]
    return True


def main():
    with open(os.path.join(ROOT, "testing", "budgets", "budgets.toml"), "rb") as file:
        rows = tomllib.load(file)["row"]
    self_test = "--self-test" in sys.argv[1:]
    have_valgrind = shutil.which("valgrind") is not None
    failures, skipped = [], []

    def report(name, text, ok):
        print(f"  {'ok  ' if ok else 'FAIL'} {name}: {text}")
        if not ok:
            failures.append(name)

    # Every callgrind row, and the planted pair, counted at once.
    wanted = [] if self_test else [row["command"] for row in rows if row["tool"] == "callgrind"]
    if have_valgrind:
        wanted += ["chain {n} 16", "chain {n} 16 extra"]
    wanted = list(dict.fromkeys(wanted))
    key = record.key()
    fresh = "--fresh" in sys.argv[1:] or os.environ.get("BUDGETS_FRESH") == "1"
    values = {} if fresh else (record.load(RECORD, key) or {})
    if measure(rows, wanted, self_test, have_valgrind, values, key):
        record.save(RECORD, key, values)
        print(f"  measured, kept under {key[:12]}")
    else:
        print(f"  measured before from the same inputs ({key[:12]}): used again; --fresh measures again")
    counted_ir = values.get("ir", {})
    measured = {}
    if not self_test:
        counted = values["alloc"]
        for row in rows:
            name, tool = row["name"], row["tool"]
            if tool == "callgrind":
                if not have_valgrind:
                    skipped.append(name)
                    continue
                value = counted_ir[row["command"]]
                measured[name] = value
                ceiling = row["next"] * (100 + row["tolerance_percent"]) // 100
                ok = value <= row["limit"] and value <= ceiling
                report(name, f"{value} Ir (limit {row['limit']}, this branch {row['next']}"
                             f" +{row['tolerance_percent']}% = {ceiling})", ok)
            elif tool == "alloc":
                value = counted[name]
                report(name, f"{value} allocations (limit {row['limit']})", value <= row["limit"])
            elif tool == "elf":
                for target in row["targets"]:
                    flash, ram = values["elf"][f"{row['binary']} {target}"]
                    ok = flash <= row["limit_flash"] and ram <= row["limit_ram"]
                    report(f"{name} {target}", f"{flash} B flash, {ram} B RAM (limit "
                           f"{row['limit_flash']}, {row['limit_ram']})", ok)
            elif tool == "gpu-lane":
                print(f"  --   {name}: checked by scripts/gpu-lavapipe.sh in the GPU workflow")
            else:
                report(name, f"unknown tool `{tool}`", False)
        for name in skipped:
            print(f"  --   {name}: not run here, valgrind is not installed")
        if skipped and REQUIRE_ALL:
            failures.extend(skipped)

    # The planted regressions: a check that cannot fail checks nothing.
    planted_failures = []
    planted = values["planted_alloc"]
    limit = next(row["limit"] for row in rows if row["name"] == "alloc_fused_tick")
    if planted <= limit:
        planted_failures.append("a stage that allocates every tick passed the allocation row")
    else:
        print(f"  ok   planted: one allocation a tick ({planted}) fails alloc_fused_tick")
    if have_valgrind:
        plain = measured.get("fused_chain_16_tick") or counted_ir["chain {n} 16"]
        extra = counted_ir["chain {n} 16 extra"]
        # The row's limit, were it set to what the chain costs with its tolerance: a stage that
        # does a loop of extra work must be over it.
        tolerance = next(row["tolerance_percent"] for row in rows
                         if row["name"] == "fused_chain_16_tick")
        ceiling = plain * (100 + tolerance) // 100
        if extra <= ceiling:
            planted_failures.append(
                f"a stage with a loop of extra work ({extra} Ir) passed a limit of {ceiling}")
        else:
            print(f"  ok   planted: a loop of extra work ({extra} Ir) fails a limit of {ceiling} Ir")
    elif REQUIRE_ALL:
        planted_failures.append("the planted instruction row needs valgrind")
    else:
        print("  --   planted: the instruction row was not run here, valgrind is not installed")

    for failure in planted_failures:
        print(f"  FAIL planted: {failure}")
    if failures or planted_failures:
        print(f"budgets: {len(failures) + len(planted_failures)} failed")
        return 1
    print("budgets: every row run is within its limit")
    return 0


if __name__ == "__main__":
    sys.exit(main())

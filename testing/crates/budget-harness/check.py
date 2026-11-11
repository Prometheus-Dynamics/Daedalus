#!/usr/bin/env python3
"""Measures the rows of testing/budgets/budgets.toml and fails on one above its limit.

    check.py             measure and check every row this machine can, then the planted rows
    check.py --self-test only the planted rows: each must fail

A row whose tool is missing is reported as not run. With BUDGETS_REQUIRE_ALL=1, as in the
budgets workflow, a row that could not be run is a failure.
"""
import os
import re
import shutil
import subprocess
import sys
import tomllib

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
TARGET = os.environ.get("CARGO_TARGET_DIR") or os.path.join(ROOT, "target")
HARNESS = os.path.join(TARGET, "release", "daedalus-budget-harness")
FIXTURE_DIR = os.path.join(TARGET, "tmp", "budget-fixture")
REQUIRE_ALL = os.environ.get("BUDGETS_REQUIRE_ALL") == "1"
LOW, HIGH = 1000, 3000


def run(args, **kwargs):
    return subprocess.run(args, cwd=ROOT, check=True, text=True, **kwargs)


def build():
    run(["cargo", "build", "-q", "--release", "-p", "daedalus-budget-harness"])
    manifest = os.path.join(ROOT, "testing", "fixtures", "dylib", "Cargo.toml")
    run(["cargo", "build", "-q", "--manifest-path", manifest, "--target-dir", FIXTURE_DIR])
    return os.path.join(FIXTURE_DIR, "debug", "libdaedalus_dylib_fixture.so")


def instructions(command, fixture):
    """Ir per iteration: the difference between two runs, over the difference in their counts."""
    counts = []
    for n in (LOW, HIGH):
        words = [w.replace("{n}", str(n)).replace("{fixture}", fixture) for w in command.split()]
        out = subprocess.run(
            ["valgrind", "--tool=callgrind", "--callgrind-out-file=/dev/null", HARNESS, *words],
            cwd=ROOT, text=True, capture_output=True,
        )
        found = re.search(r"Collected : (\d+)", out.stderr)
        if out.returncode != 0 or not found:
            raise RuntimeError(f"callgrind failed on `{command}`:\n{out.stderr[-2000:]}")
        counts.append(int(found.group(1)))
    return (counts[1] - counts[0]) // (HIGH - LOW)


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


def firmware(row):
    values = []
    for target in row["targets"]:
        run(["cargo", "build", "-q", "--target", target, "--profile", "chip",
             "-p", "daedalus-chip-blink", "--bins"])
        values.append((target, *sections(os.path.join(TARGET, target, "chip", row["binary"]))))
    return values


def main():
    with open(os.path.join(ROOT, "testing", "budgets", "budgets.toml"), "rb") as file:
        rows = tomllib.load(file)["row"]
    self_test = "--self-test" in sys.argv[1:]
    fixture = build()
    have_valgrind = shutil.which("valgrind") is not None
    failures, skipped = [], []

    def report(name, text, ok):
        print(f"  {'ok  ' if ok else 'FAIL'} {name}: {text}")
        if not ok:
            failures.append(name)

    measured = {}
    if not self_test:
        counted = allocations(fixture)
        for row in rows:
            name, tool = row["name"], row["tool"]
            if tool == "callgrind":
                if not have_valgrind:
                    skipped.append(name)
                    continue
                value = instructions(row["command"], fixture)
                measured[name] = value
                ceiling = row["next"] * (100 + row["tolerance_percent"]) // 100
                ok = value <= row["limit"] and value <= ceiling
                report(name, f"{value} Ir (limit {row['limit']}, this branch {row['next']}"
                             f" +{row['tolerance_percent']}% = {ceiling})", ok)
            elif tool == "alloc":
                value = counted[name]
                report(name, f"{value} allocations (limit {row['limit']})", value <= row["limit"])
            elif tool == "elf":
                for target, flash, ram in firmware(row):
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
    planted = allocations(fixture, planted=True)["alloc_fused_tick"]
    limit = next(row["limit"] for row in rows if row["name"] == "alloc_fused_tick")
    if planted <= limit:
        planted_failures.append("a stage that allocates every tick passed the allocation row")
    else:
        print(f"  ok   planted: one allocation a tick ({planted}) fails alloc_fused_tick")
    if have_valgrind:
        plain = measured.get("fused_chain_16_tick") or instructions("chain {n} 16", fixture)
        extra = instructions("chain {n} 16 extra", fixture)
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

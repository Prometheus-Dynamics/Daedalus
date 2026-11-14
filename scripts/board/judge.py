#!/usr/bin/env python3
"""Judges a board run from what it left behind, and says PASS or FAIL with the reason.

  judge.py linux <results dir>   a Linux board: one <name>.out and <name>.exit for each test binary
  judge.py pico <log file>       the Pico firmware's defmt log
  judge.py --self-test           the judges against planted runs, good and bad

A judge reads only files. It runs nothing and touches no board, so the same run can be judged
again later, and a planted bad run shows the judge can fail.
"""

import os
import re
import sys
import tempfile

RESULT = re.compile(r"test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored")
REPORT = re.compile(
    r"(compiled|tunable|loaded): (\d+) ticks, (\d+)\.\.(\d+) us, mean (\d+) us, stack (\d+) B"
)
MODES = ("compiled", "tunable", "loaded")
# The firmware's whole RAM budget. A tick's stack alone has to be under it.
PICO_STACK_BYTES = 2048
PICO_TICKS = 10000


def judge_linux(directory):
    """Every binary exited 0, said so itself, and at least one test ran in all."""
    problems, lines, passed_total = [], [], 0
    names = sorted(name[:-5] for name in os.listdir(directory) if name.endswith(".exit"))
    if not names:
        return ["no results: nothing was run"], []
    for name in names:
        code = open(os.path.join(directory, name + ".exit")).read().strip()
        try:
            out = open(os.path.join(directory, name + ".out"), errors="replace").read()
        except FileNotFoundError:
            out = ""
        results = RESULT.findall(out)
        passed = sum(int(result[1]) for result in results)
        failed = sum(int(result[2]) for result in results)
        passed_total += passed
        lines.append(f"{name}: exit {code}, {passed} passed, {failed} failed")
        if code != "0":
            problems.append(f"{name} exited {code}")
        elif failed or not results or any(result[0] != "ok" for result in results):
            problems.append(f"{name} exited 0 but didn't report a clean result")
    if passed_total == 0:
        problems.append("no test ran at all")
    return problems, lines


def judge_pico(path):
    """Each mode reported at least twice, with nothing failing. The first report of each is
    left out, since the flash cache is cold for it."""
    text = open(path, errors="replace").read()
    problems, lines = [], []
    if "a tick failed" in text:
        problems.append("a tick failed")
    if "does not load" in text:
        problems.append("plan A did not load")
    reports = {mode: [] for mode in MODES}
    for mode, ticks, low, high, mean, stack in REPORT.findall(text):
        reports[mode].append(tuple(int(value) for value in (ticks, low, high, mean, stack)))
    for mode in MODES:
        warm = reports[mode][1:]
        if not warm:
            problems.append(f"{mode}: fewer than two reports, so none with a warm cache")
            continue
        ticks = {report[0] for report in warm}
        low = min(report[1] for report in warm)
        high = max(report[2] for report in warm)
        mean = sum(report[3] for report in warm) // len(warm)
        stack = max(report[4] for report in warm)
        lines.append(
            f"{mode}: {len(warm)} warm reports, tick {low}..{high} us, mean {mean} us, "
            f"stack {stack} B"
        )
        if ticks != {PICO_TICKS}:
            problems.append(f"{mode}: reports of {sorted(ticks)} ticks, not {PICO_TICKS}")
        if not low <= mean <= high:
            problems.append(f"{mode}: a mean of {mean} us outside {low}..{high} us")
        if stack == 0 or stack > PICO_STACK_BYTES:
            problems.append(f"{mode}: a stack of {stack} B, outside 1..{PICO_STACK_BYTES}")
    return problems, lines


def verdict(problems, lines):
    for line in lines:
        print("  " + line)
    if problems:
        for problem in problems:
            print("  FAIL: " + problem)
        print("FAIL")
        return 1
    print("PASS")
    return 0


def self_test():
    """A good run passes and each planted fault fails for its own reason."""
    failed = []

    def expect(name, got, want):
        hit = (not got[0]) if want is None else any(want in problem for problem in got[0])
        print(("  ok   " if hit else "  FAIL ") + name)
        if not hit:
            failed.append(name)
            print("       problems:", got[0])

    def linux(files):
        directory = tempfile.mkdtemp()
        for name, text in files.items():
            open(os.path.join(directory, name), "w").write(text)
        return judge_linux(directory)

    ok = "running 3 tests\ntest result: ok. 3 passed; 0 failed; 0 ignored; 0 measured\n"
    bad = "test result: FAILED. 2 passed; 1 failed; 0 ignored; 0 measured\n"
    expect("linux: a clean run passes", linux({"a.exit": "0", "a.out": ok}), None)
    expect("linux: a failing binary fails", linux({"a.exit": "101", "a.out": bad}), "exited 101")
    expect("linux: exit 0 with a failed test fails", linux({"a.exit": "0", "a.out": bad}), "clean")
    expect("linux: a binary killed with no result fails", linux({"a.exit": "137"}), "exited 137")
    expect("linux: exit 0 that ran nothing fails", linux({"a.exit": "0", "a.out": ""}), "clean")
    expect("linux: an empty run fails", linux({}), "nothing was run")

    def pico(text):
        handle, path = tempfile.mkstemp()
        os.write(handle, text.encode())
        os.close(handle)
        return judge_pico(path)

    def report(mode, ticks=10000, low=40, high=90, mean=50, stack=600):
        return f"INFO {mode}: {ticks} ticks, {low}..{high} us, mean {mean} us, stack {stack} B\n"

    good = "".join(report(mode) for mode in MODES) * 3
    expect("pico: three rounds of all modes pass", pico(good), None)
    expect("pico: one round only fails", pico("".join(report(m) for m in MODES)), "fewer than two")
    expect("pico: a missing mode fails", pico(good.replace("loaded:", "other:")), "loaded")
    expect("pico: a failed tick fails", pico(good + "ERROR tunable: a tick failed\n"), "tick failed")
    expect("pico: a plan that didn't load fails", pico("ERROR plan A does not load\n"), "load")
    expect("pico: too deep a stack fails", pico(good + report("compiled", stack=4000)), "stack")
    expect("pico: a short run fails", pico(good + report("loaded", ticks=7)), "ticks")
    if failed:
        print(f"judge: self-test failed: {len(failed)} cases")
        return 1
    print("judge: the planted runs behave: a good one passes, each bad one fails for its reason")
    return 0


def main(argv):
    if argv[1:] == ["--self-test"]:
        return self_test()
    if len(argv) == 3 and argv[1] == "linux":
        return verdict(*judge_linux(argv[2]))
    if len(argv) == 3 and argv[1] == "pico":
        return verdict(*judge_pico(argv[2]))
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))

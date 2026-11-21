"""The rules of the batched GPU figures (testing/board-runs/run-sheet.md), read from the results
directory scripts/board/batch-on-board.sh writes. judge.py runs them as `judge.py figures`, and its
self-test runs `planted_figures`: a good run passes, and each planted wrong result fails its rule.

A board run passes only when every figure ran and met its rule. A rehearsal (lavapipe, no layer,
no V3D) may skip a step with its reason, and does not run E11: those are reported, not failed.
"""

import os
import re
import tempfile

# A validation layer's message. The layer's own "it is active" note at instance creation is not
# one: it says nothing about the calls.
VALIDATION = re.compile(r"Validation (Error|Warning|Performance Warning|Information)|VUID-|SYNC-HAZARD|UNASSIGNED-")
LAYER_STATUS = re.compile(r"CreateInstance-status-message|Khronos Validation Layer Active")

# E9 and E14's shares of 50 runs: the write is calibrated to outlive its export in all of them, and
# a few signalled early mean a barely long enough write, not a wrong fence.
PENDING_SHARE = 0.9
# E9 and E14's planted early readers: of 50 runs, the share a reader that doesn't wait must catch.
# Fewer means the figure can't tell a reader that waits from one that doesn't.
CAUGHT_SHARE = 0.9
# E14: eight buffers, 1000 imports, and the reference's top of 44-50 allocations per import.
STEADY_BUFFERS, STEADY_IMPORTS, ALLOCATIONS_MAX = 8, 1000, 50
# E11 (b): resets the kernel has to have made before "the device survives" is a finding.
RESETS_MIN = 3
# Ticks (D2 and I2): the reference's per-stage figures on the CM5, its detector frame, and the 2026-11-11
# board figures this run is set beside (reported, not judged: D2 asks for the reason of a worse one).
DEV_MERGED_NS, DEV_UNMERGED_NS, DEV_DETECTOR_NS = 498, 655, 7000
EARLIER = {"merged": 1115, "unmerged": 1960, "one": 395, "idle": 35,
           "fused_chain_16_tick": 1112, "unfused_chain_16_tick": 1921, "idle_tick_16": 34.9, "detector_frame": 1225}
WORSE = 1.25
BENCHES = ("idle_tick_16", "fused_chain_16_tick", "unfused_chain_16_tick", "detector_frame")
UNITS = {"ps": 0.001, "ns": 1.0, "µs": 1000.0, "us": 1000.0, "ms": 1e6, "s": 1e9}
NODE_COUNTS = (4, 8)
FORMS = ("fragment merged", "fragment unmerged", "compute merged", "compute unmerged")


def read(directory, name):
    try:
        with open(os.path.join(directory, name), errors="replace") as handle:
            return handle.read()
    except FileNotFoundError:
        return None


def steps(text):
    """The figure file's steps: name to (output, exit code)."""
    found, name, lines = {}, None, []
    for line in text.splitlines():
        start = re.match(r"== step (\S+): ", line)
        if start:
            name, lines = start.group(1), []
            continue
        end = re.match(r"== exit (\d+)", line)
        if end and name:
            found[name] = ("\n".join(lines), int(end.group(1)))
            name = None
            continue
        if name:
            lines.append(line)
    return found


def share(match):
    return int(match.group(1)), int(match.group(2))


class Figure:
    """What one figure gives: its problems, its lines, and whether it was skipped with a reason."""

    def __init__(self, name, rehearsal):
        self.name, self.rehearsal = name, rehearsal
        self.problems, self.lines = [], []

    def fail(self, why):
        self.problems.append(f"{self.name}: {why}")

    def say(self, line):
        self.lines.append(f"{self.name}: {line}")

    def skipped(self, why):
        """A step that couldn't run: a note in a rehearsal, a failure on a board."""
        if self.rehearsal:
            self.say(f"skipped here: {why}")
        else:
            self.fail(f"skipped on the board: {why}")

    def ran(self, directory):
        """The figure's file, or None with the reason recorded when it didn't run."""
        gate = (read(directory, f"{self.name.lower()}-gate.txt") or "").strip()
        text = read(directory, f"{self.name.lower()}.txt")
        if text is None:
            self.fail("no result file")
            return None
        if gate.startswith("not run: E13"):
            self.fail(gate)
            return None
        for name, (_, code) in steps(text).items():
            if code != 0:
                self.fail(f"step {name} exited {code}")
        return text


def find(figure, text, pattern, what):
    match = re.search(pattern, text, re.M)
    if not match:
        figure.fail(f"no {what} line")
    return match


def judge_e13(directory, rehearsal):
    figure = Figure("E13", rehearsal)
    gate = (read(directory, "e13-gate.txt") or "").strip()
    text = figure.ran(directory)
    if text is None:
        return figure
    if gate.startswith("skipped"):
        figure.skipped(gate)
        return figure
    if not gate.startswith("open"):
        figure.fail(f"the gate did not open: {gate or 'no gate'}")
        return figure
    planted = read(directory, "e13-planted.txt") or ""
    rules = sorted(set(re.findall(r"VUID-[\w-]+", planted)))
    if not rules:
        figure.fail("the planted call went unnamed: no VUID- line")
    else:
        figure.say(f"the planted call named {', '.join(rules)}")
    found = steps(text)
    for name in ("e13-clean-vk-sys", "e13-clean-fragment", "e13-clean-region", "e13-clean-raw-submit"):
        if name not in found:
            figure.fail(f"no {name} run")
            continue
        noisy = [
            line
            for line in found[name][0].splitlines()
            if VALIDATION.search(line) and not LAYER_STATUS.search(line)
        ]
        if noisy:
            figure.fail(f"{name} printed {len(noisy)} validation lines, the first: {noisy[0].strip()}")
        else:
            figure.say(f"{name}: no validation line")
    return figure


def judge_e9(directory, rehearsal):
    figure = Figure("E9", rehearsal)
    text = figure.ran(directory)
    if text is None:
        return figure
    skip = re.search(r"^dma-fence: skipped: (.*)$", text, re.M)
    if skip:
        figure.skipped(skip.group(1))
        return figure
    attached = find(figure, text, r"dma-fence: fence attached: (\d+) of (\d+) submits; host waits (\d+)", "attach")
    if attached:
        got, submits, waits = (int(value) for value in attached.groups())
        figure.say(f"fence attached on {got} of {submits} submits, host waits {waits}")
        if got + waits < submits:
            figure.fail(f"{submits - got - waits} submits neither attached a fence nor waited on the host")
        why = re.search(r"^dma-fence: host wait: (.*)$", text, re.M)
        if waits and not why:
            figure.fail("host waits with no reason named")
        elif why:
            figure.say(f"host wait: {why.group(1)}")
    exports = find(figure, text, r"dma-fence: sync_file: (\d+) of (\d+) exports; unsignalled at export: (\d+) of (\d+)", "exports")
    if exports:
        files, runs, pending, _ = (int(value) for value in exports.groups())
        figure.say(f"sync_file {files} of {runs}, unsignalled at export {pending} of {runs}")
        if files < runs:
            figure.fail(f"sync_file: {files} of {runs}")
        if pending < PENDING_SHARE * runs:
            figure.fail(f"unsignalled at export: {pending} of {runs}, needs {PENDING_SHARE:.0%}: the attached fence doesn't follow the write")
    rules = [
        (r"dma-fence: \(a\) the CPU read in the sync bracket, no other wait, sees the GPU's values: (\d+) of (\d+)",
         "(a) the waiting reader saw the GPU's values", 1.0),
        (r"dma-fence: \(b\) planted: a plain read right after the submit sees the sentinel: (\d+) of (\d+)",
         "(b) the planted early reader saw the sentinel", CAUGHT_SHARE),
        (r"dma-fence: \(c\) planted: attaching off: signalled at export (\d+) of (\d+)",
         "(c) with attaching off, the fence was signalled at export", CAUGHT_SHARE),
        (r"dma-fence: \(c\) planted: attaching off: .*the bracketed read sees the sentinel: (\d+) of (\d+)",
         "(c) with attaching off, the bracketed read saw the sentinel", CAUGHT_SHARE),
    ]
    for pattern, what, need in rules:
        match = find(figure, text, pattern, what)
        if match:
            got, runs = share(match)
            figure.say(f"{what}: {got} of {runs}")
            if runs == 0 or got < need * runs:
                figure.fail(f"{what}: {got} of {runs}, needs {need:.0%}"
                            + ("" if need == 1.0 else ": the figure can't catch an early reader"))
    cost = re.search(r"^dma-fence: attach cost: (.*)$", text, re.M)
    if cost:
        figure.say(f"attach cost: {cost.group(1)}")
    else:
        figure.fail("no attach cost line")
    return figure


def judge_e14(directory, rehearsal):
    figure = Figure("E14", rehearsal)
    text = figure.ran(directory)
    if text is None:
        return figure
    skip = re.search(r"^dma-steady: skipped: (.*)$", text, re.M)
    if skip:
        figure.skipped(f"steady: {skip.group(1)}")
    else:
        match = find(figure, text, r"device imports (\d+), cache hits (\d+), copies (\d+)", "steady import")
        if match:
            imports, hits, copies = (int(value) for value in match.groups())
            figure.say(f"steady: {imports} imports, {hits} cache hits, {copies} copies")
            if (imports, hits, copies) != (STEADY_BUFFERS, STEADY_IMPORTS - STEADY_BUFFERS, 0):
                figure.fail(f"steady: {imports} imports, {hits} hits, {copies} copies; needs "
                            f"{STEADY_BUFFERS}, {STEADY_IMPORTS - STEADY_BUFFERS} and 0")
        match = find(figure, text, r"allocations per import: .*after them median (\d+)", "allocations")
        if match:
            allocations = int(match.group(1))
            figure.say(f"steady: {allocations} allocations per import (reference 44-50)")
            if allocations > ALLOCATIONS_MAX:
                figure.fail(f"steady: {allocations} allocations per import, over {ALLOCATIONS_MAX}")
    skip = re.search(r"^dma-wait: skipped: (.*)$", text, re.M)
    if skip:
        figure.skipped(f"device-side wait: {skip.group(1)}")
        return figure
    rules = [
        (r"dma-wait: producer still writing at the consumer's submit: (\d+) of (\d+)", "producer pending", PENDING_SHARE),
        (r"dma-wait: the consumer's copy saw the GPU's values: (\d+) of (\d+)", "copy correct", 1.0),
    ]
    for pattern, what, need in rules:
        match = find(figure, text, pattern, what)
        if match:
            got, runs = share(match)
            figure.say(f"wait: {what}: {got} of {runs}")
            if runs == 0 or got < need * runs:
                figure.fail(f"wait: {what}: {got} of {runs}, needs {need:.0%}")
    wait = re.search(r"dma-wait: copy done after the fence signalled.*?: median ([\d.]+) ms", text)
    if wait:
        figure.say(f"wait: copy done {wait.group(1)} ms after the signal (reference 0.2 ms)")
    judge_e14_plant(figure, text)
    return figure


PLANT = (r"^dma-wait: {label}: the copy with no wait saw the sentinel (\d+) of (\d+), the GPU's values "
         r"(\d+), neither (\d+)(?:; it waited for the hold (\d+); the write was submitted before the copy (\d+))?$")


def plant_outcome(figure, text, label, need_submitted):
    """The outcome of one held plant, as the run sheet sets it out before the run: A, the copy saw the
    sentinel (the plant catches a consumer that doesn't wait); B, it saw the GPU's values and waited for
    the hold (one V3D orders a later job behind held work); None, neither, which fails."""
    match = re.search(PLANT.format(label=re.escape(label)), text, re.M)
    if not match:
        figure.fail(f"plant: no '{label}' line")
        return None
    sentinel, runs, gpu, other, waited, submitted = (int(v or 0) for v in match.groups())
    figure.say(f"plant: {label}: sentinel {sentinel} of {runs}, GPU's values {gpu}, neither {other}, "
               f"waited for the hold {waited}, write submitted first {submitted}")
    if runs == 0:
        figure.fail(f"plant: {label}: no runs")
        return None
    if need_submitted and submitted < CAUGHT_SHARE * runs:
        figure.fail(f"plant: {label}: the held write was submitted before the copy in {submitted} of {runs}, "
                    f"needs {CAUGHT_SHARE:.0%}: the copy didn't overtake a submitted write")
        return None
    if sentinel >= CAUGHT_SHARE * runs:
        return "A"
    if gpu >= CAUGHT_SHARE * runs and waited >= CAUGHT_SHARE * runs:
        return "B"
    figure.fail(f"plant: {label}: neither outcome (A: sentinel, B: GPU's values after waiting for the hold, "
                f"each {CAUGHT_SHARE:.0%})")
    return None


OUTCOMES = {
    "A": "A: a copy that doesn't wait sees the sentinel, so the plant catches it",
    "B": "B: one V3D runs the copy after held work, so the plant is an observation here, not a rule",
}


def judge_e14_plant(figure, text):
    skip = re.search(r"^dma-wait: planted: skipped: (.*)$", text, re.M)
    if skip:
        figure.skipped(f"plant: {skip.group(1)}")
        return
    for line in re.findall(r"^dma-wait: (.*): failures (\d+), the last: (.*)$", text, re.M):
        figure.fail(f"plant: {line[0]}: {line[1]} failures, the last: {line[2]}")
    here = plant_outcome(figure, text, "planted, held, this process", not figure.rehearsal)
    if here:
        figure.say(f"plant: outcome {OUTCOMES[here]}")
    peer = re.search(r"^dma-wait: second process: skipped: (.*)$", text, re.M)
    if peer:
        figure.skipped(f"second process: {peer.group(1)}")
    else:
        there = plant_outcome(figure, text, "planted, held, second process", not figure.rehearsal)
        if there:
            figure.say(f"plant: second process: outcome {OUTCOMES[there]}")
        if here and there and here != there:
            figure.say(f"plant: the second process's outcome {there} differs from this process's {here}")
    for label in ("order, not held, this process", "order, not held, second process"):
        match = re.search(PLANT.format(label=re.escape(label)), text, re.M)
        if match:
            sentinel, runs, gpu, other = (int(v) for v in match.groups()[:4])
            figure.say(f"order: {label.removeprefix('order, ')}: GPU's values {gpu} of {runs}, "
                       f"sentinel {sentinel}, neither {other}")


def judge_e6(directory, rehearsal):
    figure = Figure("E6", rehearsal)
    text = figure.ran(directory)
    if text is None:
        return figure
    rows = {}
    pattern = r"^e6 nodes (\d+) (fragment merged|fragment unmerged|compute merged|compute unmerged)[^:]*: median ([\d.]+) ms, submits (\d+), passes (\d+)"
    for nodes, form, median, submits, passes in re.findall(pattern, text, re.M):
        rows.setdefault((int(nodes), form), []).append((float(median), int(submits), int(passes)))
    for nodes in NODE_COUNTS:
        got = {form: rows.get((nodes, form), []) for form in FORMS}
        if any(len(runs) < 3 for runs in got.values()):
            figure.fail(f"{nodes} nodes: fewer than three rounds of every form")
            continue
        mid = {form: sorted(run[0] for run in runs)[1] for form, runs in got.items()}
        for submits, passes in {(run[1], run[2]) for run in got["fragment merged"]}:
            if (submits, passes) != (1, 1):
                figure.fail(f"{nodes} nodes: the merged fragment pass is {submits} submits and {passes} passes, not 1 and 1")
        for kind in ("fragment", "compute"):
            merged, apart = got[f"{kind} merged"][0], got[f"{kind} unmerged"][0]
            figure.say(f"{nodes} nodes {kind}: merged {mid[f'{kind} merged']:.3f} ms ({merged[1]} submits, {merged[2]} passes), "
                       f"unmerged {mid[f'{kind} unmerged']:.3f} ms ({apart[1]} submits, {apart[2]} passes)")
            if merged[1] > apart[1] or merged[2] > apart[2]:
                figure.fail(f"{nodes} nodes {kind}: merged takes more submits or passes than unmerged")
            if mid[f"{kind} merged"] > mid[f"{kind} unmerged"]:
                figure.fail(f"{nodes} nodes {kind}: merged is slower than unmerged")
    return figure


def judge_e11(directory, rehearsal):
    figure = Figure("E11", rehearsal)
    gate = (read(directory, "e11-gate.txt") or "").strip()
    text = figure.ran(directory)
    if text is None:
        return figure
    if gate.startswith("not run") and rehearsal:
        figure.say(gate)
        return figure
    if not gate.startswith("open"):
        figure.fail(gate or "no gate")
        return figure
    summaries = [
        dict(field.split("=", 1) for field in line.split())
        for line in re.findall(r"^grind: summary: (.*)$", text, re.M)
    ]
    resets = re.search(r"v3d resets in the kernel log: start (-?\d+), end (-?\d+)", text)
    if not summaries:
        figure.fail("no grind summary")
        return figure
    during = int(resets.group(2)) - int(resets.group(1)) if resets else 0
    figure.say(f"{len(summaries)} grind runs, {during} kernel resets during E11")
    fired = [entry for entry in summaries if entry.get("lost_callback") == "fired"]
    if fired:
        good = [
            entry
            for entry in fired
            if entry.get("known_run") == "correct" and re.fullmatch(r"[\d.]+", entry.get("reopen_s", ""))
        ]
        if good:
            figure.say(f"(a): the lost callback fired; reopened in {good[0]['reopen_s']} s, and the known run is correct")
        else:
            figure.fail("the lost callback fired, but no reopened device ran the known run correctly")
        return figure
    if any(entry.get("wait") != "finished" for entry in summaries):
        figure.fail("a wait failed with no loss reported by the driver: neither (a) nor (b)")
    if any(entry.get("known_run") != "correct" for entry in summaries):
        figure.fail("no loss reported, and a known run on the device was not correct")
    if any(entry.get("same_device") != "yes" for entry in summaries):
        figure.fail("no loss reported, but the device changed")
    if during < RESETS_MIN:
        figure.fail(f"only {during} kernel resets, needs {RESETS_MIN} for (b)")
    if not figure.problems:
        figure.say("(b): V3D recovers a hung job in the kernel; the device survives, so driver-caused "
                   "loss is not reachable by a hang on this hardware")
    return figure


def judge_ticks(directory, rehearsal):
    figure = Figure("ticks", rehearsal)
    text = figure.ran(directory)
    if text is None:
        return figure
    wall = {}
    for stages, form, each in re.findall(r"^chain: (\d+) stages (merged|unmerged), \d+ ticks, sum \d+, ([\d.]+) ns a tick", text, re.M):
        wall.setdefault("one" if stages == "1" else form, []).append(float(each))
    wall["idle"] = [float(each) for each in re.findall(r"^idle: 16 stages, \d+ ticks, \d+ idle, ([\d.]+) ns a tick", text, re.M)]
    if any(len(wall.get(kind, [])) < 3 for kind in ("merged", "unmerged", "one", "idle")):
        figure.fail("fewer than three rounds of the wall-clock runs")
        return figure
    mid = {kind: sorted(runs)[len(runs) // 2] for kind, runs in wall.items()}
    merged, unmerged = (mid["merged"] - mid["one"]) / 15, (mid["unmerged"] - mid["one"]) / 15
    figure.say(f"wall, a tick: merged {mid['merged']:.1f} ns, unmerged {mid['unmerged']:.1f} ns, "
               f"1 stage {mid['one']:.1f} ns, idle {mid['idle']:.2f} ns")
    figure.say(f"per stage, 1 stage subtracted: merged {merged:.1f} ns, unmerged {unmerged:.1f} ns; "
               f"a tick / 16: merged {mid['merged'] / 16:.1f} ns, unmerged {mid['unmerged'] / 16:.1f} ns")
    if merged >= unmerged:
        figure.fail("wall: merged per stage is not below unmerged")
    if mid["merged"] / 16 > DEV_MERGED_NS or mid["unmerged"] / 16 > DEV_UNMERGED_NS:
        figure.fail(f"wall: a per-stage figure is worse than the reference's ({DEV_MERGED_NS} and {DEV_UNMERGED_NS} ns)")
    bench = {}
    for name in BENCHES:
        found = re.search(rf"^{name}\s+time:\s+\[\S+ \S+ ([\d.]+) (\S+) ", text, re.M)
        if found and found.group(2) in UNITS:
            bench[name] = float(found.group(1)) * UNITS[found.group(2)]
        else:
            figure.fail(f"no criterion median for {name}")
    if len(bench) == len(BENCHES):
        figure.say("criterion medians: " + ", ".join(f"{name} {bench[name]:.1f} ns" for name in BENCHES))
        if bench["fused_chain_16_tick"] >= bench["unfused_chain_16_tick"]:
            figure.fail("criterion: fused is not below unfused")
        if bench["fused_chain_16_tick"] / 16 > DEV_MERGED_NS or bench["unfused_chain_16_tick"] / 16 > DEV_UNMERGED_NS:
            figure.fail(f"criterion: a per-stage figure is worse than the reference's ({DEV_MERGED_NS} and {DEV_UNMERGED_NS} ns)")
        if bench["detector_frame"] > DEV_DETECTOR_NS:
            figure.fail(f"criterion: detector_frame is above the reference's {DEV_DETECTOR_NS} ns")
    for name, now in list(mid.items()) + list(bench.items()):
        if now > EARLIER[name] * WORSE:
            figure.say(f"worse than 2026-11-11: {name} {now:.1f} ns against {EARLIER[name]} ns: D2 and I2 give the reason")
    return figure


def judge_figures(directory, rehearsal=False):
    """The batch's preflight, end and GPU check, then each figure's rule."""
    problems, lines = [], [f"mode: {'rehearsal' if rehearsal else 'board'}"]
    if not (read(directory, "preflight.txt") or "").rstrip().endswith("preflight ok"):
        problems.append("the preflight did not pass")
    if read(directory, "done") is None:
        problems.append("the run did not reach its end")
    health = (read(directory, "gpu-health.txt") or "").rstrip().splitlines()
    if not health or health[-1] != "gpu ok":
        problems.append(f"the GPU check at the end: {health[-1] if health else 'no result'}")
    adapters = set()
    for name in sorted(os.listdir(directory)):
        if name.endswith(".txt"):
            adapters |= set(re.findall(r"^adapter: (.*)$", read(directory, name) or "", re.M))
    lines.append(f"adapters: {', '.join(sorted(adapters)) or 'none named'}")
    software = sorted(name for name in adapters if re.search(r"llvmpipe|lavapipe", name, re.I))
    if software and not rehearsal:
        problems.append(f"a step ran on a software adapter: {', '.join(software)}")
    # A session of some figures only (BATCH_FIGURES) says which in figures.txt; the others aren't
    # judged, and the verdict names what it covers. With no figures.txt every figure is judged.
    session = (read(directory, "figures.txt") or "").split()
    if session:
        lines.append(f"figures in this session: {' '.join(session)}")
    for judge in (judge_e13, judge_e9, judge_e14, judge_e6, judge_ticks, judge_e11):
        name = judge.__name__.removeprefix("judge_")
        if session and name not in session:
            lines.append(f"{name.upper() if name != 'ticks' else name}: not in this session")
            continue
        figure = judge(directory, rehearsal)
        lines.extend(figure.lines)
        problems.extend(figure.problems)
        lines.append(f"{figure.name}: {'FAIL' if figure.problems else 'pass'}")
    return problems, lines


# The planted runs: a whole board run that passes, and one wrong result at a time.
def block(name, output, code=0):
    return f"== step {name}: cmd\n{output}\n== exit {code}\n"


E13_CLEAN = "".join(block(f"e13-clean-{name}", "test result: ok. 3 passed; 0 failed") for name in ("vk-sys", "fragment", "region", "raw-submit"))
E9_GOOD = block("e9-fence", "\n".join([
    "dma-fence: import: the device imported the 1 MiB dma-heap buffer in place",
    "dma-fence: GPU write: 256 loop rounds per word, sized so the write outlives a read right after its submit",
    "dma-fence: fence attached: 55 of 55 submits; host waits 0",
    "dma-fence: sync_file: 50 of 50 exports; unsignalled at export: 50 of 50",
    "dma-fence: (a) the CPU read in the sync bracket, no other wait, sees the GPU's values: 50 of 50",
    "dma-fence: (b) planted: a plain read right after the submit sees the sentinel: 50 of 50",
    "dma-fence: (c) planted: attaching off: signalled at export 50 of 50; the bracketed read sees the sentinel: 50 of 50",
    "dma-fence: attach cost: host 41000 ns a submit; a short write, submit to done: median 0.4100 ms attached, 0.3600 ms not",
]))
E14_GOOD = block("e14-steady", "\n".join([
    "dma-steady: 8 buffers of 1 MiB, 1000 imports in rotation: device imports 8, cache hits 992, copies 0",
    "dma-steady: allocations per import: first 8 imports [60, 60, 60, 60, 60, 60, 60, 60]; after them median 30 (min 30, max 31)",
])) + block("e14-wait", "\n".join([
    "dma-wait: producer still writing at the consumer's submit: 50 of 50",
    "dma-wait: the consumer's copy saw the GPU's values: 50 of 50",
    "dma-wait: copy done after the fence signalled (the device-side wait's cost and the copy): median 0.3000 ms (p99 1, best 0, n 50)",
    "dma-wait: planted, held, this process: the copy with no wait saw the sentinel 50 of 50, the GPU's values 0, neither 0; it waited for the hold 0; the write was submitted before the copy 50",
    "dma-wait: order, not held, this process: the copy with no wait saw the sentinel 0 of 50, the GPU's values 50, neither 0",
    "dma-wait: planted, held, second process: the copy with no wait saw the sentinel 50 of 50, the GPU's values 0, neither 0; it waited for the hold 0; the write was submitted before the copy 50",
    "dma-wait: order, not held, second process: the copy with no wait saw the sentinel 0 of 50, the GPU's values 50, neither 0",
]))
E14_B = (E14_GOOD
         .replace("sentinel 50 of 50, the GPU's values 0, neither 0; it waited for the hold 0",
                  "sentinel 0 of 50, the GPU's values 50, neither 0; it waited for the hold 50"))


def e6_round():
    out = []
    for nodes in NODE_COUNTS:
        out += [
            f"e6 nodes {nodes} fragment merged: median 1.3000 ms, submits 1, passes 1",
            f"e6 nodes {nodes} fragment unmerged, one pass per node: median 5.0000 ms, submits {nodes}, passes {nodes}",
            f"e6 nodes {nodes} compute merged: median 7.3000 ms, submits 1, passes 1",
            f"e6 nodes {nodes} compute unmerged, one region per node: median 28.0000 ms, submits {nodes}, passes {nodes}",
        ]
    return "\n".join(out)


def grind(lost="not-fired", known="correct", same="yes", wait="finished", reopen="none"):
    return block("e11-hang", f"grind: summary: wait={wait} lost_callback={lost} known_run={known} same_device={same} reopen_s={reopen}")


def ticks_round(n, merged=1100.0, unmerged=1900.0, one=400.0, idle=35.0):
    return (block(f"ticks-merged-{n}", f"chain: 16 stages merged, 10000000 ticks, sum 1, {merged:.2f} ns a tick")
            + block(f"ticks-unmerged-{n}", f"chain: 16 stages unmerged, 10000000 ticks, sum 1, {unmerged:.2f} ns a tick")
            + block(f"ticks-one-{n}", f"chain: 1 stages merged, 10000000 ticks, sum 1, {one:.2f} ns a tick")
            + block(f"ticks-idle-{n}", f"idle: 16 stages, 10000000 ticks, 10000000 idle, {idle:.2f} ns a tick"))


TICKS_BENCH = block("ticks-bench", "\n".join([
    "idle_tick_16            time:   [34.512 ns 34.900 ns 35.301 ns]",
    "fused_chain_16_tick     time:   [1.1050 µs 1.1120 µs 1.1190 µs]",
    "unfused_chain_16_tick   time:   [1.9100 µs 1.9210 µs 1.9320 µs]",
    "detector_frame          time:   [1.2200 µs 1.2250 µs 1.2300 µs]",
]))
TICKS_GOOD = "".join(ticks_round(n) for n in (1, 2, 3)) + TICKS_BENCH


E11_RESETS = "e11: v3d resets in the kernel log: start 5, end 8, during E11 3\n"
E11_B = "".join(grind() for _ in range(3)) + E11_RESETS


def board_run():
    return {
        "preflight.txt": "daedalus-gpu-budgets: exit 1\npreflight ok\n",
        "done": "2026-11-20T00:00:00Z\n",
        "gpu-health.txt": "== exit 0\ngpu ok\n",
        "e13-gate.txt": "open: the layer named the planted call's rule\n",
        "e13-planted.txt": "Validation Error: [ VUID-VkBufferCreateInfo-size-00912 ] size is 0\n",
        "e13.txt": block("e13-planted", "VUID-VkBufferCreateInfo-size-00912") + E13_CLEAN,
        "e9-gate.txt": "open: run\n",
        "e9.txt": "adapter: V3D 7.1.10.2\n" + E9_GOOD,
        "e14-gate.txt": "open: run\n",
        "e14.txt": E14_GOOD,
        "e6-gate.txt": "open: run\n",
        "e6.txt": "".join(block(f"e6-elements-{n}", e6_round()) for n in (1, 2, 3)),
        "ticks-gate.txt": "open: run\n",
        "ticks.txt": TICKS_GOOD,
        "e11-gate.txt": "open: the watchdog is armed\n",
        "e11.txt": E11_B,
    }


def planted_figures(scratch, expect):
    def judged(files, rehearsal=False):
        directory = tempfile.mkdtemp(dir=scratch)
        for name, text in files.items():
            if text is not None:
                with open(os.path.join(directory, name), "w") as handle:
                    handle.write(text)
        return judge_figures(directory, rehearsal)

    good = board_run()

    def planted(**changes):
        return dict(good, **{name.replace("_", "-").replace("-txt", ".txt"): text for name, text in changes.items()})

    expect("figures: a good board run passes, E11 by (b)", judged(good), None)
    fired = grind(lost="fired", same="no", reopen="1.200")
    expect("figures: a good board run passes, E11 by (a)", judged(planted(e11_txt=fired + E11_RESETS)), None)

    # E13
    expect("figures: E13 planted call unnamed fails", judged(planted(e13_planted_txt="test result: ok\n")), "went unnamed")
    noisy = E13_CLEAN.replace("3 passed; 0 failed", "Validation Error: [ VUID-StandaloneSpirv-None-10684 ] x", 1)
    expect("figures: E13 a clean run with a validation line fails",
           judged(planted(e13_txt=block("e13-planted", "VUID-x") + noisy)), "validation lines")
    closed = "not run: E13 did not open: closed: the planted call went unnamed"
    expect("figures: E13 closed stops the rest, which fails",
           judged(planted(e13_gate_txt="closed: unnamed\n", e9_gate_txt=closed, e9_txt=closed)), "not run: E13")
    expect("figures: E13 with no layer fails on a board", judged(planted(e13_gate_txt="skipped: rehearsal: no layer\n")), "skipped on the board")
    expect("figures: a board run on a software adapter fails",
           judged(planted(e9_txt="adapter: llvmpipe (LLVM 19.1.0, 256 bits)\n" + E9_GOOD)), "software adapter")
    # E9
    expect("figures: E9 skipped fails on a board",
           judged(planted(e9_txt=block("e9-fence", "dma-fence: skipped: no /dev/dma_heap/system"))), "skipped on the board")
    expect("figures: E9 a fence signalled at export fails",
           judged(planted(e9_txt=E9_GOOD.replace("unsignalled at export: 50", "unsignalled at export: 3"))), "unsignalled at export")
    expect("figures: E9 not a sync_file fails",
           judged(planted(e9_txt=E9_GOOD.replace("sync_file: 50 of 50", "sync_file: 0 of 50"))), "sync_file")
    expect("figures: E9 a waiting reader that once saw the sentinel fails",
           judged(planted(e9_txt=E9_GOOD.replace("GPU's values: 50 of 50", "GPU's values: 49 of 50"))), "(a)")
    expect("figures: E9 a planted early reader that saw the GPU's values fails",
           judged(planted(e9_txt=E9_GOOD.replace("submit sees the sentinel: 50", "submit sees the sentinel: 30"))),
           "can't catch an early reader")
    expect("figures: E9 attaching off with the fence still pending fails",
           judged(planted(e9_txt=E9_GOOD.replace("signalled at export 50", "signalled at export 2"))), "attaching off, the fence")
    expect("figures: E9 attaching off with the read still right fails",
           judged(planted(e9_txt=E9_GOOD.replace("read sees the sentinel: 50", "read sees the sentinel: 0"))), "attaching off, the bracketed")
    expect("figures: E9 submits that neither attached nor waited fail",
           judged(planted(e9_txt=E9_GOOD.replace("attached: 55 of 55", "attached: 40 of 55"))), "neither attached")
    expect("figures: E9 a host wait with no reason fails",
           judged(planted(e9_txt=E9_GOOD.replace("attached: 55 of 55 submits; host waits 0", "attached: 50 of 55 submits; host waits 5"))),
           "no reason named")
    expect("figures: E9 with no attach cost fails",
           judged(planted(e9_txt=E9_GOOD.replace("attach cost", "attach price"))), "attach cost")
    expect("figures: E9 a step that crashed fails",
           judged(planted(e9_txt=E9_GOOD.replace("== exit 0", "== exit 101"))), "exited 101")
    # E14
    expect("figures: E14 no cache hits fails",
           judged(planted(e14_txt=E14_GOOD.replace("imports 8, cache hits 992", "imports 1000, cache hits 0"))), "steady")
    expect("figures: E14 too many allocations fails",
           judged(planted(e14_txt=E14_GOOD.replace("median 30", "median 61"))), "allocations")
    expect("figures: E14 a copy that missed the GPU's values fails",
           judged(planted(e14_txt=E14_GOOD.replace("GPU's values: 50 of 50", "GPU's values: 40 of 50"))), "copy correct")
    expect("figures: E14 outcome B, the copy after the held write, passes as an observation",
           judged(planted(e14_txt=E14_B)), None)
    mixed = E14_GOOD.replace("sentinel 50 of 50, the GPU's values 0, neither 0; it waited for the hold 0",
                             "sentinel 25 of 50, the GPU's values 25, neither 0; it waited for the hold 0", 1)
    expect("figures: E14 a held plant that is neither outcome fails", judged(planted(e14_txt=mixed)), "neither outcome")
    late = E14_GOOD.replace("submitted before the copy 50", "submitted before the copy 3", 1)
    expect("figures: E14 a held write not submitted before the copy fails", judged(planted(e14_txt=late)),
           "didn't overtake")
    alone = re.sub(r"dma-wait: planted, held, second process.*\n", "dma-wait: second process: skipped: no socket\n",
                   E14_GOOD)
    expect("figures: E14 with no second process fails on a board", judged(planted(e14_txt=alone)), "second process")
    broken = E14_GOOD.replace("dma-wait: order, not held, this process",
                              "dma-wait: planted, held, this process: failures 2, the last: lost\ndma-wait: order, not held, this process", 1)
    expect("figures: E14 plant failures fail", judged(planted(e14_txt=broken)), "failures")
    expect("figures: E14 a producer done before the consumer fails",
           judged(planted(e14_txt=E14_GOOD.replace("submit: 50 of 50", "submit: 0 of 50"))), "producer pending")
    # E6
    two = good["e6.txt"].replace("fragment merged: median 1.3000 ms, submits 1", "fragment merged: median 1.3000 ms, submits 2")
    expect("figures: E6 a merged fragment of two submits fails", judged(planted(e6_txt=two)), "not 1 and 1")
    slow = good["e6.txt"].replace("compute merged: median 7.3000", "compute merged: median 99.0000")
    expect("figures: E6 merged slower than unmerged fails", judged(planted(e6_txt=slow)), "slower")
    short = "".join(block(f"e6-elements-{n}", e6_round()) for n in (1, 2))
    expect("figures: E6 two rounds only fails", judged(planted(e6_txt=short)), "three rounds")
    # Ticks
    flat = "".join(ticks_round(n, merged=1900.0) for n in (1, 2, 3)) + TICKS_BENCH
    expect("figures: ticks merged not below unmerged fails", judged(planted(ticks_txt=flat)), "merged per stage")
    slow = "".join(ticks_round(n, merged=9000.0, unmerged=12000.0) for n in (1, 2, 3)) + TICKS_BENCH
    expect("figures: ticks worse than the reference fails", judged(planted(ticks_txt=slow)), "worse than the reference")
    expect("figures: ticks two rounds only fails",
           judged(planted(ticks_txt="".join(ticks_round(n) for n in (1, 2)) + TICKS_BENCH)), "three rounds")
    expect("figures: ticks with no criterion median fails",
           judged(planted(ticks_txt=TICKS_GOOD.replace("detector_frame  ", "detector_gone   "))), "detector_frame")
    expect("figures: ticks fused not below unfused fails",
           judged(planted(ticks_txt=TICKS_GOOD.replace("1.1120 µs", "2.5000 µs"))), "fused is not below")
    expect("figures: ticks a slow detector frame fails",
           judged(planted(ticks_txt=TICKS_GOOD.replace("1.2250 µs", "7.5000 µs"))), "detector_frame is above")
    # E11
    expect("figures: E11 fired with a wrong known run fails",
           judged(planted(e11_txt=grind(lost="fired", known="wrong", same="no") + E11_RESETS)), "no reopened device")
    two_resets = E11_B.replace("end 8, during E11 3", "end 7, during E11 2")
    expect("figures: E11 too few resets fails", judged(planted(e11_txt=two_resets)), "kernel resets")
    moved = E11_B.replace("same_device=yes", "same_device=no", 1)
    expect("figures: E11 a device that changed with no loss fails", judged(planted(e11_txt=moved)), "device changed")
    expect("figures: E11 not run fails on a board",
           judged(planted(e11_gate_txt="not run: no armed hardware watchdog\n")), "not run: no armed")
    # A rehearsal: no layer, the fence not on the dma-buf, no E11. Each says why, and it passes.
    rehearsal = planted(
        e13_gate_txt="skipped: rehearsal: no validation layer\n",
        e13_txt="e13: skipped: rehearsal\n",
        e9_txt=block("e9-fence", "dma-fence: skipped: the fence was signalled at export"),
        e14_txt=block("e14-steady", "dma-steady: skipped: no import") + block("e14-wait", "dma-wait: skipped: no fence"),
        e11_gate_txt="not run: rehearsal on lavapipe\n",
        e11_txt="e11: not run: rehearsal on lavapipe\n",
    )
    # A session of some figures only.
    some = {name: text for name, text in good.items() if not name.startswith(("e6", "ticks", "e11"))}
    some["figures.txt"] = "e13 e9 e14\n"
    expect("figures: a session of E13, E9 and E14 passes on those", judged(some), None)
    expect("figures: a figure the session names with no result fails",
           judged(dict(some, **{"figures.txt": "e13 e9 e14 e6\n"})), "no result file")
    expect("figures: with no figures.txt every figure is judged",
           judged({name: text for name, text in some.items() if name != "figures.txt"}), "no result file")
    expect("figures: a rehearsal with its reasons passes", judged(rehearsal, rehearsal=True), None)
    expect("figures: the same results fail as a board run", judged(rehearsal), "skipped on the board")

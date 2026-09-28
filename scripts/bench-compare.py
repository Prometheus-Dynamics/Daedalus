#!/usr/bin/env python3
"""Compare two criterion output directories and flag regressions.

Usage: scripts/bench-compare.py BASELINE_DIR CURRENT_DIR [--threshold PCT] [--stat median|mean]

Both directories are criterion roots (normally `target/criterion`). Every
`<bench id>/new/estimates.json` is read; the bench id comes from the sibling
`benchmark.json` (`full_id`) when present, otherwise from the relative path.
Benchmarks whose chosen point estimate grew by more than `--threshold` percent
(default 15) are reported as regressions and the script exits 1. A missing or
empty baseline is not an error: the script reports the current numbers and exits 0.

When `GITHUB_STEP_SUMMARY` is set, a Markdown table is appended to it, and each
regression is also emitted as a GitHub `::warning` annotation.
"""

import argparse
import json
import os
import sys
from pathlib import Path


def load(root: Path, stat: str) -> dict[str, float]:
    results: dict[str, float] = {}
    if not root.is_dir():
        return results
    for estimates in sorted(root.rglob("new/estimates.json")):
        bench_dir = estimates.parent
        bench_id = str(bench_dir.parent.relative_to(root))
        meta = bench_dir / "benchmark.json"
        if meta.is_file():
            try:
                bench_id = json.loads(meta.read_text()).get("full_id", bench_id)
            except (OSError, json.JSONDecodeError):
                pass
        try:
            results[bench_id] = float(json.loads(estimates.read_text())[stat]["point_estimate"])
        except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as err:
            print(f"warning: skipping {estimates}: {err}", file=sys.stderr)
    return results


def fmt_ns(ns: float) -> str:
    for unit, scale in (("s", 1e9), ("ms", 1e6), ("µs", 1e3)):
        if ns >= scale:
            return f"{ns / scale:.2f} {unit}"
    return f"{ns:.1f} ns"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("baseline", type=Path)
    parser.add_argument("current", type=Path)
    parser.add_argument("--threshold", type=float, default=15.0, help="regression threshold in percent")
    parser.add_argument("--stat", choices=("median", "mean"), default="median")
    args = parser.parse_args()

    baseline = load(args.baseline, args.stat)
    current = load(args.current, args.stat)
    if not current:
        print(f"error: no criterion estimates under {args.current}", file=sys.stderr)
        return 2

    rows, regressions = [], []
    for bench_id in sorted(current):
        now = current[bench_id]
        before = baseline.get(bench_id)
        if before is None or before <= 0:
            rows.append((bench_id, "-", fmt_ns(now), "new", ""))
            continue
        change = (now - before) / before * 100.0
        flag = ""
        if change > args.threshold:
            flag = "REGRESSION"
            regressions.append((bench_id, change))
        elif change < -args.threshold:
            flag = "improved"
        rows.append((bench_id, fmt_ns(before), fmt_ns(now), f"{change:+.1f}%", flag))
    removed = sorted(set(baseline) - set(current))

    header = ("benchmark", "baseline", "current", "change", "")
    widths = [max(len(str(r[i])) for r in [header, *rows]) for i in range(len(header))]
    for row in [header, *rows]:
        print("  ".join(str(c).ljust(w) for c, w in zip(row, widths)).rstrip())
    for bench_id in removed:
        print(f"(removed) {bench_id}")
    if not baseline:
        print("note: no baseline estimates found; nothing to compare against")

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as out:
            out.write(f"### Criterion {args.stat} vs previous run (threshold {args.threshold:g}%)\n\n")
            if not baseline:
                out.write("_No baseline artifact found; this run becomes the baseline._\n\n")
            out.write("| benchmark | baseline | current | change | |\n|---|---:|---:|---:|---|\n")
            for row in rows:
                out.write("| " + " | ".join(f"`{row[0]}`" if i == 0 else str(c) for i, c in enumerate(row)) + " |\n")
            for bench_id in removed:
                out.write(f"| `{bench_id}` | removed | | | |\n")
    if os.environ.get("GITHUB_ACTIONS"):
        for bench_id, change in regressions:
            print(f"::warning title=Benchmark regression::{bench_id} {args.stat} {change:+.1f}%")

    if regressions:
        print(f"{len(regressions)} benchmark(s) regressed by more than {args.threshold:g}%", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

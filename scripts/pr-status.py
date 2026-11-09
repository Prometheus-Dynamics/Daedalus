#!/usr/bin/env python3
"""Shows which PRs into next are merged, open, ready and blocked."""
import glob, json, os, re, subprocess, sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
briefs = {}
for path in glob.glob(os.path.join(ROOT, "docs", "prs", "*.md")):
    text = open(path).read()
    pid = os.path.basename(path)[:-3]
    title = text.splitlines()[0].split(": ", 1)[1]
    needs = re.search(r"\*\*Needs:\*\* (.*)", text).group(1)
    wave = int(re.search(r"\*\*Wave:\*\* (\d+)", text).group(1))
    briefs[pid] = {
        "title": title,
        "wave": wave,
        "needs": [] if needs == "nothing" else [n.strip() for n in needs.split(",")],
        "done": "**Status:** done" in text,
    }

out = subprocess.run(
    ["gh", "pr", "list", "--base", "next", "--state", "all", "--limit", "1000", "--json", "headRefName,state,number"],
    capture_output=True, text=True, cwd=ROOT,
)
if out.returncode != 0:
    sys.exit(f"gh failed: {out.stderr.strip()}")
state = {}
for pr in json.loads(out.stdout):
    m = re.fullmatch(r"pr/([A-Z][0-9]+)", pr["headRefName"])
    if m and (m.group(1) not in state or pr["state"] == "MERGED"):
        state[m.group(1)] = pr
merged = {p for p, b in briefs.items() if b["done"] or state.get(p, {}).get("state") == "MERGED"}
opened = {p for p in briefs if p not in merged and state.get(p, {}).get("state") == "OPEN"}
order = sorted(briefs, key=lambda p: (briefs[p]["wave"], p[0], int(p[1:])))
ready = [p for p in order if p not in merged and p not in opened and set(briefs[p]["needs"]) <= merged]
blocked = [p for p in order if p not in merged and p not in opened and p not in ready]

print(f"merged {len(merged)}, open {len(opened)}, ready {len(ready)}, blocked {len(blocked)}")
print("\nready")
for p in ready:
    print(f"  {p:4} wave {briefs[p]['wave']:2}  {briefs[p]['title']}")
print("\nopen")
for p in order:
    if p in opened:
        print(f"  {p:4} #{state[p]['number']}  {briefs[p]['title']}")
print("\nblocked")
for p in blocked:
    waiting = ", ".join(n for n in briefs[p]["needs"] if n not in merged)
    print(f"  {p:4} waits on {waiting}")

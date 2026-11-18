#!/usr/bin/env python3
"""Prints the workspace packages a change can reach, one a line, or `all`.

    affected.py [BASE]
    affected.py --self-test

The changed files are what differs from BASE (default: where this branch left origin/next),
committed or not, and the files git doesn't track yet. A file inside a workspace package
reaches that package and every package that depends on it, through any kind of dependency. A
file in no package (a script, a doc, a fixture of its own) could reach anything: `all`.
"""
import json
import os
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def git(*words):
    return subprocess.run(["git", *words], cwd=ROOT, check=True, capture_output=True,
                          text=True).stdout.splitlines()


def changed(base):
    fork = git("merge-base", "HEAD", base)[0]
    return set(git("diff", "--name-only", fork)) | set(git("ls-files", "--others", "--exclude-standard"))


def reach(files, packages):
    """The names of the packages the changed files reach, or None for all. `packages` are
    (directory relative to the root, name, names of its dependencies)."""
    dirs = {directory: name for directory, name, _ in packages}
    users = {name: set() for _, name, _ in packages}
    for _, name, deps in packages:
        for dep in deps:
            if dep in users:
                users[dep].add(name)
    reached = set()
    for file in files:
        owner = max((d for d in dirs if d == "." or file.startswith(d + "/")), key=len, default=None)
        if owner is None or owner == ".":
            return None
        reached.add(dirs[owner])
    todo = list(reached)
    while todo:
        for user in users[todo.pop()]:
            if user not in reached:
                reached.add(user)
                todo.append(user)
    return reached


def self_test():
    packages = [("core/a", "a", []), ("core/b", "b", ["a"]), ("core/c", "c", ["b"]),
                ("core/a/sub", "sub", []), ("tools/d", "d", [])]
    cases = [
        (["core/c/src/lib.rs"], {"c"}),
        (["core/a/src/lib.rs"], {"a", "b", "c"}),
        (["core/a/sub/src/lib.rs"], {"sub"}),
        (["core/ab/x.rs"], None),
        (["scripts/gate.sh"], None),
        (["tools/d/README.md", "core/b/tests/t.rs"], {"d", "b", "c"}),
        ([], set()),
    ]
    failed = 0
    for files, want in cases:
        got = reach(files, packages)
        if got != want:
            print(f"affected self-test: {files} reached {got}, not {want}", file=sys.stderr)
            failed = 1
    if not failed:
        print("affected self-test: every planted change reached what it should")
    return failed


def main():
    if sys.argv[1:] == ["--self-test"]:
        return self_test()
    base = sys.argv[1] if len(sys.argv) > 1 else "origin/next"
    meta = json.loads(subprocess.run(
        ["cargo", "metadata", "--locked", "--format-version", "1", "--no-deps"],
        cwd=ROOT, check=True, capture_output=True, text=True).stdout)
    members = set(meta["workspace_members"])
    packages = [
        (os.path.relpath(os.path.dirname(p["manifest_path"]), ROOT), p["name"],
         [dep["name"] for dep in p["dependencies"]])
        for p in meta["packages"] if p["id"] in members
    ]
    reached = reach(changed(base), packages)
    if reached is None:
        print("all")
    else:
        for name in sorted(reached):
            print(name)
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Lists the build directories that every checkout building in them is finished with, and
deletes them when asked.

    prune-targets.py DIR...            list them
    prune-targets.py --delete DIR...   delete what it lists
    prune-targets.py --self-test       check it against planted directories

Each DIR is a build directory, or a directory holding build directories. Only a build directory
carrying the record scripts/ci.sh leaves in it (`.daedalus-checkout`, the path of the checkout it
belongs to) is looked at; anything else is never touched. It is finished with when its checkout
is gone, or when the checkout has nothing uncommitted, its HEAD is in its origin/next, and no
step has used the directory for two days.
"""
import os
import shutil
import subprocess
import sys
import time

MARK = ".daedalus-checkout"
IDLE = 2 * 24 * 3600


def git(checkout, *words):
    return subprocess.run(["git", "-C", checkout, *words], capture_output=True, text=True)


def why_finished(checkout, idle):
    """Why the checkout is finished with this directory, or None while it isn't."""
    if not os.path.isdir(checkout) or git(checkout, "rev-parse", "--git-dir").returncode != 0:
        return "its checkout is gone"
    if idle < IDLE:
        return None
    if git(checkout, "status", "--porcelain").stdout.strip():
        return None
    if git(checkout, "merge-base", "--is-ancestor", "HEAD", "origin/next").returncode != 0:
        return None
    return f"merged into origin/next, clean, unused for {idle // 86400} days"


def marked(dirs):
    for top in dirs:
        candidates = [top] + sorted(
            os.path.join(top, name) for name in os.listdir(top)
            if os.path.isdir(os.path.join(top, name)))
        for path in candidates:
            if os.path.isfile(os.path.join(path, MARK)):
                yield path


def self_test():
    import tempfile
    failed = 0
    with tempfile.TemporaryDirectory() as top:
        def make(name, checkout, age=0):
            path = os.path.join(top, name)
            os.makedirs(path)
            if checkout is not None:
                mark = os.path.join(path, MARK)
                with open(mark, "w") as file:
                    file.write(checkout + "\n")
                os.utime(mark, (time.time() - age, time.time() - age))
            return path
        live = os.path.join(top, "live")
        os.makedirs(live)
        subprocess.run(["git", "init", "-q", live], check=True)
        with open(os.path.join(live, "work"), "w") as file:
            file.write("uncommitted\n")
        merged = os.path.join(top, "merged")
        os.makedirs(merged)
        for words in (["init", "-q"], ["-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q",
                      "--allow-empty", "-m", "t"], ["update-ref", "refs/remotes/origin/next", "HEAD"]):
            subprocess.run(["git", "-C", merged, *words], check=True)
        gone = os.path.join(top, "gone")
        expect = {
            make("unmarked", None, age=10 * IDLE): True,
            make("gone", gone): False,
            make("dirty", live, age=10 * IDLE): True,
            make("empty", ""): True,
            make("merged-idle", merged, age=10 * IDLE): False,
            make("merged-in-use", merged): True,
        }
        main(["--delete", top], quiet=True)
        for path, kept in expect.items():
            if os.path.isdir(path) != kept:
                print(f"prune self-test: {os.path.basename(path)} was "
                      f"{'deleted' if kept else 'kept'}", file=sys.stderr)
                failed = 1
    if not failed:
        print("prune self-test: only the finished directories were deleted")
    return failed


def main(args, quiet=False):
    if args == ["--self-test"]:
        return self_test()
    delete = "--delete" in args
    dirs = [a for a in args if a != "--delete"]
    if not dirs:
        print(__doc__, file=sys.stderr)
        return 2
    for path in marked(dirs):
        mark = os.path.join(path, MARK)
        idle = int(time.time() - os.path.getmtime(mark))
        with open(mark) as file:
            checkout = file.read().strip()
        reason = why_finished(checkout, idle) if checkout else None
        if reason is None:
            continue
        if not quiet:
            print(f"{path}: {checkout}: {reason}")
        if delete:
            shutil.rmtree(path)
            if not quiet:
                print(f"  deleted {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

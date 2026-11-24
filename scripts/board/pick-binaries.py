#!/usr/bin/env python3
"""Copies the binaries a `cargo test|bench --no-run --message-format=json` built, read on stdin.

    pick-binaries.py tests PREFIX NAME...  each named test binary to PREFIX.NAME (`lib` is a
                                           package's unit tests)
    pick-binaries.py bench NAME DEST       the bench binary NAME to DEST
    pick-binaries.py all DIR               every test binary to DIR/<package>.<target>
    pick-binaries.py --self-test

Fails, naming what is missing, when a named binary was not built or nothing was: a bundle with a
binary left out must never look complete.
"""
import json
import os
import shutil
import sys


def built(stream):
    for line in stream:
        try:
            message = json.loads(line)
        except ValueError:
            continue
        if message.get("executable"):
            yield message


def package_name(package_id):
    """The package's name in a cargo package id: `path+file:///p/dir#name@1.0`, or
    `path+file:///p/name#1.0` when the directory is named after the package."""
    url, _, rest = package_id.rpartition("#")
    if "@" in rest:
        return rest.split("@")[0]
    return url.rstrip("/").rsplit("/", 1)[-1]


def main(args):
    if len(args) >= 3 and args[0] == "tests":
        prefix, wanted = args[1], set(args[2:])
        copied = set()
        for m in built(sys.stdin):
            if m.get("profile", {}).get("test"):
                name = "lib" if "lib" in m["target"]["kind"] else m["target"]["name"]
                if name in wanted:
                    shutil.copy(m["executable"], prefix + "." + name)
                    copied.add(name)
        missing = sorted(wanted - copied)
    elif len(args) == 3 and args[0] == "bench":
        name, dest = args[1], args[2]
        missing = [name]
        for m in built(sys.stdin):
            if "bench" in m["target"]["kind"] and m["target"]["name"] == name:
                shutil.copy(m["executable"], dest)
                missing = []
    elif len(args) == 2 and args[0] == "all":
        dest, count = args[1], 0
        for m in built(sys.stdin):
            if m.get("profile", {}).get("test"):
                # Two packages may each have a test of one name, so the package is in the name.
                package = package_name(m["package_id"])
                shutil.copy(m["executable"], os.path.join(dest, package + "." + m["target"]["name"]))
                count += 1
        missing = [] if count else ["every test binary"]
    else:
        sys.exit(__doc__)
    if missing:
        sys.exit(f"pick-binaries: not built: {', '.join(missing)}")


def self_test():
    import io
    import tempfile
    failed = []
    ids = {"path+file:///x/daedalus-foo#3.0.1-dev": "daedalus-foo",
           "path+file:///x/dir#daedalus-bar@3.0.1-dev": "daedalus-bar"}
    for package_id, name in ids.items():
        if package_name(package_id) != name:
            failed.append(f"{package_id} named {package_name(package_id)}")
    with tempfile.TemporaryDirectory() as tmp:
        exe = os.path.join(tmp, "exe")
        open(exe, "w").close()
        line = json.dumps({"executable": exe, "profile": {"test": True}, "package_id": next(iter(ids)),
                           "target": {"kind": ["test"], "name": "alpha"}})
        cases = [(["tests", tmp + "/t", "alpha"], line, True), (["tests", tmp + "/t", "alpha", "beta"], line, False),
                 (["all", tmp], line, True), (["all", tmp], "not json", False),
                 (["bench", "alpha", tmp + "/b"], line, False)]
        for args, stdin, ok in cases:
            sys.stdin = io.StringIO(stdin + "\n")
            try:
                main(args)
                passed = True
            except SystemExit:
                passed = False
            if passed != ok:
                failed.append(f"{args}: {'passed' if passed else 'failed'}")
        if not os.path.exists(os.path.join(tmp, "daedalus-foo.alpha")):
            failed.append("all: the copy is not named <package>.<target>")
    sys.stdin = sys.__stdin__
    if failed:
        sys.exit("pick-binaries self-test: " + "; ".join(failed))
    print("pick-binaries: self-test ok")


if __name__ == "__main__":
    self_test() if sys.argv[1:] == ["--self-test"] else main(sys.argv[1:])

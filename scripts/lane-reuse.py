#!/usr/bin/env python3
"""Which packages' tests passed before on exactly these inputs, and need not run again.

    lane-reuse.py members                   the workspace's packages, one a line
    lane-reuse.py visible                   for each narrowly keyed package, the paths of the
                                            checkout its key holds (JSON): all its tests see
    lane-reuse.py reads ARTIFACTS           fails naming each narrowly keyed package whose units
                                            compiled from a file of the checkout its key doesn't
                                            hold (cargo's artifact messages; rustc's dep-info)
    lane-reuse.py plan STATE PACKAGE... [-- ARGS...]
                                            key each package's tests (run with ARGS); STATE
                                            gets one line each: PACKAGE, key, `run` or `reused`
    lane-reuse.py runs STATE                the packages STATE says run, one a line
    lane-reuse.py record STATE PACKAGE...   those packages' tests passed: record their keys
    lane-reuse.py status STATE              `passed`, or `passed(K-of-N-reused)`
    lane-reuse.py --explain PACKAGE         each input of the package's key, then the key
    lane-reuse.py --self-test               in a copy of this tree, changes each kind of input
                                            and checks the key moves (or, for a note or a
                                            crate the package doesn't build, that it stays)

A package's key is a hash of everything its tests' result depends on:

- every file, by content, of each workspace package its tests build (normal, build and
  dev-dependencies, on every platform: a superset), and the lockfile's entry of every package
  they build, so a dependency's change reruns only what builds it;
- of the files outside the workspace's packages, those a test's result can depend on: the
  root's (the manifest, the toolchain file), cargo's config, the scripts that run and judge the
  step (RUNNERS, JUDGES), and the files of any package outside the workspace it builds. Any other
  file out there (the other steps' scripts, the lander's tools, the fixtures that are not
  packages) is read only by a package keyed by the whole tree, below, or by no test at all;
- cargo's config files, the toolchain (`rustc -vV`, `cargo -V`), the C compiler, the programs a
  test may start and the Vulkan drivers it may load, and the environment a build or a test
  reads (with the checkout's path written as <root>, so two clones share nothing but keys);
- the step's arguments.

A package whose tests may read outside what it builds is keyed by the whole tree but the
notes TODO.md and AGENTS.md. Such a package's sources name a path out of it: a `..` path, a
walk up from its manifest or from the running binary, cargo itself, or the build directory.
docs/ is read that way (the docs:: tests), so those packages run again on a docs change.

Records are kept in the clone's git directory, `daedalus-lane-records/KEY`, each holding the
hash of the scripts that judged it: a record made by older scripts is never read as a pass.
On CI, or with GATE_LANE_REUSE=0, nothing is reused.
"""
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys

FORMAT = "daedalus-lane-record 1"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# What runs and judges a test step: a change to any reruns every package.
JUDGES = ("scripts/lane-reuse.py", "scripts/ci.sh", "scripts/ci.d/test.sh",
          "scripts/test-parallel.py", "scripts/bounded.py", "scripts/gate.sh")
# What else the test step runs that can change a test's result: a binary's bound, the Vulkan
# driver a test gets, the fixtures' lockfiles, the build directory a test runs from.
RUNNERS = ("scripts/test-slowest.json", "scripts/lavapipe-icd.sh", "scripts/fixture-locks.sh",
           "scripts/seed-target.py")
# Paths the runners and judges name that change no test's result, and why: the self-test checks
# that every other path they name is keyed.
NAMED_NOT_KEYED = {
    "scripts/prune-targets.py": "removes build directories no step uses",
    "scripts/check.sh": "named in prose: it runs a change's packages the same way",
    "scripts/ci.d": "the other steps; the test step is a judge",
    "scripts/check-file-sizes.sh": "the scripts lane's checks, run beside the test step",
    "scripts/check-workspace-deps.sh": "the scripts lane's checks, run beside the test step",
    "testing/boundary/": "the scripts lane's boundary check, run beside the test step",
    "scripts/dev/": "the lander's tools: they run the gate, not a test",
    "scripts/step-reuse.py": "keys the steps outside the test lane; the test step never runs it",
    "scripts/release-check.sh": "run before a release, reading the commits' records: never by a step",
    "testing/fixtures/": "built for and read by packages keyed by the whole tree",
}
# The notes: no build or test outside a whole-tree package reads them.
NOTE = re.compile(r"(\.md$|^docs/|^\.github/|^LICENSE-[^/]*$)")
# Keyed by each package's own entry, below.
LOCKFILE = "Cargo.lock"
# Read by nothing at all.
NEVER_READ = {"TODO.md", "AGENTS.md"}
# Packages keyed by the whole tree whose sources don't show it, and why. The gate finds the others:
# rustc's dep-info names every file a package compiles from (`reads`), and in the gate a narrowly
# keyed package's tests see no more of the checkout than its key (`visible`, scripts/test-parallel.py).
WHOLE_TREE = {}
# The environment a build or a test reads. The gate's own settings (where it keeps its state and
# its builds) are not: none changes what a build makes or a test does.
ENV = re.compile(r"^(CARGO_|RUST|CC|CXX|AR(_|$)|CFLAGS|CXXFLAGS|LDFLAGS|TARGET_|HOST_|ZIG|PKG_CONFIG|"
                 r"SOURCE_DATE_EPOCH|DAEDALUS_|VK_|MESA_|WGPU_|LD_LIBRARY_PATH$|PATH$|HOME$)")
# DAEDALUS_BOUND_STARTED is the path of a step's start mark, new on every run (scripts/bounded.py);
# DAEDALUS_TEST_ARTIFACTS and DAEDALUS_TEST_VISIBLE are where a run keeps its own records.
NOT_IN_KEY = {"CARGO_TARGET_DIR", "CARGO_BUILD_JOBS", "DAEDALUS_WORK", "DAEDALUS_LOGS",
              "DAEDALUS_TARGET_SEED", "DAEDALUS_VVL_DIR", "DAEDALUS_TEST_PACKAGES",
              "DAEDALUS_BOUND_STARTED", "DAEDALUS_TEST_ARTIFACTS", "DAEDALUS_TEST_VISIBLE"}
# Programs a test may start that the machine has, not the repository.
PROGRAMS = ("sh", "bash", "cc", "c++", "gcc", "g++", "clang", "ld", "python3", "node", "java",
            "javac", "cmake", "make", "pkg-config", "zig", "valgrind")
# A path out of the package: see the docstring.
# A `..` that starts a string or follows a `/` in one (`concat!(env!("CARGO_MANIFEST_DIR"),
# "/../docs")`), and trybuild, which runs cargo on the workspace: its manifest names every member.
LEAVES = [re.compile(r'["/]\.\.(/|")'), re.compile(r"current_exe"), re.compile(r"CARGO_TARGET_(TMP)?DIR"),
          re.compile(r'env!\("CARGO"\)|var(_os)?\("CARGO"\)|Command::new\("cargo"\)'),
          re.compile(r"trybuild::")]
UP = re.compile(r"\.parent\(\)|ancestors\(\)")
FROM_MANIFEST = re.compile(r"CARGO_MANIFEST_DIR")
NAMED = re.compile(r'#\[path\s*=\s*"([^"]+)"\]|include_(?:str|bytes)!\(\s*"([^"]+)"')


def digest(data):
    return hashlib.sha256(data).hexdigest()


def file_digest(path):
    try:
        with open(path, "rb") as file:
            return digest(file.read())
    except (FileNotFoundError, IsADirectoryError):
        return "absent"


def run(args):
    try:
        return subprocess.run(args, cwd=ROOT, capture_output=True, text=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        return f"<{args[0]} failed: {error}>"


def git(*args):
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()


def records():
    return os.path.join(git("rev-parse", "--path-format=absolute", "--git-common-dir"), "daedalus-lane-records")


def judges_hash():
    return digest("".join(f"{n}\0{file_digest(os.path.join(ROOT, n))}\0" for n in JUDGES).encode())


class Keys:
    def __init__(self, root=ROOT):
        self.root = root
        listing = subprocess.run(["cargo", "metadata", "--locked", "--format-version", "1"],
                                 cwd=self.root, capture_output=True, text=True)
        if listing.returncode != 0:
            sys.exit(f"lane-reuse.py: cargo metadata failed:\n{listing.stderr}")
        meta = json.loads(listing.stdout)
        self.packages = {p["id"]: p for p in meta["packages"]}
        self.members = set(meta["workspace_members"])
        self.by_name = {self.packages[i]["name"]: i for i in self.members}
        nodes = meta["resolve"]["nodes"]
        self.deps = {n["id"]: [d["pkg"] for d in n["deps"] if any(k["kind"] != "dev" for k in d["dep_kinds"])]
                     for n in nodes}
        self.with_dev = {n["id"]: [d["pkg"] for d in n["deps"]] for n in nodes}
        self.dirs = {i: os.path.relpath(os.path.dirname(self.packages[i]["manifest_path"]), self.root) + "/"
                     for i in self.members}
        self.read_lock()
        out = subprocess.run(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
                             cwd=self.root, capture_output=True, check=True).stdout
        self.listed = sorted({n.decode() for n in out.split(b"\0") if n
                              and os.path.isfile(os.path.join(self.root, n.decode()))})
        self.digests = {}
        self.cache = {}

    def read_lock(self):
        with open(os.path.join(self.root, "Cargo.lock")) as file:
            blocks = file.read().split("[[package]]")[1:]
        self.lock = {}
        for block in blocks:
            fields = dict(l.split(" = ", 1) for l in block.strip().splitlines() if " = " in l)
            self.lock[(fields.get("name", "").strip('"'), fields.get("version", "").strip('"'))] = block.strip()

    def reread(self):
        """Forget what was read of the files and the environment: the self-test changes them."""
        self.read_lock()
        self.digests, self.cache = {}, {}

    def file(self, name):
        if name not in self.digests:
            self.digests[name] = file_digest(os.path.join(self.root, name))
        return self.digests[name]

    def files(self, prefix):
        return [(n, self.file(n)) for n in self.listed if n.startswith(prefix)]

    def built(self, name):
        """Every package the package's tests build, the package itself among them."""
        # Its own dev-dependencies build; theirs do not.
        own = self.by_name[name]
        seen, todo = set(), [own, *self.with_dev[own]]
        while todo:
            current = todo.pop()
            if current not in seen:
                seen.add(current)
                todo.extend(self.deps.get(current, []))
        return seen

    def leaves(self, name):
        """Why the package's sources may read outside what it builds, or None."""
        if name in WHOLE_TREE:
            return f"listed in WHOLE_TREE: {WHOLE_TREE[name]}"
        home = self.dirs[self.by_name[name]]
        for path, _ in self.files(home):
            if not path.endswith(".rs"):
                continue
            with open(os.path.join(self.root, path), errors="replace") as file:
                text = file.read()
            for pattern in LEAVES:
                if pattern.search(text):
                    return f"{path}: {pattern.pattern}"
            if UP.search(text) and FROM_MANIFEST.search(text):
                return f"{path}: walks up from its manifest"
            for match in NAMED.finditer(text):
                said = match.group(1) or match.group(2)
                target = os.path.normpath(os.path.join(os.path.dirname(path), said))
                if match.group(1) and "/mod.rs" not in path and not path.endswith(("lib.rs", "main.rs")):
                    # A #[path] in a non-mod file is relative to the file's own module directory.
                    target = os.path.normpath(os.path.join(path[:-3], said))
                if not (target + "/").startswith(home):
                    return f"{path}: names {said}"
        return None

    def common(self):
        if "common" not in self.cache:
            inputs = [("format", FORMAT), ("judges", judges_hash())]
            here = self.root
            while True:
                for name in ("config.toml", "config"):
                    path = os.path.join(here, ".cargo", name)
                    if os.path.isfile(path):
                        inputs.append((f"cargo config {path.replace(self.root, '<root>', 1)}", file_digest(path)))
                if os.path.dirname(here) == here:
                    break
                here = os.path.dirname(here)
            home = os.environ.get("CARGO_HOME") or os.path.expanduser("~/.cargo")
            inputs.append(("CARGO_HOME", home))
            for name in ("config.toml", "config"):
                inputs.append((f"CARGO_HOME/{name}", file_digest(os.path.join(home, name))))
            for words in (["rustc", "-vV"], ["cargo", "-V"], ["cc", "--version"]):
                inputs.append((" ".join(words), digest(run(words).encode())))
            env = sorted((k, v.replace(self.root, "<root>")) for k, v in os.environ.items()
                         if ENV.match(k) and k not in NOT_IN_KEY and not k.startswith("GATE_"))
            inputs.append(("environment", digest(json.dumps(env).encode())))
            programs = []
            for name in PROGRAMS:
                path = shutil.which(name) or ""
                programs.append((name, path, file_digest(os.path.realpath(path)) if path else ""))
            programs.append(("kernel", os.uname().release))
            inputs.append(("programs", digest(json.dumps(programs).encode())))
            inputs.append(("vulkan drivers", self.drivers()))
            member_dirs = tuple(self.dirs.values())
            outside = [(n, self.file(n)) for n in self.listed
                       if not n.startswith(member_dirs) and not NOTE.search(n) and n != LOCKFILE
                       and ("/" not in n or n.startswith(".cargo/") or n in RUNNERS + JUDGES)]
            inputs.append(("outside the packages", digest(json.dumps(outside).encode())))
            self.cache["common"] = inputs
        return list(self.cache["common"])

    def drivers(self):
        """Each Vulkan driver manifest a test may load, and the library it names."""
        found = []
        for directory in ("/usr/share/vulkan/icd.d", "/etc/vulkan/icd.d"):
            try:
                names = sorted(os.listdir(directory))
            except OSError:
                continue
            for name in names:
                path = os.path.join(directory, name)
                found.append((path, file_digest(path)))
                try:
                    with open(path) as file:
                        library = json.load(file)["ICD"]["library_path"]
                    real = library if os.path.isabs(library) else shutil.which(library) or ""
                    found.append((library, file_digest(os.path.realpath(real)) if real else ""))
                except (OSError, ValueError, KeyError, TypeError):
                    pass
        return digest(json.dumps(found).encode())

    def inputs(self, name, args=()):
        inputs = self.common()
        inputs.append(("package", name))
        inputs.append(("arguments", " ".join(args)))
        why = self.leaves(name)
        if why:
            inputs.append(("reads outside what it builds", why))
            inputs.append(("the tree", digest(json.dumps(
                [(n, self.file(n)) for n in self.listed if n not in NEVER_READ]).encode())))
            return inputs
        for package_id in sorted(self.built(name), key=lambda i: self.packages[i]["id"]):
            package = self.packages[package_id]
            entry = self.lock.get((package["name"], package["version"]), "absent")
            inputs.append((f"Cargo.lock {package['name']} {package['version']}", digest(entry.encode())))
            if package_id in self.members:
                inputs.extend(self.files(self.dirs[package_id]))
            elif package["source"] is None:
                # A path package outside the workspace: its files, wherever in the tree.
                home = os.path.relpath(os.path.dirname(package["manifest_path"]), self.root) + "/"
                inputs.extend(self.files(home))
        return inputs

    def root_files(self):
        """The files at the checkout's root that a narrow key holds."""
        return [n for n in self.listed if "/" not in n and not NOTE.search(n) and n != LOCKFILE]

    def visible(self, name):
        """The paths of the checkout a narrowly keyed package's key holds, and so all its tests may
        see of it: the root's files, cargo's config, and the directories of the packages it builds."""
        found = set(self.root_files()) | {".cargo/"}
        for package_id in self.built(name):
            package = self.packages[package_id]
            if package_id in self.members:
                found.add(self.dirs[package_id])
            elif package["source"] is None:
                home = os.path.relpath(os.path.dirname(package["manifest_path"]), self.root)
                if not home.startswith(".."):
                    found.add(home + "/")
        return sorted(found)

    def reads(self, artifacts):
        """Each narrowly keyed package's unit that compiled from a file of the checkout its key
        doesn't hold, from rustc's dep-info beside the unit: (package, file)."""
        target = os.path.realpath(os.environ.get("CARGO_TARGET_DIR") or os.path.join(self.root, "target"))
        root = os.path.realpath(self.root) + os.sep
        found, seen = [], {}
        for message in artifacts:
            reason = message.get("reason")
            if reason not in ("compiler-artifact", "build-script-executed") \
                    or message["package_id"] not in self.members:
                continue
            if reason == "compiler-artifact" and "custom-build" in message["target"]["kind"]:
                continue
            name = self.packages[message["package_id"]]["name"]
            if name not in seen:
                seen[name] = None if self.leaves(name) else tuple(self.visible(name))
            allowed = seen[name]
            if allowed is None:
                continue
            if reason == "compiler-artifact":
                # cargo runs rustc from the workspace's root: its paths are relative to it.
                paths = [os.path.join(self.root, p) for d in dep_info_files(message) for p in dep_info(d)]
            else:
                # What the build script said it read, relative to its package.
                home = os.path.join(self.root, self.dirs[message["package_id"]])
                paths = [os.path.join(home, p) for p in build_script_reads(message)]
            dirs = tuple(a for a in allowed if a.endswith("/"))
            for path in paths:
                real = os.path.realpath(path)
                if not real.startswith(root) or real.startswith(target + os.sep):
                    continue
                rel = real[len(root):]
                if rel not in allowed and not rel.startswith(dirs):
                    found.append((name, rel))
        return sorted(set(found))

    def key(self, name, args=()):
        return digest(json.dumps(self.inputs(name, args)).encode())


def reuse_off():
    if os.environ.get("CI"):
        return "on CI"
    if os.environ.get("GATE_LANE_REUSE") == "0":
        return "GATE_LANE_REUSE=0"
    if not shutil.which("bwrap"):
        # A narrow key holds only while the package's tests see no more than it (`visible`).
        return "no bwrap to run a narrowly keyed package's tests in"
    return None


def passed_before(key):
    try:
        with open(os.path.join(records(), key)) as file:
            return file.read() == f"{FORMAT} {judges_hash()}\n"
    except OSError:
        return False


def dep_info_files(message):
    """The dep-info files rustc wrote beside a unit's outputs: `name-hash.d`."""
    found = set()
    for path in message.get("filenames", []):
        base, ext = os.path.splitext(os.path.basename(path))
        if ext in (".rlib", ".rmeta", ".so", ".a", ".dylib") and base.startswith("lib"):
            base = base[3:]
        depfile = os.path.join(os.path.dirname(path), base + ".d")
        if os.path.isfile(depfile):
            found.add(depfile)
            continue
        # A binary copied up out of deps/ has no hash in its name: the file in deps/ it is a link
        # of has, and its dep-info beside it.
        deps = os.path.join(os.path.dirname(path), "deps")
        crate = message["target"]["name"].replace("-", "_")
        try:
            names = [n for n in os.listdir(deps) if n.startswith(crate + "-") and "." not in n]
        except OSError:
            continue
        for name in names:
            if os.path.exists(path) and os.path.samefile(path, os.path.join(deps, name)) \
                    and os.path.isfile(os.path.join(deps, name + ".d")):
                found.add(os.path.join(deps, name + ".d"))
    return sorted(found)


def build_script_reads(message):
    """The paths a build script's run declared with `rerun-if-changed`, from its output file."""
    output = os.path.join(os.path.dirname(message.get("out_dir") or ""), "output")
    try:
        with open(output, errors="replace") as file:
            lines = file.read().splitlines()
    except OSError:
        return []
    return [line.split("rerun-if-changed=", 1)[1] for line in lines
            if re.match(r"cargo::?rerun-if-changed=", line)]


def dep_info(depfile):
    """The files a dep-info file says its unit was compiled from."""
    with open(depfile, errors="replace") as file:
        first = file.readline()
    _, _, deps = first.partition(": ")
    return [p.replace("\\ ", " ") for p in re.split(r"(?<!\\) ", deps.strip()) if p]


def read_state(state):
    with open(state) as file:
        return [line.rstrip("\n").split("\t") for line in file if line.strip()]


def plan(state, names, args):
    if not names:
        sys.exit("lane-reuse.py: no package to plan: nothing would be tested")
    keys, off, reused = Keys(), reuse_off(), 0
    with open(state, "w") as file:
        for name in names:
            key = keys.key(name, args)
            use = off is None and passed_before(key)
            reused += use
            file.write(f"{name}\t{key}\t{'reused' if use else 'run'}\n")
    print(f"  reuse: {reused} of {len(names)} packages' tests passed before on these inputs"
          + (f" (nothing reused: {off})" if off else ""))
    return 0


def record(state, names):
    if reuse_off():
        return 0
    rows = {name: key for name, key, _ in read_state(state)}
    os.makedirs(records(), exist_ok=True)
    for name in names:
        if name not in rows:
            sys.exit(f"lane-reuse.py: {name!r} is not in {state}")
        path = os.path.join(records(), rows[name])
        with open(path + ".tmp", "w") as file:
            file.write(f"{FORMAT} {judges_hash()}\n")
        os.replace(path + ".tmp", path)
    return 0


def status(state):
    rows = read_state(state)
    if not rows:
        sys.exit(f"lane-reuse.py: {state} has no package: not passed")
    reused = sum(1 for _, _, how in rows if how == "reused")
    print(f"passed({reused}-of-{len(rows)}-reused)" if reused else "passed")
    return 0


def self_test():
    """Each kind of input changed in a copy of this tree: the key moves, or for a note or a
    package not built, stays."""
    import tempfile

    # Its result is a function of this script alone: it passed before on this one, it passes.
    proved = os.path.join(records(), "self-test-" + file_digest(os.path.abspath(__file__))[:16])
    if reuse_off() is None and os.path.exists(proved):
        print("lane-reuse self-test passed before on this script")
        return 0
    work = tempfile.mkdtemp(prefix="daedalus-keys.")
    copy = os.path.join(work, "tree")
    failures = []
    try:
        keys = Keys()
        for name in keys.listed:
            os.makedirs(os.path.dirname(os.path.join(copy, name)), exist_ok=True)
            shutil.copy2(os.path.join(ROOT, name), os.path.join(copy, name))
        subprocess.run(["git", "init", "-q"], cwd=copy, check=True)
        # The copy's keys, in this process: one listing of the packages, each key read again.
        copied = Keys(copy)
        shims = os.path.join(work, "bin")
        os.makedirs(shims)

        def key(name, extra=None):
            saved = dict(os.environ)
            os.environ.update(extra or {})
            try:
                copied.reread()
                return copied.key(name)
            finally:
                os.environ.clear()
                os.environ.update(saved)

        def touch(name, text="\n// changed by the keys' self-test\n"):
            path = os.path.join(copy, name)
            with open(path) as file:
                before = file.read()
            with open(path, "a") as file:
                file.write(text)
            return lambda: open(path, "w").write(before)

        def replace(name, old):
            path = os.path.join(copy, name)
            with open(path) as file:
                before = file.read()
            assert old in before, f"{name} lacks {old!r}"
            with open(path, "w") as file:
                file.write(before.replace(old, old + "\n# changed", 1))
            return lambda: open(path, "w").write(before)

        def lock_entry(name):
            return next(b for (n, _), b in keys.lock.items() if n == name)

        package = "daedalus-planner"
        assert not keys.leaves(package), f"{package} is keyed by the whole tree: pick another"
        built = {keys.dirs[i] for i in keys.built(package) if i in keys.members}
        home = keys.dirs[keys.by_name[package]]
        dep = next(d for d in sorted(built) if d != home)
        apart = next(d for d in sorted(keys.dirs.values()) if d not in built and not home.startswith(d))
        whole = "daedalus"
        assert keys.leaves(whole), f"{whole} is no longer keyed by the whole tree: pick another"
        registry = lambda ids: sorted(keys.packages[i]["name"] for i in ids
                                      if i not in keys.members and keys.packages[i]["source"])
        locked = registry(keys.built(package))[0]
        unlocked = next(n for n in registry(keys.packages) if n not in registry(keys.built(package)))
        rs = lambda d: next(n for n in keys.listed if n.startswith(d) and n.endswith(".rs"))
        with open(os.path.join(shims, "cc"), "w") as file:
            file.write("#!/bin/sh\nexit 0\n")
        os.chmod(os.path.join(shims, "cc"), 0o755)
        nothing = lambda: (lambda: None)
        cases = [
            (package, "a file of the package", True, lambda: touch(rs(home)), None),
            (package, f"a file of a package it builds ({dep})", True, lambda: touch(rs(dep)), None),
            (package, "a script the test step runs", True,
             lambda: touch("scripts/lavapipe-icd.sh", "\n# changed\n"), None),
            (package, "a test binary's bound", True,
             lambda: replace("scripts/test-slowest.json", "{"), None),
            (package, "a script the test step doesn't run", False,
             lambda: touch("scripts/dev/land.sh", "\n# changed\n"), None),
            (package, "a fixture that is not a package", False,
             lambda: touch("testing/fixtures/dylib/Cargo.toml", "\n# changed\n"), None),
            (whole, "a script the test step doesn't run, for a package that reads the tree", True,
             lambda: touch("scripts/dev/land.sh", "\n# changed\n"), None),
            (package, "the root manifest", True, lambda: touch("Cargo.toml", "\n# changed\n"), None),
            (package, "the toolchain file", True, lambda: touch("rust-toolchain.toml", "\n# changed\n"), None),
            (package, f"the lockfile's entry of a dependency it builds ({locked})", True,
             lambda: replace("Cargo.lock", lock_entry(locked)), None),
            (package, f"the lockfile's entry of one it doesn't build ({unlocked})", False,
             lambda: replace("Cargo.lock", lock_entry(unlocked)), None),
            (package, "the build's environment", True, nothing, {"RUSTFLAGS": "-Cdebuginfo=0"}),
            (package, "the environment a test reads", True, nothing, {"DAEDALUS_SELF_TEST": "1"}),
            (package, "a program a test starts", True, nothing,
             {"PATH": shims + os.pathsep + os.environ.get("PATH", "")}),
            (package, "the gate's own settings", False, nothing, {"GATE_X": "1", "DAEDALUS_LOGS": "/x"}),
            (package, "a note in docs/", False, lambda: touch("docs/development.md", "\nchanged\n"), None),
            (package, "TODO.md", False, lambda: touch("TODO.md", "\nchanged\n"), None),
            (package, f"a package it doesn't build ({apart})", False, lambda: touch(rs(apart)), None),
            (whole, "a note in docs/, for a package that reads the tree", True,
             lambda: touch("docs/development.md", "\nchanged\n"), None),
            (whole, "TODO.md, for a package that reads the tree", False,
             lambda: touch("TODO.md", "\nchanged\n"), None),
        ]
        # Every path the runners and judges name outside the packages is keyed or said not to be.
        for script in RUNNERS + JUDGES:
            with open(os.path.join(ROOT, script), errors="replace") as file:
                for said in sorted(set(re.findall(r"(?:scripts|testing)/[A-Za-z0-9_./-]*[A-Za-z0-9_/]", file.read()))):
                    if (said in RUNNERS + JUDGES or said.startswith(tuple(keys.dirs.values()))
                            or any(said.startswith(n) for n in NAMED_NOT_KEYED)):
                        continue
                    failures.append(f"{script} names {said}: add it to RUNNERS, or to NAMED_NOT_KEYED"
                                    " with why it changes no test's result")
        # What a unit compiled from, by rustc's dep-info, and what a build script said it read:
        # inside a narrow key's paths it passes; outside, the package and the file are named.
        units = os.path.join(work, "units")
        os.makedirs(units)

        def unit(name, sources, said=()):
            crate = name.replace("-", "_")
            with open(os.path.join(units, f"{crate}-1.d"), "w") as file:
                file.write(f"{units}/lib{crate}-1.rlib: {' '.join(sources)}\n")
            out = os.path.join(units, f"{crate}-build", "out")
            os.makedirs(out, exist_ok=True)
            with open(os.path.join(os.path.dirname(out), "output"), "w") as file:
                file.write("".join(f"cargo:rerun-if-changed={p}\n" for p in said))
            package_id = copied.by_name[name]
            return [{"reason": "compiler-artifact", "package_id": package_id,
                     "target": {"kind": ["lib"], "name": crate},
                     "filenames": [f"{units}/lib{crate}-1.rlib"]},
                    {"reason": "build-script-executed", "package_id": package_id, "out_dir": out}]

        up = os.path.relpath(copy, os.path.join(copy, home))
        own = [rs(home)]
        for label, plant, want in [
            ("its own sources", lambda: unit(package, own), []),
            ("a script, by dep-info", lambda: unit(package, own + ["scripts/check.sh"]),
             [(package, "scripts/check.sh")]),
            ("a note, by its build script",
             lambda: unit(package, own, [f"{up}/docs/development.md"]),
             [(package, "docs/development.md")]),
            ("a script, for a package keyed by the whole tree",
             lambda: unit(whole, own + ["scripts/check.sh"]), []),
        ]:
            copied.reread()
            got = copied.reads(plant())
            if got != want:
                failures.append(f"compiling from {label}: named {got}, not {want}")
            else:
                print(f"    compiling from {label}: {'named' if want else 'passed'}")
        # The plant a static scan misses: a test that reads a file of the checkout by a path it is
        # given at run time. Its source names no path, and rustc's dep-info doesn't list a file read
        # at run time, so both keep the package narrow; scripts/test-parallel.py's self-test runs
        # the same read held to a narrow key and sees it fail, named.
        undo = touch(rs(home), '\n#[test]\nfn planted_read() {\n    '
                     'let _ = std::fs::read(std::env::var("PLANT_READ").unwrap());\n}\n')
        try:
            copied.reread()
            why, got = copied.leaves(package), copied.reads(unit(package, own))
        finally:
            undo()
            copied.reread()
        if why is not None or got:
            failures.append(f"a read by a run-time path: the static checks saw it ({why}, {got}); "
                            "the plant no longer stands for what only the sandbox catches")
        else:
            print("    a read by a run-time path: missed by the source scan and the dep-info, as planted")
        bases = {}
        for name, label, moves, change, extra in cases:
            if name not in bases:
                bases[name] = key(name)
            undo = change()
            try:
                now = key(name, extra)
            finally:
                undo()
            if (now != bases[name]) != moves:
                failures.append(f"{name}, {label}: the key {'did not move' if moves else 'moved'}")
            else:
                print(f"    {name}, {label}: {'moved' if moves else 'stayed'}")
        if key(package) != bases[package]:
            failures.append("the same inputs gave two keys")
    finally:
        shutil.rmtree(work, ignore_errors=True)
    if failures:
        print("lane-reuse self-test FAILED:", *failures, sep="\n  ")
        return 1
    os.makedirs(records(), exist_ok=True)
    open(proved, "w").close()
    print("lane-reuse self-test passed")
    return 0


def main(argv):
    if len(argv) >= 2 and argv[0] == "plan":
        # PACKAGE... [-- STEP_ARGS...]
        rest = argv[2:]
        args = rest[rest.index("--") + 1:] if "--" in rest else []
        names = rest[:rest.index("--")] if "--" in rest else rest
        return plan(argv[1], names, args)
    if argv == ["visible"]:
        if reuse_off():
            # Nothing is reused, so nothing needs a test held to its key.
            print("{}")
            return 0
        keys = Keys()
        print(json.dumps({name: keys.visible(name) for name in sorted(keys.by_name)
                          if not keys.leaves(name)}))
        return 0
    if len(argv) == 2 and argv[0] == "reads":
        with open(argv[1]) as file:
            artifacts = [json.loads(line) for line in file if line.startswith("{")]
        found = Keys().reads(artifacts)
        for name, path in found:
            print(f"lane-reuse: {name} compiles from {path}, which its reuse key doesn't hold: name "
                  f"the path in its sources as scripts/lane-reuse.py looks for (LEAVES), or list "
                  f"{name} in WHOLE_TREE there", file=sys.stderr)
        return 1 if found else 0
    if argv == ["members"]:
        print("\n".join(sorted(Keys().by_name)))
        return 0
    if len(argv) == 2 and argv[0] == "runs":
        print("\n".join(name for name, _, how in read_state(argv[1]) if how == "run"))
        return 0
    if len(argv) >= 2 and argv[0] == "record":
        return record(argv[1], argv[2:])
    if len(argv) == 2 and argv[0] == "status":
        return status(argv[1])
    if len(argv) == 2 and argv[0] == "--explain":
        keys = Keys()
        for label, value in keys.inputs(argv[1]):
            print(f"{label}\t{value}")
        print("key", keys.key(argv[1]))
        return 0
    if argv == ["--self-test"]:
        return self_test()
    print(__doc__.strip().splitlines()[2], file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

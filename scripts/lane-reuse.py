#!/usr/bin/env python3
"""Which packages' tests passed before on exactly these inputs, and need not run again.

    lane-reuse.py members                   the workspace's packages, one a line
    lane-reuse.py reads ARTIFACTS           fails naming each narrowly keyed package whose units
                                            compiled from a file of the checkout its key doesn't
                                            hold (cargo's artifact messages; rustc's dep-info)
    lane-reuse.py plan STATE ARTIFACTS [-- ARGS...]
                                            key each workspace package's tests (run with ARGS)
                                            after the build ARTIFACTS lists (cargo's messages);
                                            STATE gets one line each: PACKAGE, key, `run` or
                                            `reused`, and STATE.visible what each narrowly keyed
                                            package's tests may see of the checkout (JSON)
    lane-reuse.py runs STATE                the packages STATE says run, one a line
    lane-reuse.py record STATE PACKAGE...   those packages' tests passed: record their keys
    lane-reuse.py status STATE              `passed`, or `passed(K-of-N-reused)`
    lane-reuse.py --explain PACKAGE ARTIFACTS
                                            each input of the package's key after the build
                                            ARTIFACTS lists, then the key
    lane-reuse.py --self-test               in a copy of this tree, changes each kind of input
                                            and checks the key moves (or, for a note or a
                                            crate the package doesn't build, that it stays)

A package's key is a hash of everything its tests' result depends on, taken after the build:

- what its tests run and load: their binaries, and each program and shared library of a package
  they build (a CARGO_BIN_EXE_ program, a plugin), each by what the loader maps of it (its
  allocated ELF sections, but the build-id note: `loaded_digest`; a file that isn't whole ELF,
  whole), and the output directories of those packages' build scripts. A change that moves no
  code or data (an appended comment, a function nothing calls) reruns nothing; a comment above
  code moves a panic's location, and reruns;
- every file of the checkout its tests may read as they run: of the files of each workspace
  package they build, the root's and cargo's config, every one they weren't compiled from
  (rustc's dep-info, what build scripts declare). In the gate the tests run seeing only those
  (scripts/test-parallel.py, bwrap): a test that reads a file it was compiled from fails, named;
- the lockfile's entry of every package they build (normal, build and dev-dependencies, on every
  platform: a superset);
- of the files outside the workspace's packages, those a test's result can depend on: the
  root's (the manifest, the toolchain file), cargo's config, the scripts that run and judge the
  step (RUNNERS, JUDGES). Any other file out there (the other steps' scripts, the lander's tools,
  the fixtures that are not packages) is read only by a package keyed by the whole tree, below,
  or by no test at all;
- cargo's config files, the toolchain (`rustc -vV`, `cargo -V`), the C compiler, the programs a
  test may start and the Vulkan drivers it may load, and the environment a build or a test
  reads (with the checkout's path written as <root>, so two clones share nothing but keys);
- the step's arguments.

A package whose tests may read outside what it builds is keyed by the whole tree but the
notes TODO.md and AGENTS.md, by content, with the parts above every key holds. Such a package's
sources name a path out of it: a `..` path, a walk up from its manifest or from the running
binary, cargo itself, or the build directory. docs/ is read that way (the docs:: tests), so those
packages run again on a docs change.

Records are kept in the clone's git directory, `daedalus-lane-records/KEY`, each holding the
hash of the scripts that judged it: a record made by older scripts is never read as a pass.
On CI, or with GATE_LANE_REUSE=0, nothing is reused.
"""
import functools
import hashlib
import json
import os
import re
import shutil
import struct
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

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
# Files a narrowly keyed package's tests compile from and also read as they run, and why: they stay
# in what its tests see, keyed by content. The gate finds the others: a test that reads a file it
# was compiled from fails there, named (scripts/test-parallel.py).
READ_AS_IT_RUNS = {
    "daedalus-cpu": ("backends/cpu/tests/fixtures/",
                     "source_fresh compares the generator's output with the checked-in modules, "
                     "which the other tests include!"),
    "daedalus-chip-shared": ("examples/chip_shared/generated/",
                             "generated_files_match_the_planner compares the generator's output with "
                             "the checked-in files, which the other tests include!"),
}
# The environment a build or a test reads. The gate's own settings (where it keeps its state and
# its builds) are not: none changes what a build makes or a test does.
ENV = re.compile(r"^(CARGO_|RUST|CC|CXX|AR(_|$)|CFLAGS|CXXFLAGS|LDFLAGS|TARGET_|HOST_|ZIG|PKG_CONFIG|"
                 r"SOURCE_DATE_EPOCH|DAEDALUS_|VK_|MESA_|WGPU_|LD_LIBRARY_PATH$|PATH$|HOME$)")
# DAEDALUS_BOUND_STARTED is the path of a step's start mark, new on every run (scripts/bounded.py);
# DAEDALUS_TEST_ARTIFACTS, DAEDALUS_TEST_VISIBLE, DAEDALUS_TEST_PLAN and DAEDALUS_TEST_PASSED are
# where a run keeps its own records.
NOT_IN_KEY = {"CARGO_TARGET_DIR", "CARGO_BUILD_JOBS", "DAEDALUS_WORK", "DAEDALUS_LOGS",
              "DAEDALUS_TARGET_SEED", "DAEDALUS_VVL_DIR", "DAEDALUS_TEST_PACKAGES",
              "DAEDALUS_BOUND_STARTED", "DAEDALUS_TEST_ARTIFACTS", "DAEDALUS_TEST_VISIBLE",
              "DAEDALUS_TEST_PLAN", "DAEDALUS_TEST_PASSED"}
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
# An ELF section the loader maps; one with no bytes in the file; the note that hashes the whole file.
SHF_ALLOC, SHT_NOBITS, BUILD_ID = 0x2, 8, b".note.gnu.build-id"
# A path a source names by a literal: a module's file, or a file it includes. Resolved first: one
# inside the package is not a way out of it, whatever `..` it takes to get there.
NAMED = re.compile(r'#\[path\s*=\s*"([^"]+)"\]|include(?:_str|_bytes)?!\(\s*"([^"]+)"')


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


@functools.lru_cache(maxsize=None)
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
        self.loaded = {}

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
        self.digests, self.cache, self.loaded = {}, {}, {}

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
        if ("leaves", name) not in self.cache:
            self.cache[("leaves", name)] = self.scan(name)
        return self.cache[("leaves", name)]

    def scan(self, name):
        if name in WHOLE_TREE:
            return f"listed in WHOLE_TREE: {WHOLE_TREE[name]}"
        home = self.dirs[self.by_name[name]]
        for path, _ in self.files(home):
            if not path.endswith(".rs"):
                continue
            with open(os.path.join(self.root, path), errors="replace") as file:
                text = file.read()
            for match in NAMED.finditer(text):
                said = match.group(1) or match.group(2)
                target = os.path.normpath(os.path.join(os.path.dirname(path), said))
                if match.group(1) and "/mod.rs" not in path and not path.endswith(("lib.rs", "main.rs")):
                    # A #[path] in a non-mod file is relative to the file's own module directory.
                    target = os.path.normpath(os.path.join(path[:-3], said))
                if not (target + "/").startswith(home):
                    return f"{path}: names {said}"
            # Every literal path left the package nowhere: the blunt checks read the rest.
            text = NAMED.sub("", text)
            for pattern in LEAVES:
                if pattern.search(text):
                    return f"{path}: {pattern.pattern}"
            if UP.search(text) and FROM_MANIFEST.search(text):
                return f"{path}: walks up from its manifest"
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

    def inputs(self, name, units, args=()):
        """Each input of the package's key: from its build (`built_inputs`, of `units`, cargo's
        messages), or for a package that may read outside what it builds, the whole tree."""
        why = self.leaves(name)
        if not why:
            return self.built_inputs(name, units, args)
        return self.common() + [
            ("package", name), ("arguments", " ".join(args)), ("reads outside what it builds", why),
            ("the tree", digest(json.dumps([(n, self.file(n)) for n in self.listed
                                            if n not in NEVER_READ]).encode()))]

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

    def key(self, name, units, args=()):
        return digest(json.dumps(self.inputs(name, units, args)).encode())

    def closure_units(self, name, units):
        """The units of a build (cargo's artifact messages) that the package's tests are made of:
        every unit of each package they build but the others' tests, and its build scripts' runs."""
        own, closure = self.by_name[name], self.built(name)
        return [m for m in units if m.get("package_id") in closure
                and (m.get("reason") == "build-script-executed"
                     or m.get("reason") == "compiler-artifact"
                     and (not m["profile"]["test"] or m["package_id"] == own))]

    def ran(self, name, units):
        """What of the build the package's tests run or load: their own binaries, and each program
        and shared library of a package they build (a CARGO_BIN_EXE_ program, a plugin), as
        (label, path); and each build script's output directory, which a test may read (OUT_DIR)."""
        programs, outputs = [], []
        for m in self.closure_units(name, units):
            package = self.packages[m["package_id"]]
            if m["reason"] == "build-script-executed":
                if package["source"] is None and m.get("out_dir"):
                    outputs.append((package["name"], m["out_dir"]))
                continue
            kinds, test = m["target"]["kind"], m["profile"]["test"]
            if m.get("executable") and (test or "bin" in kinds):
                paths = [m["executable"]]
            elif {"cdylib", "dylib"} & set(kinds):
                paths = [f for f in m.get("filenames", []) if f.endswith((".so", ".dylib", ".dll"))]
            else:
                continue
            for path in paths:
                label = f"{package['name']} {'test ' if test else ''}{'/'.join(kinds)} {m['target']['name']}"
                programs.append((label + os.path.splitext(path)[1], path))
        return programs, outputs

    def load(self, paths):
        """Each file's `loaded_digest`, side by side: the step's binaries are hundreds of MB."""
        todo = sorted(set(paths) - set(self.loaded))
        with ThreadPoolExecutor(max_workers=8) as pool:
            self.loaded.update(zip(todo, pool.map(loaded_digest, todo)))
        return [self.loaded[p] for p in paths]

    def unit_sources(self, message):
        """The files of the checkout a unit compiled from, by rustc's dep-info, or a build
        script's run said it read (a directory ends in /)."""
        if "sources" not in message:
            root = os.path.realpath(self.root) + os.sep
            if message["reason"] == "compiler-artifact":
                # cargo runs rustc from the workspace's root: its paths are relative to it.
                paths = [os.path.join(self.root, p) for d in dep_info_files(message) for p in dep_info(d)]
            else:
                home = os.path.dirname(self.packages[message["package_id"]]["manifest_path"])
                paths = [os.path.join(home, p) for p in build_script_reads(message)]
            found = set()
            for path in paths:
                real = os.path.realpath(path)
                if real.startswith(root):
                    found.add(real[len(root):] + ("/" if os.path.isdir(real) else ""))
            message["sources"] = found
        return message["sources"]

    def compiled(self, name, units):
        """The files of the checkout that each package the package's tests build compiled from,
        its tests among them: what the tests get of them is in what they run, or nowhere."""
        closure = self.built(name)
        return set().union(*(self.unit_sources(m) for m in units if m.get("package_id") in closure
                             and m.get("reason") in ("compiler-artifact", "build-script-executed")))

    def runtime(self, name, units):
        """What a narrowly keyed package's tests may read of the checkout as they run: of the paths
        its key holds (`visible`), every file its tests weren't compiled from, and the directories
        those paths are, made empty. A file compiled from is keyed by what it compiled to, so the
        tests don't see it: one that reads it as it runs fails, named (scripts/test-parallel.py)."""
        held = self.visible(name)
        dirs = tuple(h for h in held if h.endswith("/"))
        compiled = self.compiled(name, units)
        gone = tuple(c for c in compiled if c.endswith("/"))
        read = READ_AS_IT_RUNS.get(name, ("\0",))[0]
        files = [n for n in self.listed if (n in held or n.startswith(dirs))
                 and (n.startswith(read) or n not in compiled and not n.startswith(gone))]
        return files, list(dirs)

    def built_inputs(self, name, units, args=()):
        """A narrowly keyed package's key from its build: what its tests run and load, by what the
        loader maps of each (`loaded_digest`), and every file of the checkout they may read as they
        run (`runtime`), with the parts every key holds and the lockfile's entries."""
        inputs = self.common()
        inputs += [("package", name), ("arguments", " ".join(args)), ("keyed by", "its build")]
        for package_id in sorted(self.built(name)):
            package = self.packages[package_id]
            entry = self.lock.get((package["name"], package["version"]), "absent")
            inputs.append((f"Cargo.lock {package['name']} {package['version']}", digest(entry.encode())))
        programs, outputs = self.ran(name, units)
        inputs += sorted(zip((l for l, _ in programs), self.load([p for _, p in programs])))
        for package, out_dir in outputs:
            for here, _, names in os.walk(out_dir):
                inputs += sorted((f"{package} OUT_DIR {os.path.relpath(os.path.join(here, n), out_dir)}",
                                  file_digest(os.path.join(here, n))) for n in names)
        files, _ = self.runtime(name, units)
        inputs += [(n, self.file(n)) for n in files]
        return inputs


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


def loaded_digest(path):
    """What the loader maps of an ELF file: each allocated (SHF_ALLOC) section's name, type, flags,
    address and size, and its bytes but for a NOBITS one, which has none; not the build-id note, a
    hash of the whole file, debug info and all. A comment that moves no code moves none of it; one
    that moves a line does (a panic's location is in the data). A file that isn't a whole 64-bit
    little-endian ELF file (truncated, malformed, or something else) is hashed whole."""
    try:
        with open(path, "rb") as file:
            size = os.fstat(file.fileno()).st_size
            head = file.read(64)
            if len(head) < 64 or head[:4] != b"\x7fELF" or head[4:6] != b"\x02\x01":
                raise ValueError("not a 64-bit little-endian ELF file")
            table_at, = struct.unpack_from("<Q", head, 0x28)
            entry, count, names_index = struct.unpack_from("<HHH", head, 0x3A)
            if entry < 64 or not count or names_index >= count or table_at + count * entry > size:
                raise ValueError("its section table is outside it")
            file.seek(table_at)
            table = file.read(count * entry)
            sections = [struct.unpack_from("<IIQQQQ", table, i * entry) for i in range(count)]
            names_at, names_size = sections[names_index][4:6]
            if names_at + names_size > size:
                raise ValueError("its section names are outside it")
            file.seek(names_at)
            names = file.read(names_size)
            hashed = hashlib.sha256()
            for name_at, kind, flags, address, offset, length in sections:
                if not flags & SHF_ALLOC:
                    continue
                end = names.find(b"\0", name_at)
                if end < 0:
                    raise ValueError("a section's name is outside the section names")
                if names[name_at:end] == BUILD_ID:
                    continue
                hashed.update(names[name_at:end] + b"\0" + struct.pack("<IQQQ", kind, flags, address, length))
                if kind != SHT_NOBITS:
                    if offset + length > size:
                        raise ValueError("a section's bytes are outside it")
                    file.seek(offset)
                    hashed.update(file.read(length))
            return "loaded " + hashed.hexdigest()
    except (OSError, ValueError, struct.error):
        return "whole " + file_digest(path)


def read_state(state):
    with open(state) as file:
        return [line.rstrip("\n").split("\t") for line in file if line.strip()]


def read_units(artifacts):
    with open(artifacts) as file:
        return [json.loads(line) for line in file if line.startswith("{")]


def plan(state, artifacts, args):
    """Key every workspace package's tests: a narrowly keyed one by its build (`built_inputs`), one
    keyed by the whole tree by its sources. STATE.visible gets what each narrowly keyed package's
    tests may see of the checkout: the same files its key holds."""
    keys, off, reused = Keys(), reuse_off(), 0
    units = read_units(artifacts)
    names = sorted(keys.by_name)
    keys.load([p for n in names if not keys.leaves(n) for _, p in keys.ran(n, units)[0]])
    held = {}
    with open(state, "w") as file:
        for name in names:
            narrow = not keys.leaves(name)
            key = keys.key(name, units, args)
            use = off is None and passed_before(key)
            reused += use
            file.write(f"{name}\t{key}\t{'reused' if use else 'run'}\n")
            if narrow and off is None:
                files, dirs = keys.runtime(name, units)
                held[name] = {"bind": files, "make": dirs}
    with open(state + ".visible", "w") as file:
        json.dump(held, file)
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


# The planted test's source: a panic whose location is in the binary's data.
PLANTED_TEST = """fn main() {
    if std::env::args().count() > 100 {
        panic!("planted");
    }
}
"""


def self_test():
    """Each kind of input changed in a copy of this tree: the key moves, or for a note or a
    package not built, stays."""
    import tempfile

    work = tempfile.mkdtemp(prefix="daedalus-keys.")
    copy = os.path.join(work, "tree")
    failures = []
    try:
        keys = Keys()
        for name in keys.listed:
            os.makedirs(os.path.dirname(os.path.join(copy, name)), exist_ok=True)
            shutil.copy2(os.path.join(ROOT, name), os.path.join(copy, name))
        package = "daedalus-planner"
        assert not keys.leaves(package), f"{package} is keyed by the whole tree: pick another"
        home = keys.dirs[keys.by_name[package]]
        # The package's planted test, built by rustc as cargo would, and a fixture it reads.
        located, fixture = f"{home}tests/planted_location.rs", f"{home}tests/planted.txt"
        for name, text in ((located, PLANTED_TEST), (fixture, "read by the planted test\n")):
            os.makedirs(os.path.dirname(os.path.join(copy, name)), exist_ok=True)
            with open(os.path.join(copy, name), "w") as file:
                file.write(text)
        subprocess.run(["git", "init", "-q"], cwd=copy, check=True)
        # The copy's keys, in this process: one listing of the packages, each key read again.
        copied = Keys(copy)
        shims = os.path.join(work, "bin")
        os.makedirs(shims)
        units = os.path.join(work, "units")
        os.makedirs(units)
        built = {keys.dirs[i] for i in keys.built(package) if i in keys.members}
        dep = next(d for d in sorted(built) if d != home)
        dep_id = next(i for i in copied.members if copied.dirs[i] == dep)
        crate = lambda i: copied.packages[i]["name"].replace("-", "_")
        test, plugin = os.path.join(units, "planted"), os.path.join(units, "libplanted_plugin.so")

        def build(out=test):
            subprocess.run(["rustc", "--edition", "2021", "-C", "debuginfo=2", "-o", out,
                            os.path.join(copy, located)], cwd=copy, check=True)

        def compiled_from(path, sources):
            with open(path + ".d", "w") as file:
                file.write(f"{path}: {' '.join(os.path.join(copy, n) for n in sources)}\n")

        build()
        build(plugin)
        shutil.copyfile(os.path.realpath(shutil.which("sh")), os.path.join(work, "other"))
        rs_of = lambda d: [n for n in copied.listed if n.startswith(d) and n.endswith(".rs")]
        compiled_from(test, rs_of(home))
        compiled_from(os.path.join(units, f"{crate(dep_id)}-1"), rs_of(dep))
        # cargo's messages for the build: the package's test, a library and a plugin of a package
        # it builds.
        planted = [
            {"reason": "compiler-artifact", "package_id": copied.by_name[package],
             "target": {"kind": ["test"], "name": "planted"}, "profile": {"test": True},
             "executable": test, "filenames": [test]},
            {"reason": "compiler-artifact", "package_id": dep_id,
             "target": {"kind": ["lib"], "name": crate(dep_id)}, "profile": {"test": False},
             "executable": None, "filenames": [os.path.join(units, f"lib{crate(dep_id)}-1.rlib")]},
            {"reason": "compiler-artifact", "package_id": dep_id,
             "target": {"kind": ["cdylib"], "name": "planted_plugin"}, "profile": {"test": False},
             "executable": None, "filenames": [plugin]},
        ]

        once = []

        def key(name, extra=None, fresh=True):
            saved = dict(os.environ)
            os.environ.update(extra or {})
            try:
                copied.reread()
                if not fresh:
                    once[:] = once or [copied.common()]
                    copied.cache["common"] = once[0]
                return copied.key(name, [dict(m) for m in planted])
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

        def replace_text(name, old, new):
            path = os.path.join(copy, name)
            with open(path) as file:
                before = file.read()
            with open(path, "w") as file:
                file.write(before.replace(old, new, 1))
            return lambda: open(path, "w").write(before)

        def lock_entry(name):
            return next(b for (n, _), b in keys.lock.items() if n == name)

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
        # Another narrowly keyed package, which doesn't build the plugin's package.
        aside = next(n for n in sorted(keys.by_name) if not keys.leaves(n)
                     and n != package and dep_id not in copied.built(n))

        def rebuilt(change):
            def plant():
                undo = change()
                build()
                return lambda: (undo(), build())
            return plant

        def swap(path, other):
            def plant():
                with open(path, "rb") as file:
                    before = file.read()
                shutil.copyfile(other, path)
                return lambda: open(path, "wb").write(before)
            return plant

        def read_as_it_runs(change):
            def plant():
                READ_AS_IT_RUNS[package] = (located, "the self-test's plant")
                undo = change()
                return lambda: (undo(), READ_AS_IT_RUNS.pop(package))
            return plant

        def truncate():
            with open(test, "rb") as file:
                before = file.read()
            with open(test, "wb") as file:
                file.write(before[:len(before) // 2])
            return lambda: open(test, "wb").write(before)

        cases = [
            (package, "a fixture its tests read", True, lambda: touch(fixture, "changed\n"), None),
            (aside, "a fixture of a package it doesn't build", False, lambda: touch(fixture, "changed\n"), None),
            (package, f"a file of a package it builds, not compiled from ({dep}Cargo.toml)", True,
             lambda: touch(f"{dep}Cargo.toml", "\n# changed\n"), None),
            (package, "a source it compiled from, what it runs unchanged", False, lambda: touch(rs(home)), None),
            (package, f"a source of a package it builds, what it runs unchanged ({dep})", False,
             lambda: touch(rs(dep)), None),
            (package, "a plugin a package it builds makes", True,
             swap(plugin, os.path.join(work, "other")), None),
            (aside, "a plugin of a package it doesn't build", False,
             swap(plugin, os.path.join(work, "other")), None),
            (package, "a comment above code (a panic's location moves)", True,
             rebuilt(lambda: replace_text(located, "fn main", "// a comment\nfn main")), None),
            (package, "a comment after the code", False,
             rebuilt(lambda: touch(located, "// a comment\n")), None),
            (package, "a comment after the code, in a file it reads as it runs too", True,
             read_as_it_runs(rebuilt(lambda: touch(located, "// a comment\n"))), None),
            (package, "its test binary truncated", True, truncate, None),
        ]
        # The plants above change none of what every key holds (`common`): it is read once for them.
        built_only = len(cases)
        cases += [
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
        units = os.path.join(work, "reads")
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
        # A path a source names by a literal, `..` and all: inside the package it keeps the package
        # narrow; out of it, it is named.
        here = os.path.dirname(rs(home))
        for label, said, want in [("inside the package", os.path.relpath(f"{home}Cargo.toml", here), None),
                                  ("out of it", os.path.relpath("Cargo.lock", here), "names")]:
            undo = touch(rs(home), f'\nconst _: &[u8] = include_bytes!("{said}");\n')
            try:
                copied.reread()
                why = copied.leaves(package)
            finally:
                undo()
                copied.reread()
            if (why is None) != (want is None) or want and want not in why:
                failures.append(f"include_bytes! of {said}, {label}: {why}")
            else:
                print(f"    include_bytes! of {said}, {label}: {why or 'narrow'}")
        bases = {}
        for at, (name, label, moves, change, extra) in enumerate(cases):
            if name not in bases:
                bases[name] = key(name)
            undo = change()
            try:
                now = key(name, extra, fresh=at >= built_only)
            finally:
                undo()
            if (now != bases[name]) != moves:
                failures.append(f"{name}, {label}: the key {'did not move' if moves else 'moved'}")
            else:
                print(f"    {name}, {label}: {'moved' if moves else 'stayed'}")
        if key(package) != bases[package]:
            failures.append("the same inputs gave two keys")
        # A file that isn't whole ELF is hashed whole: two truncations of a binary are two keys.
        with open(test, "rb") as file:
            whole = file.read()
        cut = os.path.join(work, "cut")
        seen = set()
        for size in (len(whole) // 2, len(whole) // 3, 63):
            with open(cut, "wb") as file:
                file.write(whole[:size])
            got = loaded_digest(cut)
            seen.add(got)
            if got != "whole " + file_digest(cut):
                failures.append(f"a binary cut to {size} bytes: {got}, not its whole file's hash")
        if len(seen) != 3:
            failures.append("three cuts of a binary gave fewer than three hashes")
        else:
            print("    a binary cut short: hashed whole, each cut its own hash")
    finally:
        shutil.rmtree(work, ignore_errors=True)
    if failures:
        print("lane-reuse self-test FAILED:", *failures, sep="\n  ")
        return 1
    print("lane-reuse self-test passed")
    return 0


def main(argv):
    if len(argv) >= 3 and argv[0] == "plan" and (len(argv) == 3 or argv[3] == "--"):
        return plan(argv[1], argv[2], argv[4:])
    if len(argv) == 2 and argv[0] == "reads":
        found = Keys().reads(read_units(argv[1]))
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
    if len(argv) == 3 and argv[0] == "--explain":
        inputs = Keys().inputs(argv[1], read_units(argv[2]))
        for label, value in inputs:
            print(f"{label}\t{value}")
        print("key", digest(json.dumps(inputs).encode()))
        return 0
    if argv == ["--self-test"]:
        return self_test()
    print(__doc__.strip().splitlines()[2], file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

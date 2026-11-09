#!/usr/bin/env python3
"""The rules behind check-boundaries.sh. Each rule appends (rule, path, line, detail)."""
import argparse, glob, json, os, re, subprocess, sys

GLOB_FOLDERS = ["core", "backends", "languages", "service", "testing/crates", "examples"]
HARDWARE_WORDS = {"gpu", "wgpu", "shader", "texture", "npu", "frame", "image", "pixel", "camera", "mcu"}
UNSAFE_ALLOWED = ("core/abi", "core/capi", "core/loader", "core/runner")
HEAP_FREE_TARGETS = ["thumbv7em-none-eabihf", "thumbv6m-none-eabi", "riscv32imac-unknown-none-elf"]
PRODUCT_NAMES = re.compile(r"\b(lemnos|eidos|ariadne|gaia|orion|helios)\b", re.I)
HEAP_TOKENS = re.compile(r"\b(alloc::|std::|Vec<|Box<|extern\s+crate\s+alloc)|\bString::|(?<!:):\s*String\b|->\s*String\b|[<&(]\s*String\b")
FEATURE_GATE = re.compile(r'#\[cfg\((?:test|(?:any\()?feature\s*=\s*"(?:alloc|std)")')

ITEM_KEYWORD = re.compile(r"\b(fn|impl|mod|struct|enum|trait|use|const|static|type)\b")
GATED_FILE = re.compile(r'#!\[cfg\(feature\s*=\s*"(?:alloc|std)"\)\]')

violations = []


def report(rule, path, line, detail):
    violations.append((rule, path, line, detail))


def rel(root, path):
    return os.path.relpath(path, root)


def blank_comments_and_strings(text, keep_strings=False):
    """Replaces comments, and string contents unless kept, with spaces. Newlines and offsets stay."""
    out, i, n = [], 0, len(text)
    while i < n:
        c, d = text[i], text[i + 1] if i + 1 < n else ""
        if c == "/" and d == "/":
            j = text.find("\n", i)
            j = n if j < 0 else j
            out.append(" " * (j - i))
            i = j
        elif c == "/" and d == "*":
            depth, j = 1, i + 2
            while j < n and depth:
                if text.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif text.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            out.append(re.sub(r"[^\n]", " ", text[i:j]))
            i = j
        elif (m := re.match(r'(?<![\w])b?r(#*)"', text[i:i + 12])) and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")):
            close = '"' + m.group(1)
            j = text.find(close, i + m.end())
            j = n if j < 0 else j + len(close)
            out.append(text[i:j] if keep_strings else re.sub(r"[^\n]", " ", text[i:j]))
            i = j
        elif c == '"':
            j = i + 1
            while j < n and text[j] != '"':
                j += 2 if text[j] == "\\" else 1
            out.append(text[i:j + 1] if keep_strings else '"' + re.sub(r"[^\n]", " ", text[i + 1:j]) + '"')
            i = j + 1
        elif c == "'" and (m := re.match(r"'(\\.[^']*|[^\\'])'", text[i:i + 14])):
            out.append(" " * m.end())
            i += m.end()
        else:
            out.append(c)
            i += 1
    return "".join(out)


def blank_gated_items(text):
    """Blanks comments and strings, and each item under #[cfg(test)] or a cfg on alloc or std."""
    code = blank_comments_and_strings(text)
    out = list(code)
    for m in FEATURE_GATE.finditer(blank_comments_and_strings(text, keep_strings=True)):
        depth, j = 0, code.index("]", m.end()) + 1
        while j < len(code):
            c = code[j]
            if c in "{([":
                depth += 1
            elif c in "})]":
                depth -= 1
                if depth == 0 and c == "}":
                    j += 1
                    break
            elif depth == 0 and (c == ";" or (c == "," and not ITEM_KEYWORD.search(code, m.end(), j))):
                j += 1
                break
            j += 1
        for k in range(m.start(), min(j, len(code))):
            if code[k] != "\n":
                out[k] = " "
    return "".join(out)


def rust_files(base):
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = [d for d in dirnames if d not in ("target", ".git")]
        for name in filenames:
            if name.endswith(".rs"):
                yield os.path.join(dirpath, name)


def cargo_metadata(root):
    done = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1",
         "--manifest-path", os.path.join(root, "Cargo.toml")],
        capture_output=True, text=True)
    if done.returncode != 0:
        report("cargo-metadata", "Cargo.toml", 0, done.stderr.strip())
        return {"packages": []}
    return json.loads(done.stdout)


def check_core_deps(root, packages):
    core_root = os.path.realpath(os.path.join(root, "core")) + os.sep
    for pkg in packages:
        manifest = os.path.realpath(pkg["manifest_path"])
        if not manifest.startswith(core_root):
            continue
        for dep in pkg["dependencies"]:
            path = dep.get("path")
            if not path:
                continue
            target = rel(os.path.realpath(root), os.path.realpath(path))
            if target.split(os.sep)[0] in ("backends", "languages", "service"):
                report("core-deps", rel(root, manifest), 0, f"depends on {target}")


def check_no_lemnos(root, packages):
    for pkg in packages:
        for dep in pkg["dependencies"]:
            source = dep.get("source") or ""
            if dep["name"].lower().startswith("lemnos") or "lemnos" in source.lower():
                report("no-lemnos", rel(root, pkg["manifest_path"]), 0, f"depends on {dep['name']}")


def check_core_names(root):
    core = os.path.join(root, "core")
    for dirpath, dirnames, filenames in os.walk(core):
        dirnames[:] = [d for d in dirnames if d != "target"]
        for name in dirnames + filenames:
            stem = name.rsplit(".", 1)[0]
            if {p.lower() for p in re.split(r"[_-]", stem)} & HARDWARE_WORDS:
                report("core-names", rel(root, os.path.join(dirpath, name)), 0, f"named {name}")
    for path in rust_files(core):
        code = blank_comments_and_strings(open(path, encoding="utf-8").read())
        for lineno, line in enumerate(code.splitlines(), 1):
            for ident in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", line):
                if {p.lower() for p in ident.split("_")} & HARDWARE_WORDS:
                    report("core-names", rel(root, path), lineno, f"identifier {ident}")


def unsafe_allowed(crate):
    if crate in UNSAFE_ALLOWED:
        return True
    parts = crate.split("/")
    return len(parts) == 2 and parts[0] in ("backends", "languages") and parts[1].endswith("-sys")


def crate_dirs(root):
    for folder in GLOB_FOLDERS:
        for path in sorted(glob.glob(os.path.join(root, folder, "*"))):
            if os.path.isdir(path):
                yield rel(root, path)


def check_unsafe(root):
    for crate in crate_dirs(root):
        base = os.path.join(root, crate)
        allowed = unsafe_allowed(crate)
        for path in rust_files(base):
            source = open(path, encoding="utf-8").read()
            code = blank_comments_and_strings(source)
            lines = source.splitlines()
            for lineno, line in enumerate(code.splitlines(), 1):
                if not re.search(r"\bunsafe\b", line):
                    continue
                if re.search(r"\bunsafe\s+(extern\s+\"\s*\"\s+)?fn\s*\(", line) or "unsafe_code" in line:
                    continue
                if not allowed:
                    report("unsafe", rel(root, path), lineno, "unsafe outside the allowed crates")
                elif not any("SAFETY" in l or "# Safety" in l for l in lines[max(0, lineno - 4):lineno]):
                    report("unsafe", rel(root, path), lineno, "no SAFETY comment within three lines above")
        lib = os.path.join(base, "src", "lib.rs")
        if not allowed and os.path.exists(lib) and "#![forbid(unsafe_code)]" not in open(lib, encoding="utf-8").read():
            report("unsafe", rel(root, lib), 1, "missing #![forbid(unsafe_code)]")


def check_heap_free(root, packages, build):
    for pkg in packages:
        marker = (pkg.get("metadata") or {}).get("daedalus") or {}
        if not marker.get("heap_free"):
            continue
        src = os.path.join(os.path.dirname(pkg["manifest_path"]), "src")
        clean = True
        for path in rust_files(src):
            text = open(path, encoding="utf-8").read()
            if GATED_FILE.search(blank_comments_and_strings(text, keep_strings=True)):
                continue
            code = blank_gated_items(text)
            for lineno, line in enumerate(code.splitlines(), 1):
                if m := HEAP_TOKENS.search(line):
                    clean = False
                    report("heap-free", rel(root, path), lineno, f"uses {m.group(0)}")
        if not (clean and build):
            continue
        for target in HEAP_FREE_TARGETS:
            done = subprocess.run(
                ["cargo", "build", "-q", "--manifest-path", pkg["manifest_path"], "-p", pkg["name"],
                 "--no-default-features", "--target", target],
                capture_output=True, text=True)
            if done.returncode != 0:
                report("heap-free", rel(root, pkg["manifest_path"]), 0, f"does not build for {target}: {done.stderr.strip()}")


def check_glob_folders(root):
    for folder in GLOB_FOLDERS:
        base = os.path.join(root, folder)
        if not os.path.isdir(base):
            continue
        for name in sorted(os.listdir(base)):
            path = os.path.join(base, name)
            if os.path.isdir(path):
                if not os.path.exists(os.path.join(path, "Cargo.toml")):
                    report("glob-folders", rel(root, path), 0, "directory is not a crate")
            elif name != "README.md":
                report("glob-folders", rel(root, path), 0, "only README.md may sit directly here")


def tracked_files(root):
    done = subprocess.run(["git", "-C", root, "ls-files", "-z", "--cached", "--others", "--exclude-standard", "."],
                          capture_output=True, text=True)
    return [p for p in done.stdout.split("\0") if p]


def names_exempt(path):
    return (path.startswith("docs/") or path in ("TODO.md", "AGENTS.md")
            or path == "scripts/boundaries.py" or path.startswith("testing/boundary/"))


def check_names(root):
    for path in tracked_files(root):
        if names_exempt(path):
            continue
        full = os.path.join(root, path)
        if not os.path.isfile(full):
            continue
        try:
            text = open(full, encoding="utf-8").read()
        except UnicodeDecodeError:
            continue
        for lineno, line in enumerate(text.splitlines(), 1):
            if m := PRODUCT_NAMES.search(line):
                report("names", path, lineno, f"names {m.group(0)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
    parser.add_argument("--no-build", action="store_true", help="skip the heap-free target builds")
    args = parser.parse_args()
    root = os.path.abspath(args.root)
    packages = cargo_metadata(root)["packages"]
    check_core_deps(root, packages)
    check_no_lemnos(root, packages)
    check_core_names(root)
    check_unsafe(root)
    check_heap_free(root, packages, not args.no_build)
    check_glob_folders(root)
    check_names(root)
    for rule, path, line, detail in violations:
        print(f"{rule}: {path}:{line}: {detail}")
    sys.exit(1 if violations else 0)


main()

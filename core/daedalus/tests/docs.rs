//! The documentation under `docs/`: every relative link resolves, and no guide sends a reader
//! to a path of the branch this one replaces.

use std::fs;
use std::path::{Path, PathBuf};

fn docs() -> PathBuf {
    [env!("CARGO_MANIFEST_DIR"), "..", "..", "docs"]
        .iter()
        .collect()
}

fn markdown(root: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root).expect("docs directory") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            markdown(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "md") {
            files.push(path);
        }
    }
}

/// The targets of the Markdown links in `text`: what is between `](` and the next `)`. Code
/// spans and fenced blocks are left out, since a `](` there is code.
fn links(text: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut fenced = false;
    for line in text.lines() {
        if line.trim_start().starts_with("```") {
            fenced = !fenced;
            continue;
        }
        if fenced {
            continue;
        }
        // Drop code spans: the odd pieces between backticks.
        let prose: String = line
            .split('`')
            .enumerate()
            .filter(|(at, _)| at % 2 == 0)
            .map(|(_, piece)| piece)
            .collect::<Vec<_>>()
            .join(" ");
        let mut rest = prose.as_str();
        while let Some(at) = rest.find("](") {
            rest = &rest[at + 2..];
            let Some(end) = rest.find(')') else { break };
            found.push(rest[..end].to_string());
            rest = &rest[end..];
        }
    }
    found
}

/// The relative links of the file at `path` that point at nothing.
fn broken(path: &Path, text: &str) -> Vec<String> {
    let dir = path.parent().expect("a file has a directory");
    links(text)
        .into_iter()
        .filter(|link| {
            let external = link.contains("://") || link.starts_with("mailto:");
            // A link to a heading of the same file has no path to check.
            let target = link.split('#').next().unwrap_or("");
            !external && !target.is_empty() && !dir.join(target).exists()
        })
        .collect()
}

#[test]
fn every_relative_link_resolves() {
    let mut files = Vec::new();
    markdown(&docs(), &mut files);
    assert!(files.len() > 12, "the docs were found");
    let mut problems = Vec::new();
    for path in &files {
        let text = fs::read_to_string(path).expect("read a doc");
        for link in broken(path, &text) {
            problems.push(format!("{}: {link}", path.display()));
        }
    }
    assert!(
        problems.is_empty(),
        "links that resolve to nothing: {problems:#?}"
    );
}

#[test]
fn a_planted_broken_link_is_caught() {
    let page = docs().join("README.md");
    let planted = "See [the guide](no-such-guide.md), [this one](host.md) and `[code](gone.md)`.";
    assert_eq!(broken(&page, planted), ["no-such-guide.md"]);
    // A link to a missing file is caught with a heading on it too, and a fenced block is code.
    assert_eq!(
        broken(&page, "[a](gone.md#part)\n```\n[b](also-gone.md)\n```"),
        ["gone.md#part"]
    );
    assert!(
        broken(
            &page,
            "[web](https://example.org/x.md) [here](#the-service)"
        )
        .is_empty()
    );
}

/// The guides, which say how to do things on this branch. The planning files and the two pages
/// that exist to point at the earlier branch are not guides.
const GUIDES: [&str; 11] = [
    "README.md",
    "development.md",
    "testing.md",
    "node-authoring.md",
    "host.md",
    "plan-forms.md",
    "gpu-nodes.md",
    "plugins.md",
    "mcu.md",
    "ffi.md",
    "foreign-values.md",
];

#[test]
fn no_guide_names_a_dev_path_as_the_way_to_build_something() {
    // Every crate on the earlier branch was under `crates/`. Nothing on this branch is, apart from
    // `testing/crates/`.
    for name in GUIDES {
        let text = fs::read_to_string(docs().join(name)).expect("a guide");
        for (at, line) in text.lines().enumerate() {
            let named = line.replace("testing/crates/", "");
            assert!(
                !named.contains("crates/"),
                "docs/{name}:{}: names a path under `crates/`: {line}",
                at + 1
            );
        }
    }
    // The page that maps the earlier branch to this one does name them: that is what it is for.
    let map = fs::read_to_string(docs().join("migration-from-dev.md")).expect("the map");
    assert!(map.contains("`crates/runtime`"));
}

/// Files that are allowed to point at the planning files and at the branch this one replaces:
/// the planning files themselves, which stay until the owner removes them, the pages whose
/// subject is that branch, the records of measurements against it, and the indexes that list
/// them.
const POINTERS_ALLOWED: &[&str] = &[
    "AGENTS.md",
    "TODO.md",
    "README.md",
    "docs/README.md",
    "docs/design.md",
    "docs/architecture",
    "docs/prs/",
    "docs/migration-from-dev.md",
    "testing/capability-audit.md",
    "testing/board-runs/",
    "testing/budgets/",
    "scripts/check-capability-audit.sh",
    "scripts/ci.d/capability.sh",
    "scripts/pr-status.py",
    // The rule that a product name may appear in the planning files names them.
    "scripts/boundaries.py",
    // This file names what it looks for.
    "core/daedalus/tests/docs.rs",
];

/// Whether a line points at a planning file.
fn names_a_planning_file(line: &str) -> bool {
    [
        "docs/prs/",
        "TODO.md",
        "AGENTS.md",
        "design.md",
        "docs/architecture",
        "rebuild plan",
    ]
    .iter()
    .any(|name| line.contains(name))
}

/// Whether a line uses `dev` as the name of a branch: the word standing alone, and not part
/// of a path, a flag, a version, a manifest key or an identifier.
fn points_at_dev(line: &str) -> bool {
    let bytes = line.as_bytes();
    let word = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_';
    let mut from = 0;
    while let Some(at) = line[from..].find("dev") {
        let (start, end) = (from + at, from + at + 3);
        from = end;
        let before = start.checked_sub(1).map(|at| bytes[at]);
        let after = bytes.get(end).copied();
        // Part of a longer word, a path, a flag, a version or a dotted name.
        if before.is_some_and(|byte| word(byte) || b"/.-[\"".contains(&byte)) {
            continue;
        }
        // `dev's` is the branch's, and `dev.` ends a sentence unless a name follows the dot.
        let possessive = line[end..].starts_with("'s");
        let sentence_end =
            after == Some(b'.') && !bytes.get(end + 1).is_some_and(|byte| word(*byte));
        let carries_on = after.is_some_and(|byte| word(byte) || b"-/.]=(:\"".contains(&byte));
        if carries_on && !possessive && !sentence_end {
            continue;
        }
        return true;
    }
    false
}

/// Every text file git tracks or would track.
fn tracked_files(root: &Path) -> Vec<String> {
    let listed = std::process::Command::new("git")
        .args(["ls-files", "--cached", "--others", "--exclude-standard"])
        .current_dir(root)
        .output()
        .expect("git lists the files");
    assert!(listed.status.success(), "git ls-files failed");
    String::from_utf8_lossy(&listed.stdout)
        .lines()
        .map(str::to_owned)
        .collect()
}

#[test]
fn no_planning_anchors() {
    let root: PathBuf = [env!("CARGO_MANIFEST_DIR"), "..", ".."].iter().collect();
    let mut found = Vec::new();
    for file in tracked_files(&root) {
        if POINTERS_ALLOWED
            .iter()
            .any(|allowed| file.starts_with(allowed))
            || file == "Cargo.lock"
        {
            continue;
        }
        // Anything that isn't text has nothing to say.
        let Ok(text) = fs::read_to_string(root.join(&file)) else {
            continue;
        };
        for (at, line) in text.lines().enumerate() {
            if names_a_planning_file(line) || points_at_dev(line) {
                found.push(format!("{file}:{}: {}", at + 1, line.trim()));
            }
        }
    }
    assert!(found.is_empty(), "{found:#?}");
}

#[test]
fn a_planted_pointer_is_caught_and_what_only_looks_like_one_is_not() {
    for pointer in [
        "as on dev, the limit is 8",
        "`dev` measured 1.32 ms",
        "ported from dev's examples",
        "see dev",
        "worse than dev.",
    ] {
        assert!(points_at_dev(pointer), "{pointer}");
    }
    for fine in [
        "[dev-dependencies]",
        "open /dev/dma_heap/system",
        "[profile.dev]",
        "version 3.0.1-dev",
        "pub dev: u64,",
        "st_dev as u64",
        "a developer's machine",
        "cargo build --dev",
        "\"dev\"",
    ] {
        assert!(!points_at_dev(fine), "{fine}");
    }
    assert!(names_a_planning_file("see docs/prs/E3.md"));
    assert!(names_a_planning_file("# (see AGENTS.md)"));
    assert!(!names_a_planning_file("see docs/node-authoring.md"));
}

/// The Rust blocks of `text`: the lines of each fence that opens with `rust`, without the fences.
fn rust_blocks(text: &str) -> Vec<String> {
    let mut blocks: Vec<String> = Vec::new();
    let mut inside = false;
    for line in text.lines() {
        if line.trim_start().starts_with("```") {
            if inside {
                inside = false;
            } else if line.trim() == "```rust" {
                inside = true;
                blocks.push(String::new());
            }
            continue;
        }
        if inside {
            let block = blocks.last_mut().expect("a block is open");
            block.push_str(line);
            block.push('\n');
        }
    }
    blocks
}

/// The Rust the foreign-values doc quotes is in the test file that compiles it, word for word.
#[test]
fn the_foreign_values_doc_quotes_code_that_compiles() {
    let doc = fs::read_to_string(docs().join("foreign-values.md")).expect("the doc");
    let code_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "tests", "foreign_values.rs"]
        .iter()
        .collect();
    let code = fs::read_to_string(&code_path).expect("the example's test file");
    let blocks = rust_blocks(&doc);
    assert!(!blocks.is_empty(), "the doc quotes no Rust");
    for block in &blocks {
        assert!(
            code.contains(block.as_str()),
            "a Rust block of docs/foreign-values.md is not in {}:\n{block}",
            code_path.display()
        );
    }
}

#[test]
fn a_planted_quote_is_read_and_other_fences_are_not() {
    let text =
        "prose\n```rust\nlet x = 1;\n```\n```text\nnot code\n```\n```rust\nlet y = 2;\n```\n";
    assert_eq!(rust_blocks(text), ["let x = 1;\n", "let y = 2;\n"]);
    assert!(rust_blocks("no fence here").is_empty());
}

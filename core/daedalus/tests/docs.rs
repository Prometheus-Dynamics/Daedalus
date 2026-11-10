//! The documentation under `docs/`: every relative link resolves, and no guide sends a reader
//! to a path on `dev`.

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
    let planted = "See [the guide](no-such-guide.md), [this one](design.md) and `[code](gone.md)`.";
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
/// that exist to point at `dev` are not guides.
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
    // Every crate on `dev` was under `crates/`. Nothing on this branch is, apart from
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
    // The page that maps `dev` to this branch does name them: that is what it is for.
    let map = fs::read_to_string(docs().join("migration-from-dev.md")).expect("the map");
    assert!(map.contains("`crates/runtime`"));
}

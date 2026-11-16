//! Scenario files: `scenarios/*.scn`. Their syntax is in the crate's top-level doc.

use std::path::{Path, PathBuf};

/// One scenario, read from its file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scenario {
    /// The file's name without `.scn`. Its golden transcript is `golden/<name>.out`.
    pub name: String,
    /// The command lines, each with its line number in the file (1-based).
    pub commands: Vec<(usize, String)>,
    /// Whether the host loads plugins (`@host plugins`, the default, or `@host none`).
    pub plugins: bool,
    /// The proxy the driver is put behind, if any. A scenario behind a proxy runs TCP only.
    pub proxy: Proxy,
}

/// A proxy in front of the driver.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Proxy {
    None,
    /// `@proxy newer`: the server looks like a newer one.
    Newer,
    /// `@proxy frames N`: the client announces a packet cap of N bytes.
    Frames(u32),
}

/// Where the scenario files are.
pub fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scenarios")
}

/// Where the golden transcripts are.
pub fn golden_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("golden")
}

/// The scenario files, in name order.
pub fn load_all() -> Result<Vec<Scenario>, String> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir())
        .map_err(|error| format!("{}: {error}", dir().display()))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "scn"))
        .collect();
    paths.sort();
    paths.iter().map(|path| load(path)).collect()
}

pub fn load(path: &Path) -> Result<Scenario, String> {
    let name = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .ok_or_else(|| format!("{}: no name", path.display()))?
        .to_owned();
    let text =
        std::fs::read_to_string(path).map_err(|error| format!("{}: {error}", path.display()))?;
    parse(&name, &text).map_err(|error| format!("{}: {error}", path.display()))
}

/// Parses a scenario's text. A `@` line is a directive for the harness and is never sent to the
/// driver. A `#` line is a comment. A blank line is nothing. Any other line is a command.
pub fn parse(name: &str, text: &str) -> Result<Scenario, String> {
    let mut scenario = Scenario {
        name: name.to_owned(),
        commands: Vec::new(),
        plugins: true,
        proxy: Proxy::None,
    };
    for (index, line) in text.lines().enumerate() {
        let number = index + 1;
        let line = line.trim_end();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(directive) = line.strip_prefix('@') {
            match directive.split(' ').collect::<Vec<_>>().as_slice() {
                ["host", "plugins"] => scenario.plugins = true,
                ["host", "none"] => scenario.plugins = false,
                ["proxy", "newer"] => scenario.proxy = Proxy::Newer,
                ["proxy", "frames", cap] => {
                    let cap = cap
                        .parse()
                        .map_err(|_| format!("line {number}: no cap `{cap}`"))?;
                    scenario.proxy = Proxy::Frames(cap);
                }
                _ => return Err(format!("line {number}: no directive `{line}`")),
            }
            continue;
        }
        scenario.commands.push((number, line.to_owned()));
    }
    Ok(scenario)
}

/// The command line as the driver gets it: `{plugin}` is the fixture library, and `{graphs}` is
/// the directory of graph documents. A path must not contain a space, since the syntax splits on
/// spaces.
pub fn expand(line: &str, plugin: &Path, graphs: &Path) -> Result<String, String> {
    let plugin = plugin.display().to_string();
    let graphs = graphs.display().to_string();
    if plugin.contains(' ') || graphs.contains(' ') {
        return Err("a path the suite passes to a driver has a space in it".to_owned());
    }
    Ok(line
        .replace("{plugin}", &plugin)
        .replace("{graphs}", &graphs))
}

/// Where the graph documents are.
pub fn graphs_dir() -> PathBuf {
    dir().join("graphs")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directives_and_comments_are_not_commands() {
        let scenario = parse(
            "x",
            "@host none\n# a note\n\nping\n@proxy newer\nsession a\n",
        )
        .unwrap();
        assert!(!scenario.plugins);
        assert_eq!(scenario.proxy, Proxy::Newer);
        assert_eq!(
            scenario.commands,
            vec![(4, "ping".to_owned()), (6, "session a".to_owned())]
        );
    }

    #[test]
    fn an_unknown_directive_is_refused() {
        assert!(parse("x", "@host everything\n").is_err());
    }

    #[test]
    fn placeholders_expand_to_paths() {
        let line = expand(
            "put_graph g {graphs}/pass.json",
            Path::new("/lib/p.so"),
            Path::new("/g"),
        )
        .unwrap();
        assert_eq!(line, "put_graph g /g/pass.json");
    }
}

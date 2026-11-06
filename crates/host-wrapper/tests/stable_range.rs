//! `daedalus_surface_stable_abi_min` and `_max`: the stable plugin ABI range the surface library
//! installs, as `u32` data symbols. An image checker (Gaia) reads them from the file, without
//! loading it, and checks each plugin's `daedalus_plugin_stable_version` against them. Checked here
//! through the dynamic loader and, where `objdump` is installed, statically from the file.

#![cfg(target_os = "linux")]

#[path = "support/cdylib.rs"]
mod cdylib;

use daedalus::dylib::{STABLE_ABI_MIN_VERSION, STABLE_ABI_VERSION};

const SYMBOLS: [(&str, u32); 2] = [
    ("daedalus_surface_stable_abi_min", STABLE_ABI_MIN_VERSION),
    ("daedalus_surface_stable_abi_max", STABLE_ABI_VERSION),
];

#[test]
fn the_surface_exports_its_stable_abi_range_as_data() {
    let path = cdylib::build_cdylib("daedalus-surface", "daedalus_surface", "");
    // Safety: the surface library built by this workspace; only its data symbols are read.
    let library = unsafe { libloading::Library::new(&path) }.unwrap();
    for (name, expected) in SYMBOLS {
        // Safety: the symbol is a `u32` static (`crates/surface/src/abi.rs`).
        let symbol = unsafe { library.get::<*const u32>(name.as_bytes()) }.unwrap();
        // Safety: the address of a live static of the loaded library.
        assert_eq!(unsafe { **symbol }, expected, "{name}");
        assert_ne!(
            expected, 0,
            "{name}: non-zero, so the static has file contents"
        );
    }
    drop(library);
    const { assert!(STABLE_ABI_MIN_VERSION <= STABLE_ABI_VERSION) };

    // Statically, as Gaia reads them: defined dynamic symbols of 4 bytes in `.rodata`.
    let Ok(output) = std::process::Command::new("objdump")
        .arg("-T")
        .arg(&path)
        .output()
    else {
        return;
    };
    assert!(output.status.success());
    let table = String::from_utf8_lossy(&output.stdout);
    for (name, _) in SYMBOLS {
        let line = table
            .lines()
            .find(|line| line.ends_with(name))
            .unwrap_or_else(|| panic!("`{name}` in the dynamic symbols"));
        let fields: Vec<&str> = line.split_whitespace().collect();
        assert!(fields.contains(&".rodata"), "in .rodata: {line}");
        let size = fields
            .iter()
            .skip_while(|field| **field != ".rodata")
            .nth(1)
            .and_then(|size| u64::from_str_radix(size, 16).ok());
        assert_eq!(size, Some(4), "4 bytes: {line}");
    }
}

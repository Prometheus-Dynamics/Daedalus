//! `daedalus_plugin_stable_version`: the stable ABI version as a `u32` data symbol, readable
//! without loading the plugin. Checked here through the dynamic loader (`libloading`) and, where
//! `nm` is installed, statically from the file.
#![cfg(not(target_env = "musl"))]

#[path = "support/cdylib.rs"]
mod cdylib;

use daedalus::PluginLibrary;
use daedalus::dylib::{STABLE_ABI_VERSION, STABLE_VERSION_SYMBOL};

#[test]
fn the_plugin_exports_its_stable_version_as_data() {
    let path = cdylib::stable_abi_cdylib();
    // Safety: a trusted test plugin; only its data symbol is read.
    let library = unsafe { libloading::Library::new(path) }.unwrap();
    // Safety: the symbol is a `u32` static (`export_plugin!`).
    let symbol = unsafe { library.get::<*const u32>(STABLE_VERSION_SYMBOL.as_bytes()) }.unwrap();
    // Safety: the address of a live static of the loaded library.
    assert_eq!(unsafe { **symbol }, STABLE_ABI_VERSION);
    drop(library);

    // The loader cross-checks it against the descriptor and loads the plugin.
    // Safety: as above.
    let loaded = unsafe { PluginLibrary::load(path) }.unwrap();
    assert_eq!(loaded.stable_abi_version(), STABLE_ABI_VERSION);

    // Statically, from the file, as Gaia's image checker reads it: a defined dynamic symbol of
    // exactly 4 bytes in `.rodata` (file contents, not `.bss`), non-zero.
    if cfg!(target_os = "linux")
        && let Ok(output) = std::process::Command::new("objdump")
            .arg("-T")
            .arg(path)
            .output()
        && output.status.success()
    {
        let table = String::from_utf8_lossy(&output.stdout);
        let line = table
            .lines()
            .find(|line| line.ends_with(STABLE_VERSION_SYMBOL))
            .unwrap_or_else(|| panic!("`{STABLE_VERSION_SYMBOL}` in the dynamic symbols"));
        let fields: Vec<&str> = line.split_whitespace().collect();
        assert!(fields.contains(&".rodata"), "in .rodata: {line}");
        let size = fields
            .iter()
            .skip_while(|field| **field != ".rodata")
            .nth(1)
            .and_then(|size| u64::from_str_radix(size, 16).ok());
        assert_eq!(size, Some(4), "4 bytes: {line}");
    }
    assert_ne!(
        STABLE_ABI_VERSION, 0,
        "non-zero, so the static has file contents"
    );
}

//! What a plugin built before plugins described their shapes looks like to a loader: it has the
//! old entry symbol and nothing else. The loader must say so, and must never call it.

/// The old entry point. A loader that called it would be handing it arguments it doesn't take.
///
/// # Safety
/// Never called: the loader only asks whether the symbol is there.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_plugin_entry(_host_layout: u64, _out: *mut u8) -> i32 {
    std::process::abort()
}

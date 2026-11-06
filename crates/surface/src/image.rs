//! In-image nodes: [`export_surface!`](crate::export_surface) builds an image's own surface
//! library, whose hosts come with the image's node crates installed through the Rust path.
//!
//! The C exports always come from this crate's `rlib`: a `cdylib` exports the `#[no_mangle]`
//! functions of every crate it links, so an image `cdylib` that links `daedalus-surface` exports the
//! whole surface ABI once, as the plain `libdaedalus_surface.so` does, and the macro emits no
//! export of its own. What the macro adds is a load-time constructor (an `.init_array` entry on
//! ELF, `__mod_init_func` on Apple targets, `.CRT$XCU` on Windows) that registers the image's
//! install function here before any entry point can be called. `daedalus_surface_host_create`
//! runs it on each new host's registry; the plain surface registers none and installs nothing.

use std::sync::OnceLock;

use daedalus::runtime::plugins::PluginRegistry;

/// An image's install function, as [`export_surface!`](crate::export_surface) registers it: the
/// image's nodes into a new host's registry, an error as text.
pub type ImageInstall = fn(&mut PluginRegistry) -> Result<(), String>;

static IMAGE: OnceLock<ImageInstall> = OnceLock::new();

/// Registers the image's install function. Called once, by the constructor
/// [`export_surface!`](crate::export_surface) emits; a second registration is ignored.
#[doc(hidden)]
pub fn register(install: ImageInstall) {
    let _ = IMAGE.set(install);
}

/// The registered install function, if this library is an image.
pub(crate) fn install() -> Option<ImageInstall> {
    IMAGE.get().copied()
}

/// Makes the `cdylib` being built an image surface: every host it creates has `install` run on its
/// registry first, so the image's node crates run through the normal Rust path (Rust handlers,
/// full calls, no stable ABI). Separately built plugins still install on top, by path, through the
/// stable ABI.
///
/// `install` is a `fn(&mut PluginRegistry) -> Result<(), E>` with `E: Display`, for example a
/// function calling `registry.install_plugin(&MyPlugin::new())` for each in-image plugin. A failing
/// install makes `daedalus_surface_host_create` return `DAEDALUS_IO`.
///
/// Use it once, in a crate built as `crate-type = ["cdylib"]` that depends on `daedalus-surface`
/// (its `rlib`) and on the node crates. The surface's C exports exist exactly once in the image:
/// they come from the `rlib` (a `cdylib` exports the `#[no_mangle]` functions of every crate it
/// links), and the macro emits none. It adds only a load-time constructor (an `.init_array` entry
/// on ELF targets, `__mod_init_func` on Apple targets, `.CRT$XCU` on Windows) that registers
/// `install` before any entry point can be called. Elsewhere it does not compile.
///
/// ```ignore
/// fn install(registry: &mut PluginRegistry) -> PluginResult<()> {
///     registry.install_plugin(&MyNodes::new())
/// }
/// daedalus_surface::export_surface!(install);
/// ```
#[macro_export]
macro_rules! export_surface {
    ($install:path $(,)?) => {
        const _: () = {
            fn __daedalus_surface_image_install(
                registry: &mut $crate::__private::PluginRegistry,
            ) -> ::core::result::Result<(), ::std::string::String> {
                $install(registry).map_err(|error| ::std::string::ToString::to_string(&error))
            }

            extern "C" fn __daedalus_surface_image_init() {
                $crate::__private::register(__daedalus_surface_image_install);
            }

            #[cfg(not(any(
                target_os = "linux",
                target_os = "android",
                target_os = "freebsd",
                target_os = "netbsd",
                target_os = "openbsd",
                target_os = "dragonfly",
                target_os = "illumos",
                target_os = "solaris",
                target_vendor = "apple",
                windows
            )))]
            compile_error!("export_surface! has no load-time constructor for this target");

            #[used]
            #[cfg_attr(
                any(
                    target_os = "linux",
                    target_os = "android",
                    target_os = "freebsd",
                    target_os = "netbsd",
                    target_os = "openbsd",
                    target_os = "dragonfly",
                    target_os = "illumos",
                    target_os = "solaris"
                ),
                unsafe(link_section = ".init_array")
            )]
            #[cfg_attr(
                target_vendor = "apple",
                unsafe(link_section = "__DATA,__mod_init_func")
            )]
            #[cfg_attr(windows, unsafe(link_section = ".CRT$XCU"))]
            static __DAEDALUS_SURFACE_IMAGE_INIT: extern "C" fn() = __daedalus_surface_image_init;
        };
    };
}

#[allow(unused_imports)]
use daedalus_runtime::plugins::{PluginRegistry, RegistryPluginExt};
use libloading::Library;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use thiserror::Error;

/// Symbol name exported by dynamic plugins.
///
/// ```
/// use daedalus_ffi::REGISTER_SYMBOL;
/// assert_eq!(REGISTER_SYMBOL, "daedalus_plugin_register");
/// ```
pub const REGISTER_SYMBOL: &str = "daedalus_plugin_register";
pub const PLUGIN_INFO_SYMBOL: &str = "daedalus_plugin_info";
pub const PLUGIN_ABI_SYMBOL: &str = "daedalus_plugin_abi_version";
pub const PLUGIN_ABI_VERSION: u32 = 2;
pub const FFI_VERSION: &str = env!("CARGO_PKG_VERSION");

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct StrView {
    pub ptr: *const u8,
    pub len: usize,
}

impl StrView {
    pub fn from_static(value: &'static str) -> Self {
        Self {
            ptr: value.as_ptr(),
            len: value.len(),
        }
    }

    pub fn as_str(&self) -> Option<&'static str> {
        if self.ptr.is_null() {
            return None;
        }
        // Safety: caller guarantees the pointer and length are valid for the program lifetime.
        let bytes = unsafe { std::slice::from_raw_parts(self.ptr, self.len) };
        std::str::from_utf8(bytes).ok()
    }
}

impl From<&'static str> for StrView {
    fn from(value: &'static str) -> Self {
        Self::from_static(value)
    }
}

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct PluginInfo {
    pub abi_version: u32,
    pub ffi_version: StrView,
    pub daedalus_version: StrView,
    pub plugin_name: StrView,
    pub plugin_version: StrView,
}

impl PluginInfo {
    pub fn new(
        plugin_name: &'static str,
        plugin_version: &'static str,
        daedalus_version: &'static str,
    ) -> Self {
        Self {
            abi_version: PLUGIN_ABI_VERSION,
            ffi_version: StrView::from_static(FFI_VERSION),
            daedalus_version: StrView::from_static(daedalus_version),
            plugin_name: StrView::from_static(plugin_name),
            plugin_version: StrView::from_static(plugin_version),
        }
    }
}

/// Errors that can occur while loading or installing a dynamic plugin.
///
/// ```ignore
/// use daedalus_ffi::FfiPluginError;
/// let err = FfiPluginError::MissingSymbol;
/// assert!(format!("{err}").contains("register symbol"));
/// ```
#[derive(Debug, Error)]
pub enum FfiPluginError {
    #[error("failed to load library: {0}")]
    Load(#[from] libloading::Error),
    #[error("register symbol `{REGISTER_SYMBOL}` missing")]
    MissingSymbol,
    #[error("plugin registration failed")]
    RegisterReturnedError,
}

type RegisterFn = unsafe extern "C" fn(*mut PluginRegistry) -> bool;
type InfoFn = unsafe extern "C" fn() -> PluginInfo;
type AbiFn = unsafe extern "C" fn() -> u32;

/// Loaded plugin library that can install itself into a registry.
///
/// ```no_run
/// use daedalus_ffi::PluginLibrary;
///
/// # unsafe {
/// let lib = PluginLibrary::load("libdemo_plugin.so").unwrap();
/// # let _ = lib;
/// # }
/// ```
pub struct PluginLibrary {
    /// Keep the dynamic library loaded for the lifetime of the process.
    ///
    /// This is intentional: node handlers registered by a plugin can contain function pointers
    /// into the dylib. Unloading the library while those handlers remain reachable is undefined
    /// behavior and can segfault.
    #[allow(dead_code)]
    lib: &'static Library,
    register: RegisterFn,
    info: Option<InfoFn>,
    abi_version: Option<AbiFn>,
    _path: PathBuf,
}

impl PluginLibrary {
    /// Load a plugin from a dynamic library path, resolving the registration symbol.
    ///
    /// This is the recommended API for most downstream consumers: the library is kept loaded for
    /// the lifetime of the process to avoid undefined behavior from unloading while registered
    /// handlers still exist.
    ///
    /// # Safety
    /// Caller must ensure the path points to a trusted library that matches the expected
    /// `RegisterFn` ABI; loading arbitrary libraries is undefined behavior.
    pub unsafe fn load(path: impl AsRef<Path>) -> Result<Self, FfiPluginError> {
        let path = path.as_ref().to_path_buf();
        let lib = unsafe { Library::new(&path)? };
        // Keep the library loaded permanently to avoid UB from unloading while handlers exist.
        // This matches the strategy used in the C/C++ loader (`crates/ffi/src/c_cpp.rs`).
        let lib = Box::leak(Box::new(lib));
        let register = unsafe { lib.get::<RegisterFn>(REGISTER_SYMBOL.as_bytes()) }
            .map(|f| *f)
            .map_err(|_| FfiPluginError::MissingSymbol)?;
        let info = unsafe { lib.get::<InfoFn>(PLUGIN_INFO_SYMBOL.as_bytes()) }
            .map(|f| *f)
            .ok();
        let abi_version = unsafe { lib.get::<AbiFn>(PLUGIN_ABI_SYMBOL.as_bytes()) }
            .map(|f| *f)
            .ok();
        Ok(Self {
            lib,
            register,
            info,
            abi_version,
            _path: path,
        })
    }

    /// Install the plugin into the provided registry.
    ///
    /// ```no_run
    /// use daedalus_ffi::PluginLibrary;
    /// use daedalus_runtime::plugins::PluginRegistry;
    ///
    /// # unsafe {
    /// let lib = PluginLibrary::load("libdemo_plugin.so").unwrap();
    /// let mut registry = PluginRegistry::default();
    /// let _ = lib.install_into(&mut registry);
    /// # }
    /// ```
    pub fn install_into(&self, registry: &mut PluginRegistry) -> Result<(), FfiPluginError> {
        let ok = unsafe { (self.register)(registry as *mut PluginRegistry) };
        if ok {
            Ok(())
        } else {
            Err(FfiPluginError::RegisterReturnedError)
        }
    }

    pub fn info(&self) -> Option<PluginInfo> {
        self.info.map(|f| unsafe { f() })
    }

    pub fn abi_version(&self) -> Option<u32> {
        self.abi_version.map(|f| unsafe { f() })
    }
}

/// A plugin library that can be explicitly unloaded.
///
/// This type never unloads on `Drop` (so you cannot accidentally segfault by letting it go out of
/// scope). If you truly need unloading, call `unload(self)` explicitly.
#[allow(dead_code)]
pub struct ScopedPluginLibrary {
    lib: std::mem::ManuallyDrop<Library>,
    register: RegisterFn,
    path: PathBuf,
    installed: AtomicBool,
}

#[allow(dead_code)]
impl ScopedPluginLibrary {
    /// Load a plugin library that can be explicitly unloaded via `unload(self)`.
    ///
    /// Prefer `PluginLibrary::load` unless you have a strong need to unload plugins.
    ///
    /// # Safety
    /// The dylib must be trusted and match the expected ABI.
    pub unsafe fn load(path: impl AsRef<Path>) -> Result<Self, FfiPluginError> {
        let path = path.as_ref().to_path_buf();
        let lib = unsafe { Library::new(&path)? };
        let register = unsafe { lib.get::<RegisterFn>(REGISTER_SYMBOL.as_bytes()) }
            .map(|f| *f)
            .map_err(|_| FfiPluginError::MissingSymbol)?;
        Ok(Self {
            lib: std::mem::ManuallyDrop::new(lib),
            register,
            path,
            installed: AtomicBool::new(false),
        })
    }

    pub fn install_into(&self, registry: &mut PluginRegistry) -> Result<(), FfiPluginError> {
        let ok = unsafe { (self.register)(registry as *mut PluginRegistry) };
        if ok {
            self.installed.store(true, Ordering::Relaxed);
            Ok(())
        } else {
            Err(FfiPluginError::RegisterReturnedError)
        }
    }

    /// Explicitly unload the library.
    ///
    /// # Safety
    /// If any plugin-registered handlers (function pointers) are still reachable and invoked
    /// after unload, the process may crash (undefined behavior).
    pub unsafe fn unload(mut self) {
        if self.installed.load(Ordering::Relaxed) {
            eprintln!(
                "warning: unloading plugin library '{}' after install; calling any handlers registered by this plugin after unload is undefined behavior",
                self.path.display()
            );
        }
        unsafe { std::mem::ManuallyDrop::drop(&mut self.lib) };
    }
}

/// Export a `daedalus::runtime::plugins::Plugin` implementor for dynamic loading.
///
/// This generates the `daedalus_plugin_register` symbol that hosts expect.
///
/// ```ignore
/// use daedalus_ffi::export_plugin;
///
/// #[derive(Default)]
/// struct DemoPlugin;
///
/// export_plugin!(DemoPlugin);
/// ```
#[macro_export]
macro_rules! export_plugin {
    ($ty:ty) => {
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn daedalus_plugin_abi_version() -> u32 {
            $crate::PLUGIN_ABI_VERSION
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn daedalus_plugin_info() -> $crate::PluginInfo {
            $crate::PluginInfo::new(
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION"),
                daedalus::version(),
            )
        }

        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn daedalus_plugin_register(
            registry: *mut daedalus::runtime::plugins::PluginRegistry,
        ) -> bool {
            if registry.is_null() {
                return false;
            }
            let plugin: $ty = <$ty as Default>::default();
            let reg = unsafe { &mut *registry };
            daedalus::runtime::plugins::RegistryPluginExt::install_plugin(reg, &plugin).is_ok()
        }
    };
}

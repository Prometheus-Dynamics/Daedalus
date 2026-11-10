//! The shapes written here by hand against the library's own, and the table against the
//! library's symbols.

use daedalus_capi_sys as sys;

#[test]
fn every_shape_is_the_librarys() {
    use std::mem::{align_of, size_of};
    macro_rules! same {
        ($($ours:ty = $theirs:ty),* $(,)?) => {$(
            assert_eq!(
                (size_of::<$ours>(), align_of::<$ours>()),
                (size_of::<$theirs>(), align_of::<$theirs>()),
                stringify!($ours)
            );
        )*};
    }
    same!(
        sys::Version = daedalus_capi::Version,
        sys::RawPort = daedalus_capi::PortInfo,
        sys::Telemetry = daedalus_capi::Telemetry,
        sys::Tick = daedalus_capi::TickInfo,
        sys::RawBytes = daedalus_capi::ByteSpan,
        sys::RawValue = daedalus_capi::PushValue,
        sys::RawPush = daedalus_capi::Push,
        sys::Code = daedalus_capi::Status,
        sys::Handle = daedalus_capi::Handle,
    );
    assert_eq!(
        [
            sys::KIND_I64,
            sys::KIND_F64,
            sys::KIND_BOOL,
            sys::KIND_BYTES
        ],
        [
            daedalus_capi::KIND_I64,
            daedalus_capi::KIND_F64,
            daedalus_capi::KIND_BOOL,
            daedalus_capi::KIND_BYTES
        ]
    );
    assert_eq!(sys::OK, daedalus_capi::Status::OK.0);
    assert_eq!(
        sys::BUFFER_TOO_SMALL,
        daedalus_capi::Status::BUFFER_TOO_SMALL.0
    );
}

#[test]
fn the_table_is_every_symbol_and_a_library_missing_one_is_refused() {
    assert_eq!(sys::SYMBOLS.len(), 33);
    let mut names = sys::SYMBOLS.to_vec();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), 33, "each once");

    let exe = std::env::current_exe().expect("the test's own path");
    let deps = exe.parent().expect("the deps directory");
    let library = deps.join(format!(
        "{}daedalus_capi{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    ));
    let api = sys::Api::load(&library).expect("the surface loads");
    assert_eq!(api.status_name(sys::OK), "DAEDALUS_OK");
    assert_eq!(api.status_name(4), "DAEDALUS_BUSY");
    assert_eq!(api.status_name(9999), "DAEDALUS_UNKNOWN_STATUS");
    assert_eq!(api.last_error(), "", "nothing has failed on this thread");
    let version = api.version().expect("a version");
    assert_ne!(version.layout_hash, 0);

    // Any other library lacks the surface's names. This test's own binary is no library.
    assert!(matches!(
        sys::Api::load(&exe),
        Err(sys::LoadError::Open(_) | sys::LoadError::Missing(_))
    ));
    assert!(matches!(
        sys::Api::load("/nowhere/lib.so"),
        Err(sys::LoadError::Open(_))
    ));
}

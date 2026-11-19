//! Off, the default, checks nothing: a loader made with it loads a library as it always did, on
//! every platform.

use crate::support;

use daedalus_integrity::{Integrity, Mode};
use daedalus_loader::Loader;
use support::copy_fixture;

#[test]
fn off_checks_nothing_and_keeps_no_file() {
    let lib = copy_fixture("off");
    let plugin = Loader::new().open(&lib).expect("Off loads");
    assert!(plugin.generation().digest().is_none());
    let plugin = Loader::with_integrity(Integrity::off())
        .open(&lib)
        .expect("explicit Off loads");
    assert_eq!(plugin.name(), "dylib-fixture");
    assert_eq!(Integrity::off().mode(), Mode::Off);
}

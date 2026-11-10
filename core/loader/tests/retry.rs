//! The backoff: it grows, it never passes the cap, and it resets once a load works.

use std::time::Duration;

use daedalus_loader::Loader;
use daedalus_loader::{Retry, load_with_retry};
use std::sync::atomic::AtomicBool;

#[test]
fn the_retry_delay_never_exceeds_the_cap() {
    let mut retry = Retry::new();
    let mut last = Duration::ZERO;
    for _ in 0..200 {
        let delay = retry.failed();
        assert!(delay <= Duration::from_secs(8), "{delay:?} is over the cap");
        assert!(delay >= last, "the delay never shrinks while failing");
        last = delay;
    }
    assert_eq!(last, Duration::from_secs(8));
    assert_eq!(Retry::delay_for(0), Duration::from_millis(250));
    assert_eq!(Retry::delay_for(1), Duration::from_millis(500));
}

#[test]
fn a_successful_load_clears_the_count() {
    let mut retry = Retry::new();
    retry.failed();
    retry.failed();
    retry.succeeded();
    assert_eq!(retry.failures(), 0);
    assert_eq!(retry.failed(), Duration::from_millis(250));
}

#[test]
fn a_stop_set_before_any_attempt_gives_stopped_without_opening() {
    let stop = AtomicBool::new(true);
    let mut retry = Retry::new();
    let mut loader = Loader::new();
    let outcome = load_with_retry(
        &mut loader,
        std::path::Path::new("/nonexistent/never.so"),
        &stop,
        &mut retry,
        |_| panic!("no sleep when stopped first"),
    );
    assert!(outcome.is_err());
    assert_eq!(retry.failures(), 0);
}

//! A lost GPU context is replaced by the source's next request, under the same capped backoff as a
//! failed open: inside the backoff the loss is returned at once, after it the context is opened
//! again. A reference to a replaced context stays valid. No test waits on a real clock.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use daedalus_gpu::{GpuBackendKind, GpuError, GpuOptions, select_backend};
use daedalus_runtime::{GpuSource, RetryingGpu};

fn mock() -> Result<daedalus_gpu::GpuContextHandle, GpuError> {
    select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Mock),
        allow_software: true,
        ..Default::default()
    })
}

/// Inside a long backoff, a lost context is not reopened: the loss is the request's error.
#[test]
fn a_lost_context_inside_its_backoff_is_reported_without_an_open() {
    let opens = Arc::new(AtomicUsize::new(0));
    let counter = opens.clone();
    let source = RetryingGpu::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
        mock()
    })
    .with_backoff(Duration::from_secs(3600), Duration::from_secs(3600));
    source.get().expect("opened");
    assert_eq!(opens.load(Ordering::SeqCst), 1);

    source.mark_lost("device lost");
    let Err(err) = source.get() else {
        panic!("lost, inside the backoff, must fail");
    };
    assert!(err.to_string().contains("device lost"), "{err}");
    assert_eq!(
        opens.load(Ordering::SeqCst),
        1,
        "no open inside the backoff"
    );
}

/// With no backoff a lost context is replaced by the next request, and the replacement is a
/// different context from the one that was lost.
#[test]
fn a_lost_context_is_replaced_by_the_next_request() {
    let opens = Arc::new(AtomicUsize::new(0));
    let counter = opens.clone();
    let source = RetryingGpu::new(move || {
        counter.fetch_add(1, Ordering::SeqCst);
        mock()
    })
    .with_backoff(Duration::ZERO, Duration::ZERO);
    let first = source.get().expect("opened") as *const _;

    source.mark_lost("device lost");
    let second = source.get().expect("reopened") as *const _;
    assert_eq!(opens.load(Ordering::SeqCst), 2);
    assert_ne!(first, second, "a new context");
    assert!(!source.get().expect("kept").is_lost());
    assert_eq!(opens.load(Ordering::SeqCst), 2, "the new context is kept");
}

/// A reference to a replaced context stays usable: the source keeps every opened context.
#[test]
fn a_reference_to_a_replaced_context_stays_valid() {
    let source = RetryingGpu::new(mock).with_backoff(Duration::ZERO, Duration::ZERO);
    let old = source.get().expect("opened");
    source.mark_lost("device lost");
    let replacement = source.get().expect("replaced");
    assert_eq!(old.backend_kind(), replacement.backend_kind());
}

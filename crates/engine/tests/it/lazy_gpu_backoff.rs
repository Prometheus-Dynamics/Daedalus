//! The GPU source's failed-open backoff. A failed open returns its error at once, the error is
//! kept for the backoff, and a request inside it opens nothing. The first request after the
//! backoff opens again. No request blocks on the backoff, and every request is counted.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use daedalus_gpu::{GpuBackendKind, GpuError, GpuOptions, select_backend};
use daedalus_runtime::{GpuSource, RETRY_BASE, RETRY_CAP, RetryingGpu};

/// A request returns within this bound: nothing on the request path waits for the backoff.
const NO_BLOCK: Duration = Duration::from_millis(200);

fn mock() -> Result<daedalus_gpu::GpuContextHandle, GpuError> {
    select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Mock),
        allow_software: true,
        ..Default::default()
    })
}

/// The backoff after the `failures`-th failed open (the source's own schedule).
fn backoff(failures: u32) -> Duration {
    (RETRY_BASE * (1u32 << failures.min(16))).min(RETRY_CAP)
}

#[test]
fn a_source_that_fails_then_opens_never_blocks_and_counts_every_request() {
    const FAIL_FOR: u32 = 3;
    let opens = Arc::new(AtomicUsize::new(0));
    let counter = opens.clone();
    let source = RetryingGpu::new(move || {
        let attempt = counter.fetch_add(1, Ordering::SeqCst) as u32;
        if attempt < FAIL_FOR {
            Err(GpuError::AdapterUnavailable)
        } else {
            mock()
        }
    });
    let mut requests = 0;
    for failure in 1..=FAIL_FOR {
        let start = Instant::now();
        assert!(
            source.get().is_err(),
            "failed open {failure} returns its error"
        );
        assert!(start.elapsed() < NO_BLOCK, "a failed open blocked");
        requests += 1;

        let start = Instant::now();
        assert!(
            source.get().is_err(),
            "inside the backoff the cached error returns"
        );
        assert!(
            start.elapsed() < NO_BLOCK,
            "a request inside the backoff blocked"
        );
        requests += 1;
        assert_eq!(
            opens.load(Ordering::SeqCst),
            failure as usize,
            "no open inside the backoff"
        );

        // The test waits, not the source: the next request is after the backoff.
        std::thread::sleep(backoff(failure) + Duration::from_millis(50));
    }

    let start = Instant::now();
    assert!(
        source.get().is_ok(),
        "the device opens once the backoff has passed"
    );
    assert!(
        start.elapsed() < NO_BLOCK,
        "the open after the backoff blocked"
    );
    requests += 1;
    assert!(source.created());
    assert_eq!(opens.load(Ordering::SeqCst), FAIL_FOR as usize + 1);

    assert!(
        source.get().is_ok(),
        "an open context is returned without another open"
    );
    requests += 1;
    assert_eq!(opens.load(Ordering::SeqCst), FAIL_FOR as usize + 1);
    assert_eq!(source.accesses(), requests, "every request is counted");
}

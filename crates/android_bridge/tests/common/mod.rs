//! Shared helpers for the integration tests that drive the bridge's process-global worker.

use oxideworker::{get_worker_status_impl, stop_worker_impl};
use std::time::{Duration, Instant};

/// Stops the worker and waits until its drain has finished (TEST-48).
///
/// `stop_worker_impl` returns while the worker may still be draining: it sets the status
/// `STOPPING` and a detached thread writes `STOPPED` later, after joining the worker thread. A
/// test that follows, even behind a lock, can write its own status (for example
/// `ERROR: Invalid cluster secret: ...`) and have that late `STOPPED` overwrite it before the test
/// reads it back. Waiting until the status is no longer `STOPPING` closes that window.
#[allow(dead_code)]
pub fn stop_and_settle() -> bool {
    let stopped = stop_worker_impl();
    let deadline = Instant::now() + Duration::from_secs(30);
    while get_worker_status_impl().contains("\"status\":\"STOPPING\"") {
        assert!(
            Instant::now() < deadline,
            "worker did not finish stopping within 30 s"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    stopped
}

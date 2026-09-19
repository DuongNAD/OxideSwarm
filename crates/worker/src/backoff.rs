//! Exponential backoff with uniform jitter for resilient worker reconnections.

use rusty_grid_core::GridError;
use std::io;
use std::time::Duration;

/// Configuration parameters for exponential backoff calculations.
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    /// Initial backoff delay for the first retry attempt (default: 500ms).
    pub initial: Duration,
    /// Maximum capped backoff delay (default: 10.0s).
    pub max: Duration,
    /// Exponential growth multiplier (default: 2.0).
    pub factor: f64,
    /// Uniform random jitter ratio (default: 0.20, representing +/-20%).
    pub jitter_ratio: f64,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(500),
            max: Duration::from_secs(10),
            factor: 2.0,
            jitter_ratio: 0.20,
        }
    }
}

impl BackoffConfig {
    pub fn new(initial: Duration, max: Duration, factor: f64, jitter_ratio: f64) -> Self {
        Self {
            initial,
            max,
            factor,
            jitter_ratio,
        }
    }

    pub fn with_initial(mut self, initial: Duration) -> Self {
        self.initial = initial;
        self
    }

    pub fn with_max(mut self, max: Duration) -> Self {
        self.max = max;
        self
    }

    pub fn with_factor(mut self, factor: f64) -> Self {
        self.factor = factor;
        self
    }

    pub fn with_jitter_ratio(mut self, jitter_ratio: f64) -> Self {
        self.jitter_ratio = jitter_ratio;
        self
    }
}

/// State machine calculating exponential backoff sleep intervals with uniform jitter.
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    config: BackoffConfig,
    attempt: u32,
}

impl ExponentialBackoff {
    /// Initializes a new ExponentialBackoff calculator with the specified configuration.
    pub fn new(config: BackoffConfig) -> Self {
        Self { config, attempt: 0 }
    }

    /// Increments the attempt counter and returns the next sleep duration.
    pub fn next_delay(&mut self) -> Duration {
        self.attempt = self.attempt.saturating_add(1);

        let exponent = (self.attempt - 1) as i32;
        let base_ms = (self.config.initial.as_millis() as f64) * self.config.factor.powi(exponent);
        let capped_ms = base_ms.min(self.config.max.as_millis() as f64);

        let jitter = Self::random_jitter_fraction(self.config.jitter_ratio);
        let delay_ms = (capped_ms * (1.0 + jitter)).max(0.0);

        Duration::from_millis(delay_ms as u64)
    }

    /// Resets the attempt counter back to 0 (called upon successful handshake).
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Returns the current attempt count.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Determines whether a connection or protocol failure is transient and retryable.
    ///
    /// Identifies `BrokenPipe`, `ConnectionRefused`, `ConnectionReset`, `ConnectionAborted`,
    /// `TimedOut`, `UnexpectedEof`, and EOF disconnects as retryable errors.
    pub fn is_retryable_error(err: &GridError) -> bool {
        match err {
            GridError::ConnectionClosed => true,
            GridError::ConnectionFailed(_) => true,
            GridError::Timeout { .. } => true,
            GridError::RegistrationRejected(_) => true,
            GridError::Io(io_err) => Self::is_retryable_io_error(io_err),
            _ => false,
        }
    }

    /// Checks whether an I/O error represents a transient network fault.
    pub fn is_retryable_io_error(io_err: &io::Error) -> bool {
        matches!(
            io_err.kind(),
            io::ErrorKind::BrokenPipe
                | io::ErrorKind::ConnectionRefused
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::TimedOut
                | io::ErrorKind::UnexpectedEof
                | io::ErrorKind::WouldBlock
                | io::ErrorKind::Interrupted
        )
    }

    /// Generates a uniform random float in `[-ratio, +ratio]` using `getrandom`.
    fn random_jitter_fraction(ratio: f64) -> f64 {
        let mut buf = [0u8; 4];
        if getrandom::fill(&mut buf).is_ok() {
            let val = u32::from_ne_bytes(buf);
            let unit = (val as f64) / (u32::MAX as f64); // normalized in [0.0, 1.0]
            -ratio + (2.0 * ratio * unit)
        } else {
            0.0 // deterministic fallback if random source fails
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_backoff_progression() {
        let config = BackoffConfig {
            initial: Duration::from_millis(100),
            max: Duration::from_millis(1000),
            factor: 2.0,
            jitter_ratio: 0.0, // Zero jitter for deterministic check
        };
        let mut backoff = ExponentialBackoff::new(config);

        assert_eq!(backoff.attempt(), 0);
        assert_eq!(backoff.next_delay(), Duration::from_millis(100)); // attempt 1
        assert_eq!(backoff.attempt(), 1);
        assert_eq!(backoff.next_delay(), Duration::from_millis(200)); // attempt 2
        assert_eq!(backoff.next_delay(), Duration::from_millis(400)); // attempt 3
        assert_eq!(backoff.next_delay(), Duration::from_millis(800)); // attempt 4
        assert_eq!(backoff.next_delay(), Duration::from_millis(1000)); // attempt 5 (capped)
        assert_eq!(backoff.next_delay(), Duration::from_millis(1000)); // attempt 6 (capped)

        backoff.reset();
        assert_eq!(backoff.attempt(), 0);
        assert_eq!(backoff.next_delay(), Duration::from_millis(100));
    }

    #[test]
    fn test_jitter_bounds() {
        let config = BackoffConfig {
            initial: Duration::from_millis(1000),
            max: Duration::from_secs(10),
            factor: 2.0,
            jitter_ratio: 0.20,
        };
        let mut backoff = ExponentialBackoff::new(config);

        for _ in 0..50 {
            let delay = backoff.next_delay();
            backoff.reset(); // Always check attempt 1 base = 1000ms
            let ms = delay.as_millis();
            assert!(ms >= 800, "delay {ms}ms must be >= 800ms (-20%)");
            assert!(ms <= 1200, "delay {ms}ms must be <= 1200ms (+20%)");
        }
    }

    #[test]
    fn test_retryable_error_classification() {
        assert!(ExponentialBackoff::is_retryable_error(
            &GridError::ConnectionClosed
        ));
        assert!(ExponentialBackoff::is_retryable_error(
            &GridError::Timeout {
                operation: "test".into(),
                duration_secs: 5,
            }
        ));
        assert!(ExponentialBackoff::is_retryable_error(&GridError::Io(
            io::Error::new(io::ErrorKind::ConnectionRefused, "refused")
        )));
        assert!(ExponentialBackoff::is_retryable_error(&GridError::Io(
            io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe")
        )));
        assert!(ExponentialBackoff::is_retryable_error(&GridError::Io(
            io::Error::new(io::ErrorKind::ConnectionReset, "connection reset")
        )));
    }
}

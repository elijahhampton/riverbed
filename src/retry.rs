use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Total attempts allowed, including the first.
    pub max_attempts: u32,
    /// Delay before the first retry. Doubles with each subsequent retry.
    pub base_delay: Duration,
    /// Upper bound on the delay between attempts.
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(300),
        }
    }
}

impl RetryPolicy {
    /// Delay before retrying a task whose `attempt`-th execution failed.
    pub(crate) fn backoff(&self, attempt: u32) -> Duration {
        let factor = 2u32.saturating_pow(attempt.saturating_sub(1));
        self.base_delay.saturating_mul(factor).min(self.max_delay)
    }
}

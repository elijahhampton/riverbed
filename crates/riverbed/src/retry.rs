//! Retry policy for failed tasks.

use std::time::Duration;

/// Retry limit and exponential backoff for failed tasks.
///
/// The default allows 5 attempts, with delays starting at 1 second, capped at 5 minutes, and
/// jittered.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Total attempts allowed, including the first.
    pub max_attempts: u32,
    /// Delay before the first retry. Doubles with each subsequent retry.
    pub base_delay: Duration,
    /// Upper bound on the delay between attempts.
    pub max_delay: Duration,
    /// Spreads retries over the computed delay instead of firing them together.
    ///
    /// A batch of tasks that fail for the same reason — a dependency being down — otherwise comes
    /// back at the same instant, which is the load that made it fail. With jitter each retry waits
    /// a uniformly random share of its computed delay.
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(300),
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// Delay before retrying a task whose `attempt`-th execution failed.
    pub(crate) fn backoff(&self, attempt: u32) -> Duration {
        let factor = 2u32.saturating_pow(attempt.saturating_sub(1));
        let delay = self.base_delay.saturating_mul(factor).min(self.max_delay);

        if self.jitter {
            delay.mul_f64(random_fraction())
        } else {
            delay
        }
    }
}

/// A value in `[0, 1)`.
///
/// `RandomState` is seeded randomly per instance, which is enough to spread retries and avoids a
/// dependency on an RNG for something that does not need one.
fn random_fraction() -> f64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};

    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u8(0);
    // The top 53 bits are the ones a f64 can represent exactly.
    (hasher.finish() >> 11) as f64 / (1u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_and_is_capped() {
        let policy = RetryPolicy {
            max_attempts: 10,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(8),
            jitter: false,
        };

        assert_eq!(policy.backoff(1), Duration::from_secs(1));
        assert_eq!(policy.backoff(2), Duration::from_secs(2));
        assert_eq!(policy.backoff(3), Duration::from_secs(4));
        assert_eq!(policy.backoff(4), Duration::from_secs(8));
        assert_eq!(policy.backoff(50), Duration::from_secs(8), "capped");
    }

    #[test]
    fn jitter_stays_within_the_computed_delay_and_varies() {
        let policy = RetryPolicy {
            max_attempts: 10,
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            jitter: true,
        };

        let samples: Vec<Duration> = (0..64).map(|_| policy.backoff(3)).collect();
        let ceiling = Duration::from_secs(4);
        assert!(
            samples.iter().all(|delay| *delay <= ceiling),
            "jitter must never exceed the computed delay"
        );
        assert!(
            samples
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                > 1,
            "jitter must actually vary, or it is not spreading anything"
        );
    }
}

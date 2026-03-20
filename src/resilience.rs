use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FailureBackoff {
    initial_ms: u64,
    max_ms: u64,
    failures: u32,
    jitter_salt: u64,
}

impl FailureBackoff {
    pub fn new(initial_ms: u64, max_ms: u64, jitter_salt: u64) -> Self {
        let initial_ms = initial_ms.max(1);
        let max_ms = max_ms.max(initial_ms);
        Self {
            initial_ms,
            max_ms,
            failures: 0,
            jitter_salt,
        }
    }

    pub fn on_failure(&mut self) -> u64 {
        let shift = self.failures.min(20);
        let multiplier = 1_u64 << shift;
        let delay_ms = self.initial_ms.saturating_mul(multiplier).min(self.max_ms);
        self.failures = self.failures.saturating_add(1);
        delay_ms
    }

    pub fn on_failure_with_jitter(&mut self) -> u64 {
        let base_ms = self.on_failure();
        jitter_delay_ms(base_ms, self.failures, self.jitter_salt)
    }

    pub fn on_success(&mut self) {
        self.failures = 0;
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConnectionHealth {
    pub consecutive_failures: u32,
    pub last_success_ms: Option<i64>,
    pub last_failure_ms: Option<i64>,
    pub unhealthy_since_ms: Option<i64>,
    pub last_error: Option<String>,
}

impl ConnectionHealth {
    pub fn record_success(&mut self, now_ms: i64) {
        self.consecutive_failures = 0;
        self.last_success_ms = Some(now_ms);
        self.unhealthy_since_ms = None;
        self.last_error = None;
    }

    pub fn record_failure(&mut self, now_ms: i64, unhealthy_after_failures: usize, error: String) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.last_failure_ms = Some(now_ms);
        self.last_error = Some(error);
        if unhealthy_after_failures > 0
            && self.consecutive_failures as usize >= unhealthy_after_failures
            && self.unhealthy_since_ms.is_none()
        {
            self.unhealthy_since_ms = Some(now_ms);
        }
    }

    pub fn is_unhealthy(&self) -> bool {
        self.unhealthy_since_ms.is_some()
    }
}

fn jitter_delay_ms(base_ms: u64, failures: u32, salt: u64) -> u64 {
    if base_ms <= 1 {
        return base_ms;
    }
    let spread = (base_ms / 5).max(1);
    let mix = salt
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(u64::from(failures).wrapping_mul(0xA076_1D64_78BD_642F));
    let offset = mix % (spread.saturating_mul(2).saturating_add(1));
    base_ms.saturating_sub(spread).saturating_add(offset).max(1)
}

#[cfg(test)]
mod tests {
    use super::{ConnectionHealth, FailureBackoff};

    #[test]
    fn failure_backoff_grows_with_cap_and_resets_on_success() {
        let mut backoff = FailureBackoff::new(1_000, 8_000, 0x55aa);

        assert_eq!(backoff.on_failure(), 1_000);
        assert_eq!(backoff.on_failure(), 2_000);
        assert_eq!(backoff.on_failure(), 4_000);
        assert_eq!(backoff.on_failure(), 8_000);
        assert_eq!(backoff.on_failure(), 8_000);

        backoff.on_success();
        assert_eq!(backoff.on_failure(), 1_000);
    }

    #[test]
    fn connection_health_turns_unhealthy_after_threshold_and_recovers_on_success() {
        let mut health = ConnectionHealth::default();

        health.record_failure(1_000, 3, "dial".to_string());
        health.record_failure(2_000, 3, "dial".to_string());
        assert!(!health.is_unhealthy());

        health.record_failure(3_000, 3, "dial".to_string());
        assert!(health.is_unhealthy());
        assert_eq!(health.unhealthy_since_ms, Some(3_000));

        health.record_success(4_000);
        assert!(!health.is_unhealthy());
        assert_eq!(health.consecutive_failures, 0);
        assert_eq!(health.last_success_ms, Some(4_000));
    }
}

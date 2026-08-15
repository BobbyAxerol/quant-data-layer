#[derive(Clone, Copy, Debug)]
pub struct BackoffPolicy {
    pub initial_ms: u64,
    pub maximum_ms: u64,
    pub multiplier: u32,
    pub jitter_bps: u16,
}

impl BackoffPolicy {
    pub fn validate(self) -> Result<Self, String> {
        if self.initial_ms == 0 || self.maximum_ms < self.initial_ms || self.multiplier < 1 {
            return Err("invalid backoff bounds".into());
        }
        if self.jitter_bps > 10_000 {
            return Err("jitter_bps must not exceed 10000".into());
        }
        Ok(self)
    }

    pub fn delay_ms(self, attempt: u32, jitter_basis: u16) -> u64 {
        let exponential = self
            .initial_ms
            .saturating_mul(u64::from(self.multiplier).saturating_pow(attempt))
            .min(self.maximum_ms);
        let bounded_basis = u64::from(jitter_basis.min(10_000));
        let jitter_window = exponential.saturating_mul(u64::from(self.jitter_bps)) / 10_000;
        (exponential.saturating_sub(jitter_window)
            + jitter_window
                .saturating_mul(2)
                .saturating_mul(bounded_basis)
                / 10_000)
            .min(self.maximum_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::BackoffPolicy;

    #[test]
    fn caps_exponential_delay_and_bounds_jitter() {
        let policy = BackoffPolicy {
            initial_ms: 100,
            maximum_ms: 1_000,
            multiplier: 2,
            jitter_bps: 2_000,
        }
        .validate()
        .unwrap();
        assert_eq!(policy.delay_ms(0, 0), 80);
        assert_eq!(policy.delay_ms(10, 10_000), 1_000);
    }
}

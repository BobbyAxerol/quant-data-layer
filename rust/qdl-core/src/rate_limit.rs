#[derive(Clone, Debug)]
pub struct TokenBucket {
    capacity: u64,
    tokens: u64,
    refill_tokens: u64,
    refill_interval_ns: i64,
    last_refill_ns: i64,
}

impl TokenBucket {
    pub fn new(
        capacity: u64,
        refill_tokens: u64,
        refill_interval_ns: i64,
        now_ns: i64,
    ) -> Result<Self, String> {
        if capacity == 0 || refill_tokens == 0 || refill_interval_ns <= 0 {
            return Err("token bucket values must be positive".into());
        }
        Ok(Self {
            capacity,
            tokens: capacity,
            refill_tokens,
            refill_interval_ns,
            last_refill_ns: now_ns,
        })
    }

    pub fn try_acquire(&mut self, amount: u64, now_ns: i64) -> bool {
        self.refill(now_ns);
        if amount == 0 || amount > self.tokens {
            return false;
        }
        self.tokens -= amount;
        true
    }

    fn refill(&mut self, now_ns: i64) {
        let elapsed = now_ns.saturating_sub(self.last_refill_ns);
        let periods = elapsed / self.refill_interval_ns;
        if periods > 0 {
            self.tokens = self
                .tokens
                .saturating_add((periods as u64).saturating_mul(self.refill_tokens))
                .min(self.capacity);
            self.last_refill_ns += periods * self.refill_interval_ns;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TokenBucket;

    #[test]
    fn enforces_budget_and_deterministic_refill() {
        let mut bucket = TokenBucket::new(2, 1, 1_000, 0).unwrap();
        assert!(bucket.try_acquire(2, 0));
        assert!(!bucket.try_acquire(1, 999));
        assert!(bucket.try_acquire(1, 1_000));
    }
}

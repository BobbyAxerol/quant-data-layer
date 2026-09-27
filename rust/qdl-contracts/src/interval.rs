//! The canonical interval duration (single owner:
//! `qdl.adapters.intervals.canonical_interval_ms`), shared by the stream
//! gateway (warmup quota) and the projector (BAR buckets).

/// `qdl.adapters.intervals.canonical_interval_ms` for the canonical
/// lowercase `<count><unit>` spelling (calendar months are refused).
pub fn canonical_interval_ms(interval: &str) -> Result<u64, String> {
    let value = interval.trim();
    if value.is_empty() {
        return Err("canonical interval is required".into());
    }
    if value.ends_with('M') {
        return Err(format!(
            "calendar-month bars have no fixed duration and are not canonical intervals; \
             'M' is never folded into minutes: {interval:?}"
        ));
    }
    if value != value.to_lowercase() {
        return Err(format!(
            "canonical interval must be lowercase, venue spelling is derived: {interval:?}"
        ));
    }
    let (count, unit) = value.split_at(value.len() - 1);
    let unit_ms: u64 = match unit {
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        "w" => 604_800_000,
        _ => {
            return Err(format!(
                "canonical interval must use a fixed s/m/h/d/w duration: {interval:?}"
            ))
        }
    };
    let count: u64 = count
        .parse()
        .map_err(|_| format!("canonical interval count must be an integer: {interval:?}"))?;
    if count == 0 {
        return Err(format!("canonical interval must be positive: {interval:?}"));
    }
    count
        .checked_mul(unit_ms)
        .ok_or_else(|| format!("canonical interval is too large: {interval:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_intervals_have_a_duration_and_months_are_refused() {
        assert_eq!(canonical_interval_ms("1m"), Ok(60_000));
        assert_eq!(canonical_interval_ms("3d"), Ok(259_200_000));
        assert_eq!(canonical_interval_ms("1w"), Ok(604_800_000));
        for bad in ["1M", "1H", "0m", "", "5x", "m"] {
            assert!(canonical_interval_ms(bad).is_err(), "{bad:?}");
        }
    }
}

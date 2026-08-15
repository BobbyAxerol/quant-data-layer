use qdl_contracts::qdl::common::v1::{decimal_value, DecimalValue};

pub fn parse_decimal(source: &str) -> Result<DecimalValue, String> {
    let source = source.trim();
    if source.is_empty() {
        return Err("decimal value is required".into());
    }
    let (negative, unsigned) = match source.as_bytes()[0] {
        b'-' => (true, &source[1..]),
        b'+' => (false, &source[1..]),
        _ => (false, source),
    };
    let (base, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((base, exponent)) => (
            base,
            exponent
                .parse::<i32>()
                .map_err(|_| "invalid decimal exponent")?,
        ),
        None => (unsigned, 0),
    };
    let (whole, fraction) = base.split_once('.').unwrap_or((base, ""));
    if whole.is_empty() && fraction.is_empty() {
        return Err("invalid decimal digits".into());
    }
    if !whole
        .bytes()
        .chain(fraction.bytes())
        .all(|byte| byte.is_ascii_digit())
    {
        return Err("invalid decimal digits".into());
    }
    let mut digits = format!("{whole}{fraction}");
    let mut scale = fraction.len() as i32 - exponent;
    if scale < 0 {
        digits.push_str(&"0".repeat((-scale) as usize));
        scale = 0;
    }
    let trimmed = digits.trim_start_matches('0');
    let coefficient_text = if trimmed.is_empty() { "0" } else { trimmed };
    let signed_text = if negative && coefficient_text != "0" {
        format!("-{coefficient_text}")
    } else {
        coefficient_text.to_owned()
    };
    let coefficient = match signed_text.parse::<i64>() {
        Ok(value) => decimal_value::Coefficient::Mantissa(value),
        Err(_) => decimal_value::Coefficient::MantissaText(signed_text),
    };
    Ok(DecimalValue {
        coefficient: Some(coefficient),
        scale,
        source_text: source.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::parse_decimal;
    use qdl_contracts::qdl::common::v1::decimal_value;

    #[test]
    fn preserves_scale_and_handles_exponent_and_overflow() {
        let exact = parse_decimal("61234.10").unwrap();
        assert_eq!(exact.scale, 2);
        assert_eq!(exact.source_text, "61234.10");
        assert_eq!(
            exact.coefficient,
            Some(decimal_value::Coefficient::Mantissa(6_123_410))
        );

        let exponent = parse_decimal("1.25e2").unwrap();
        assert_eq!(exponent.scale, 0);
        assert_eq!(
            exponent.coefficient,
            Some(decimal_value::Coefficient::Mantissa(125))
        );

        let overflow = parse_decimal("123456789012345678901.123").unwrap();
        assert!(matches!(
            overflow.coefficient,
            Some(decimal_value::Coefficient::MantissaText(_))
        ));
    }
}

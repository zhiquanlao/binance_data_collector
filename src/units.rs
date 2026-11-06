use anyhow::{Context, anyhow, bail};
use sonic_rs::{JsonContainerTrait, JsonValueTrait};

/// Exact nonnegative decimal, represented as coefficient / 10^scale.
#[derive(Clone, Copy, Debug)]
pub struct Step {
    coefficient: u128,
    scale: u32,
}

fn decimal(value: &str) -> anyhow::Result<(u128, u32)> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|b| b.is_ascii_digit())
    {
        bail!("invalid nonnegative decimal: {value}");
    }
    let scale = u32::try_from(fraction.len())?;
    if scale > 18 {
        bail!("decimal scale exceeds 18: {value}");
    }
    let coefficient = whole
        .bytes()
        .chain(fraction.bytes())
        .try_fold(0u128, |n, b| {
            n.checked_mul(10)
                .and_then(|n| n.checked_add((b - b'0') as u128))
                .ok_or_else(|| anyhow!("decimal overflow: {value}"))
        })?;
    Ok((coefficient, scale))
}

impl Step {
    pub fn new(value: &str) -> anyhow::Result<Self> {
        let (coefficient, scale) = decimal(value)?;
        if coefficient == 0 {
            bail!("zero step");
        }
        Ok(Self { coefficient, scale })
    }

    pub fn units(self, value: &str) -> anyhow::Result<i64> {
        let (coefficient, scale) = decimal(value)?;
        let common = scale.max(self.scale);
        let numerator = coefficient
            .checked_mul(10u128.pow(common - scale))
            .context("value overflow")?;
        let denominator = self
            .coefficient
            .checked_mul(10u128.pow(common - self.scale))
            .context("step overflow")?;
        if numerator % denominator != 0 {
            bail!("{value} is not an exact multiple of {self:?}");
        }
        i64::try_from(numerator / denominator).context("unit count exceeds i64")
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Units {
    pub price: Step,
    pub size: Step,
}

impl Units {
    pub fn from_symbol(symbol: &sonic_rs::Value) -> anyhow::Result<Self> {
        let filters = symbol
            .get("filters")
            .and_then(|v| v.as_array())
            .context("missing filters")?;
        let step = |filter: &str, field: &str| -> anyhow::Result<Step> {
            let value = filters
                .iter()
                .find(|v| v.get("filterType").and_then(|v| v.as_str()) == Some(filter))
                .and_then(|v| v.get(field))
                .and_then(|v| v.as_str())
                .context("missing symbol step")?;
            Step::new(value)
        };
        Ok(Self {
            price: step("PRICE_FILTER", "tickSize")?,
            size: step("LOT_SIZE", "stepSize")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_units_no_float_or_rounding() {
        for (step, value, expected) in [
            ("0.10", "75973.70", 759737),
            ("0.001", "3.296", 3296),
            ("0.0001", "0.0123", 123),
            ("0.0100", "123.450000", 12345),
            ("0.25", "1.75", 7),
            ("0.1", "0", 0),
            ("0.00000001", "90071992.54740993", 9007199254740993),
        ] {
            assert_eq!(Step::new(step).unwrap().units(value).unwrap(), expected);
        }
        assert!(Step::new("0.1").unwrap().units("1.01").is_err());
        assert!(Step::new("0.001").unwrap().units("-1").is_err());
        assert!(Step::new("0.001").unwrap().units("NaN").is_err());
        assert!(
            Step::new("0.00000001")
                .unwrap()
                .units("999999999999")
                .is_err()
        );
    }
}

#[cfg(test)]
mod live_fixture {
    use super::*;
    #[test]
    fn vm_decimal_reference_fixture() {
        let Ok(path) = std::env::var("BINANCE_UNIT_FIXTURE") else {
            return;
        };
        let bytes = std::fs::read(path).unwrap();
        let cases: Vec<serde_json::Value> = serde_json::from_slice(&bytes).unwrap();
        assert!(cases.len() >= 1000, "live fixture too small");
        for case in &cases {
            let got = Step::new(case["step"].as_str().unwrap())
                .unwrap()
                .units(case["value"].as_str().unwrap())
                .unwrap();
            assert_eq!(got, case["expected"].as_i64().unwrap(), "{case}");
        }
        println!(
            "Verified {} live decimal values against Python Decimal reference",
            cases.len()
        );
    }
}

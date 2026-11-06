use sonic_rs::JsonValueTrait;

#[derive(Debug, Default, Clone, Copy)]
pub struct TradeSummary {
    pub event_time_ms: u64,
    pub transaction_time_ms: u64,
    pub price: i64,
    pub quantity: i64,
    pub trade_id: u64,
    pub is_buyer_maker: bool,
}

/// Parse one raw or combined-stream futures trade message. Do not collapse messages.
pub fn parse_trade_message(
    v: &sonic_rs::Value,
    units: crate::units::Units,
) -> anyhow::Result<TradeSummary> {
    let v = v.get("data").unwrap_or(v);
    if v.get("e").and_then(|x| x.as_str()) != Some("trade") {
        anyhow::bail!("not a trade event");
    }
    Ok(TradeSummary {
        event_time_ms: v
            .get("E")
            .and_then(|x| x.as_u64())
            .ok_or_else(|| anyhow::anyhow!("missing trade E"))?,
        transaction_time_ms: v
            .get("T")
            .and_then(|x| x.as_u64())
            .ok_or_else(|| anyhow::anyhow!("missing trade T"))?,
        price: units.price.units(
            v.get("p")
                .and_then(|x| x.as_str())
                .ok_or_else(|| anyhow::anyhow!("missing trade p"))?,
        )?,
        quantity: units.size.units(
            v.get("q")
                .and_then(|x| x.as_str())
                .ok_or_else(|| anyhow::anyhow!("missing trade q"))?,
        )?,
        trade_id: v
            .get("t")
            .and_then(|x| x.as_u64())
            .ok_or_else(|| anyhow::anyhow!("missing trade t"))?,
        is_buyer_maker: v
            .get("m")
            .and_then(|x| x.as_bool())
            .ok_or_else(|| anyhow::anyhow!("missing trade m"))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn units() -> crate::units::Units {
        crate::units::Units {
            price: crate::units::Step::new("0.1").unwrap(),
            size: crate::units::Step::new("0.001").unwrap(),
        }
    }

    #[test]
    fn parses_raw_and_combined_trade_payloads_without_aggregate_fields() {
        let raw = r#"{"e":"trade","E":2000,"T":1999,"s":"BTCUSDT","t":8244368766,"p":"75850.20","q":"0.131","X":"MARKET","m":false,"st":1}"#;
        for json in [
            raw.to_string(),
            format!(r#"{{"stream":"btcusdt@trade","data":{raw}}}"#),
        ] {
            let v = sonic_rs::from_str(&json).unwrap();
            let trade = parse_trade_message(&v, units()).unwrap();
            assert_eq!(
                (trade.event_time_ms, trade.transaction_time_ms),
                (2000, 1999)
            );
            assert_eq!(trade.trade_id, 8244368766);
            assert_eq!(
                (trade.price, trade.quantity, trade.is_buyer_maker),
                (758502, 131, false)
            );
        }
    }

    #[test]
    fn rejects_trade_without_transaction_time() {
        let v = sonic_rs::from_str(r#"{"e":"trade","E":2000}"#).unwrap();
        assert_eq!(
            parse_trade_message(&v, units()).unwrap_err().to_string(),
            "missing trade T"
        );
    }
}

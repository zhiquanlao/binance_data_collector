#![allow(non_snake_case)]

use sonic_rs::{JsonContainerTrait, JsonValueTrait};

#[derive(Debug, Default, Clone, Copy)]
pub struct AggTradeSummary {
    // Event time (E field)
    pub event_time_ms: u64,
    
    // Price (p field)
    pub price: f64,
    
    // Quantity (q field) - with all market trades
    pub quantity: f64,
    
    // Normal quantity (nq field) - without trades involving RPI orders
    pub normal_quantity: f64,
    
    // First trade ID (f field)
    pub first_trade_id: u64,
    
    // Last trade ID (l field)
    pub last_trade_id: u64,
    
    // Is buyer the market maker? (m field)
    pub is_buyer_maker: bool,
}


/// Accepts either:
/// - raw aggTrade payload
/// - combined stream payload: {"stream":"...","data":<payload>}
pub fn summarize_aggtrade_message(v: &sonic_rs::Value) -> AggTradeSummary {
    let payload = v.get("data").unwrap_or(v);
    if let Some(arr) = payload.as_array() {
        summarize_arr(arr)
    } else {
        summarize_one(payload)
    }
}

fn summarize_arr(arr: &[sonic_rs::Value]) -> AggTradeSummary {
    // For array of aggTrade messages, use the last one's values
    // (following the pattern of keeping last element's values)
    if let Some(last) = arr.last() {
        summarize_one(last)
    } else {
        AggTradeSummary::default()
    }
}

fn summarize_one(v: &sonic_rs::Value) -> AggTradeSummary {
    // Binance aggTrade fields:
    // E: event time (u64)
    // p: price (string)
    // q: quantity (string) - with all market trades
    // nq: normal quantity (string) - without trades involving RPI orders
    // f: first trade ID (u64)
    // l: last trade ID (u64)
    // T: trade time (u64)
    // m: is buyer market maker (bool)
    let E = v.get("E").and_then(|x| x.as_u64()).unwrap_or(0);
    let p_s = v.get("p").and_then(|x| x.as_str()).unwrap_or("0");
    let q_s = v.get("q").and_then(|x| x.as_str()).unwrap_or("0");
    let nq_s = v.get("nq").and_then(|x| x.as_str()).unwrap_or("0");
    let f = v.get("f").and_then(|x| x.as_u64()).unwrap_or(0);
    let l = v.get("l").and_then(|x| x.as_u64()).unwrap_or(0);
    let m = v.get("m").and_then(|x| x.as_bool()).unwrap_or(false);

    let px = p_s.parse::<f64>().unwrap_or(0.0);
    let qty = q_s.parse::<f64>().unwrap_or(0.0);
    let nqty = nq_s.parse::<f64>().unwrap_or(0.0);

    AggTradeSummary {
        event_time_ms: E,
        price: px,
        quantity: qty,
        normal_quantity: nqty,
        first_trade_id: f,
        last_trade_id: l,
        is_buyer_maker: m,
    }
}



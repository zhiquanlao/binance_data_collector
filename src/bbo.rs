use crate::{orderbook::TopN, units::Units};
use anyhow::Context;
use sonic_rs::JsonValueTrait;

#[derive(Clone, Copy, Debug, Default)]
pub struct Quote {
    pub bid: i64,
    pub ask: i64,
    pub bid_size: i64,
    pub ask_size: i64,
    pub update_id: u64,
    pub is_bbo: bool,
}
impl Quote {
    pub fn from_top(top: &TopN, update_id: u64) -> Self {
        Self {
            bid: top.bid_px[0],
            ask: top.ask_px[0],
            bid_size: top.bid_sz[0],
            ask_size: top.ask_sz[0],
            update_id,
            is_bbo: false,
        }
    }
    pub fn prices(self) -> Option<(i64, i64)> {
        (self.bid > 0 && self.ask > 0).then_some((self.bid, self.ask))
    }
    pub fn changed_from(self, previous: Option<(i64, i64)>) -> bool {
        self.prices().is_some() && self.prices() != previous
    }
    /// Both depth and bookTicker use the exchange order-book update ID.
    /// Prefer BBO on a tie so a depth message cannot replace the same-ID BBO.
    pub fn update_if_newer(&mut self, candidate: Self) -> bool {
        let fresh = candidate.update_id > self.update_id
            || (candidate.update_id == self.update_id && (candidate.is_bbo || !self.is_bbo));
        if fresh && candidate.prices().is_some() {
            *self = candidate;
            return true;
        }
        false
    }
}
pub fn parse_quote(v: &sonic_rs::Value, units: Units) -> anyhow::Result<Quote> {
    let field = |key| {
        v.get(key)
            .and_then(|v| v.as_str())
            .context("missing BBO price/size")
    };
    Ok(Quote {
        bid: units.price.units(field("b")?)?,
        ask: units.price.units(field("a")?)?,
        bid_size: units.size.units(field("B")?)?,
        ask_size: units.size.units(field("A")?)?,
        update_id: v
            .get("u")
            .and_then(|v| v.as_u64())
            .context("missing BBO u")?,
        is_bbo: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn price_filter_ignores_size_only_changes_but_writes_same_midpoint_spread_changes() {
        let old = Quote {
            bid: 100,
            ask: 103,
            bid_size: 1,
            ask_size: 2,
            update_id: 10,
            is_bbo: true,
        };
        assert!(
            !Quote {
                bid_size: 999,
                ..old
            }
            .changed_from(old.prices())
        );
        assert!(
            Quote {
                bid: 101,
                ask: 102,
                ..old
            }
            .changed_from(old.prices())
        );
        assert!(Quote { ask: 104, ..old }.changed_from(old.prices()));
        assert!(old.changed_from(None));
        assert!(!Quote::default().changed_from(None));
    }
    #[test]
    fn stale_depth_cannot_replace_bbo_and_repeat_bbo_does_not_emit() {
        let mut latest = Quote {
            bid: 100,
            ask: 103,
            update_id: 10,
            is_bbo: true,
            ..Quote::default()
        };
        let last_written = latest.prices();
        assert!(!latest.update_if_newer(Quote {
            bid: 99,
            update_id: 9,
            is_bbo: false,
            ..latest
        }));
        assert!(!latest.update_if_newer(Quote {
            bid: 99,
            update_id: 10,
            is_bbo: false,
            ..latest
        }));
        assert!(latest.update_if_newer(Quote {
            bid_size: 999,
            update_id: 11,
            ..latest
        }));
        assert!(!latest.changed_from(last_written));
        // A newer depth does update the effective quote and becomes the next baseline.
        assert!(latest.update_if_newer(Quote {
            bid: 101,
            update_id: 12,
            is_bbo: false,
            ..latest
        }));
        let last_written = latest.prices();
        assert!(latest.update_if_newer(Quote {
            update_id: 13,
            is_bbo: true,
            ..latest
        }));
        assert!(!latest.changed_from(last_written));
    }
}

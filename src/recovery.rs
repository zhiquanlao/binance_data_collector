use std::time::Duration;

pub const BRIDGE_TIMEOUT: Duration = Duration::from_secs(10);
pub const MAX_BUFFERED_DEPTH: usize = 8192;

#[derive(Debug, PartialEq)]
pub enum Bridge {
    Waiting,
    Ready(usize),
    Reconnect,
}

/// Futures bridge: U <= snapshot ID <= u. Older events may be discarded.
pub fn bridge_status(
    snapshot: u64,
    events: impl IntoIterator<Item = (u64, u64)>,
    elapsed: Duration,
) -> Bridge {
    for (index, (first, last)) in events.into_iter().enumerate() {
        if last < snapshot {
            continue;
        }
        if first <= snapshot {
            return Bridge::Ready(index);
        }
        return Bridge::Reconnect;
    }
    if elapsed >= BRIDGE_TIMEOUT {
        Bridge::Reconnect
    } else {
        Bridge::Waiting
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn requires_valid_bridge_and_reconnects_on_gap_or_timeout() {
        assert_eq!(
            bridge_status(100, [(80, 90), (95, 100)], Duration::ZERO),
            Bridge::Ready(1)
        );
        assert_eq!(
            bridge_status(100, [(100, 110)], Duration::ZERO),
            Bridge::Ready(0)
        );
        assert_eq!(
            bridge_status(100, [(101, 110)], Duration::ZERO),
            Bridge::Reconnect
        );
        assert_eq!(
            bridge_status(100, [(80, 90)], Duration::ZERO),
            Bridge::Waiting
        );
        assert_eq!(bridge_status(100, [], BRIDGE_TIMEOUT), Bridge::Reconnect);
        assert_eq!(
            bridge_status(100, [(80, 90)], BRIDGE_TIMEOUT),
            Bridge::Reconnect
        );
    }
}

/// Keep recoverable message failures inside the collector's retry lifecycle.
pub fn recover_on_error<T>(
    result: anyhow::Result<T>,
    requested: &std::cell::Cell<bool>,
    context: &str,
) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(error) => {
            eprintln!("[recovery] {context}: {error:#}; refresh filters and reconnect");
            requested.set(true);
            None
        }
    }
}

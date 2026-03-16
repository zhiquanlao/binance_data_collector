#![allow(dead_code)]

use anyhow::{Context, anyhow};
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use rustls::pki_types::ServerName;
use sonic_rs::{JsonContainerTrait, JsonValueTrait};
use std::io::{Read, Write};
use std::{mem::MaybeUninit,ptr};
use std::sync::Arc;

pub type Price = f64;
pub type Volume = f64;

#[derive(Debug, Clone, Copy)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy)]
struct PriceLevel {
    price: Price,
    size: Volume,
}

impl PriceLevel {
    #[inline(always)]
    fn new(price: Price, size: Volume) -> Self {
        Self { price, size }
    }
}

// Keep more than 10 so that deletions at top-of-book can be replenished
// without needing the full 5000-level local book.
pub const MAX_LEVELS: usize = 512;

/// Fixed-depth L2 view (keeps best `MAX_LEVELS` levels per side).
pub struct OrderBookL2<const N: usize> {
    pub ts_ms: u64,
    pub bids: [MaybeUninit<PriceLevel>; N],
    pub asks: [MaybeUninit<PriceLevel>; N],
    pub bid_depth: usize,
    pub ask_depth: usize,
}

impl<const N: usize> OrderBookL2<N> {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            ts_ms: 0,
            bids: [MaybeUninit::uninit(); N],
            asks: [MaybeUninit::uninit(); N],
            bid_depth: 0,
            ask_depth: 0,
        }
    }

    #[inline(always)]
    pub fn capacity(&self) -> usize {
        N
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.bid_depth = 0;
        self.ask_depth = 0;
    }


    #[inline(always)]
    pub fn bid_depth(&self) -> usize {
        self.bid_depth
    }

    #[inline(always)]
    pub fn ask_depth(&self) -> usize {
        self.ask_depth
    }

    #[inline(always)]
    pub fn best_bid(&self) -> Option<Price> {
        if self.bid_depth == 0 {
            return None;
        }
        Some(unsafe { self.bids[self.bid_depth - 1].assume_init_ref().price })
    }

    #[inline(always)]
    pub fn best_ask(&self) -> Option<Price> {
        if self.ask_depth == 0 {
            return None;
        }
        Some(unsafe { self.asks[self.ask_depth - 1].assume_init_ref().price })
    }

    /// Maintains sorted levels (worst → best, best at end).
    pub fn update_level(&mut self, side: Side, price: Price, size: Volume) {
        const PRICE_EPS: f64 = 1e-12;
        const SIZE_EPS: f64 = 0.0;
    
        // If NaNs can appear, decide your policy. This avoids totally broken ordering.
        debug_assert!(price.is_finite());
        debug_assert!(size.is_finite());
    
        let (levels, depth) = match side {
            Side::Bid => (&mut self.bids, &mut self.bid_depth),
            Side::Ask => (&mut self.asks, &mut self.ask_depth),
        };
    
        let n = *depth;
    
        // Find either:
        //  - exact match (within eps), or
        //  - insertion point (first element that should come *after* incoming)
        let mut ins = n;
        for i in 0..n {
            let px = unsafe { levels[i].assume_init_ref().price };
    
            if (px - price).abs() < PRICE_EPS {
                // found existing
                if size <= SIZE_EPS {
                    // delete: shift left
                    unsafe {
                        let base = levels.as_mut_ptr();
                        ptr::copy(base.add(i + 1), base.add(i), n - i - 1);
                    }
                    *depth -= 1;
    
                    // Optional: enforce ">= depth is uninit" invariant
                    levels[*depth] = MaybeUninit::uninit();
                } else {
                    // update only: now take &mut safely (no shifting)
                    unsafe { levels[i].assume_init_mut().size = size; }
                }
                return;
            }
    
            let should_insert_before = match side {
                Side::Bid => px > price + PRICE_EPS, // bids are ascending (worst->best)
                Side::Ask => px < price - PRICE_EPS, // asks are descending (worst->best)
            };
    
            if should_insert_before {
                ins = i;
                break;
            }
        }
    
        // Not found: insert unless delete
        if size <= SIZE_EPS {
            return;
        }
    
        // If full and not better than worst, ignore
        if n == MAX_LEVELS {
            let worst_px = unsafe { levels[0].assume_init_ref().price };
            let incoming_is_better = match side {
                Side::Bid => price > worst_px + PRICE_EPS,
                Side::Ask => price < worst_px - PRICE_EPS,
            };
            if !incoming_is_better {
                return;
            }
    
            // Drop worst: shift left by 1 (depth stays MAX_LEVELS)
            unsafe {
                let base = levels.as_mut_ptr();
                ptr::copy(base.add(1), base.add(0), n - 1);
            }
    
            // After dropping worst, insertion index shifts by -1 (clamped at 0)
            ins = ins.saturating_sub(1);
            // And now we insert into range [0..n-1], with last slot free.
            unsafe {
                let base = levels.as_mut_ptr();
                ptr::copy(base.add(ins), base.add(ins + 1), (n - 1) - ins);
            }
            levels[ins] = MaybeUninit::new(PriceLevel::new(price, size));
            return;
        }
    
        // Has capacity: shift right to open hole at ins
        unsafe {
            let base = levels.as_mut_ptr();
            ptr::copy(base.add(ins), base.add(ins + 1), n - ins);
        }
        levels[ins] = MaybeUninit::new(PriceLevel::new(price, size));
        *depth += 1;
    }
    
    pub fn levels_best_to_worst(&self, side: Side) -> Vec<(f64, f64)> {
        let (arr, depth) = match side {
            Side::Bid => (&self.bids, self.bid_depth),
            Side::Ask => (&self.asks, self.ask_depth),
        };
        let mut out = Vec::with_capacity(depth);
        for i in 0..depth {
            let lvl = unsafe { arr[i].assume_init_ref() };
            out.push((lvl.price, lvl.size));
        }
        out.reverse(); // best first
        out
    }

    pub fn top_n_arrays(&self, n: usize) -> TopN {
        let n = n.min(MAX_LEVELS);
        let bids = self.levels_best_to_worst(Side::Bid);
        let asks = self.levels_best_to_worst(Side::Ask);

        let mut out = TopN::default();
        for i in 0..n {
            if let Some((p, q)) = bids.get(i) {
                out.bid_px[i] = *p;
                out.bid_sz[i] = *q;
            }
            if let Some((p, q)) = asks.get(i) {
                out.ask_px[i] = *p;
                out.ask_sz[i] = *q;
            }
        }
        out
    }
    #[inline(always)]
    pub fn new_from_snapshot(
        ts_ms: u64,
        bids_best_to_worst: &[(f64, f64)],
        asks_best_to_worst: &[(f64, f64)],
    ) -> Self {
        let mut ob = Self::new();
        ob.from_snapshot(ts_ms, bids_best_to_worst, asks_best_to_worst);
        ob
    }

    #[inline(always)]
    pub fn from_snapshot(
        &mut self,
        ts_ms: u64,
        bids_best_to_worst: &[(f64, f64)],
        asks_best_to_worst: &[(f64, f64)],
    ) {
        const SIZE_EPS: f64 = 0.0;

        self.ts_ms = ts_ms;
        self.bid_depth = 0;
        self.ask_depth = 0;

        if N == 0 {
            return;
        }

        // Bids
        {
            let take = bids_best_to_worst.len().min(N);
            let mut out = 0usize;
            for &(px, sz) in bids_best_to_worst[..take].iter().rev() {
                if sz <= SIZE_EPS {
                    continue;
                }
                self.bids[out] = MaybeUninit::new(PriceLevel::new(px, sz));
                out += 1;
            }
            self.bid_depth = out;
            
            // // Debug: print what was stored
            // if self.bid_depth > 0 {
            //     let best_bid_stored = unsafe { self.bids[self.bid_depth - 1].assume_init_ref() };
            //     println!("[FROM_SNAPSHOT] Bids: input_len={}, took={}, stored={}, best_bid_stored={} @ size={}",
            //              bids_best_to_worst.len(), take, out, best_bid_stored.price, best_bid_stored.size);
            // }
        }

        // Asks
        {
            let take = asks_best_to_worst.len().min(N);
            let mut out = 0usize;
            for &(px, sz) in asks_best_to_worst[..take].iter().rev() {
                if sz <= SIZE_EPS {
                    continue;
                }
                self.asks[out] = MaybeUninit::new(PriceLevel::new(px, sz));
                out += 1;
            }
            self.ask_depth = out;
            
            // // Debug: print what was stored
            // if self.ask_depth > 0 {
            //     let best_ask_stored = unsafe { self.asks[self.ask_depth - 1].assume_init_ref() };
            //     println!("[FROM_SNAPSHOT] Asks: input_len={}, took={}, stored={}, best_ask_stored={} @ size={}",
            //              asks_best_to_worst.len(), take, out, best_ask_stored.price, best_ask_stored.size);
            // }
        }
    }

}

#[derive(Debug, Default, Clone, Copy)]
pub struct TopN {
    pub bid_px: [f64; 10],
    pub ask_px: [f64; 10],
    pub bid_sz: [f64; 10],
    pub ask_sz: [f64; 10],
}

/// Apply a Binance depthUpdate (bids/asks arrays) to a local fixed-depth book.
///
/// Sequencing/contiguity checks should be enforced by the caller (ws_connector style).
///
/// Note: Binance sends prices in ascending order. To apply best price first:
/// - Bids: best bid is highest price (at end of ascending list) -> iterate in reverse
/// - Asks: best ask is lowest price (at start of ascending list) -> iterate forward
#[inline(always)]
pub fn on_binance_depth_update<const N: usize>(
    symbol: &str,
    binance_orderbook: &mut OrderBookL2<N>,
    U: u64,
    u: u64,
    pu: Option<u64>,
    bids: &[(f64, f64)],
    asks: &[(f64, f64)],
    event_time: u64,
) {
    // If you enforce contiguity in the caller, keep this function pure & fast.
    // Still consume the params to avoid warnings (and you may want to log on debug builds).
    let _ = (symbol, U, u, pu, event_time);

    // Apply bids (desc) and asks (asc). We don't rely on order in the arrays;
    // OrderBookL2 will re-position as needed.
    // NOTE: If Price/Volume are not f64 aliases, cast/convert accordingly.
    for &(px, qty) in bids {
        binance_orderbook.update_level(Side::Bid, px as Price, qty as Volume);
    }
    for &(px, qty) in asks {
        binance_orderbook.update_level(Side::Ask, px as Price, qty as Volume);
    }
    binance_orderbook.ts_ms = event_time;
    // let bids = binance_orderbook.levels_as_real(Side::Bid);
    // let asks = binance_orderbook.levels_as_real(Side::Ask);
    // println!("Current Binance Order Book bids: {:?}",bids);
    // println!("Current Binance Order Book asks: {:?}",asks);
}

// ---------------- REST snapshot parsing (blocking TLS HTTP GET) ----------------

fn tls_http_get(host: &'static str, path: &str, timeout_ms: u64) -> anyhow::Result<Vec<u8>> {
    let roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    let server_name =
        ServerName::try_from(host).map_err(|_| anyhow!("invalid DNS name: {host}"))?;
    let conn = ClientConnection::new(Arc::new(cfg), server_name)?;

    let tcp = std::net::TcpStream::connect((host, 443))?;
    tcp.set_read_timeout(Some(std::time::Duration::from_millis(timeout_ms)))?;
    tcp.set_write_timeout(Some(std::time::Duration::from_millis(timeout_ms)))?;

    let mut tls = StreamOwned::new(conn, tcp);
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nAccept: application/json\r\n\r\n"
    );
    tls.write_all(req.as_bytes())?;
    tls.flush()?;

    let mut buf = Vec::with_capacity(128 * 1024);
    let mut tmp = [0u8; 8192];
    loop {
        let n = tls.read(&mut tmp)?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }

    let sep = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| anyhow!("bad HTTP response (no CRLFCRLF)"))?;
    Ok(buf[sep + 4..].to_vec())
}

pub fn fetch_depth_snapshot_blocking(
    host: &'static str,
    path: &str,
    symbol: &str,
    limit: u32,
) -> anyhow::Result<(u64, Vec<(f64, f64)>, Vec<(f64, f64)>)> {
    let (last, bids, asks, _body) = fetch_depth_snapshot_blocking_with_body(host, path, symbol, limit)?;
    Ok((last, bids, asks))
}

pub fn fetch_depth_snapshot_blocking_with_body(
    host: &'static str,
    path: &str,
    symbol: &str,
    limit: u32,
) -> anyhow::Result<(u64, Vec<(f64, f64)>, Vec<(f64, f64)>, Vec<u8>)> {
    let sym_u = symbol.to_ascii_uppercase();
    let full_path = format!("{path}?symbol={sym_u}&limit={limit}");
    let body = tls_http_get(host, &full_path, 2500).context("tls_http_get")?;
    let body_snip = {
        const LIM: usize = 1024;
        let n = body.len().min(LIM);
        String::from_utf8_lossy(&body[..n]).to_string()
    };

    let v: sonic_rs::Value = sonic_rs::from_slice(&body).map_err(|e| {
        anyhow!(
            "parse depth snapshot json failed: {e}; host={host} path={full_path}; body_snip={body_snip:?}"
        )
    })?;

    let last = v
        .get("lastUpdateId")
        .and_then(|x| x.as_u64())
        .ok_or_else(|| {
            anyhow!(
                "missing lastUpdateId; host={host} path={full_path}; body_snip={body_snip:?}"
            )
        })?;

    fn parse_side(v: &sonic_rs::Value, name: &str) -> anyhow::Result<Vec<(f64, f64)>> {
        let arr = v
            .get(name)
            .and_then(|x| x.as_array())
            .ok_or_else(|| anyhow!("missing {name}"))?;
        let mut out = Vec::with_capacity(arr.len());
        for row in arr {
            let px = row
                .get(0)
                .and_then(|x| x.as_str())
                .unwrap_or("0")
                .parse::<f64>()
                .unwrap_or(0.0);
            let q = row
                .get(1)
                .and_then(|x| x.as_str())
                .unwrap_or("0")
                .parse::<f64>()
                .unwrap_or(0.0);
            out.push((px, q));
        }
        Ok(out)
    }

    let bids = parse_side(&v, "bids")
        .with_context(|| format!("snapshot parse bids; host={host} path={full_path}; body_snip={body_snip:?}"))?;
    let asks = parse_side(&v, "asks")
        .with_context(|| format!("snapshot parse asks; host={host} path={full_path}; body_snip={body_snip:?}"))?;
    Ok((last, bids, asks, body))
}
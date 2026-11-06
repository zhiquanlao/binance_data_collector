mod orderbook;
mod trade;
mod units;
mod bbo;
mod recovery;
use std::cell::Cell;
use std::rc::Rc;
use units::Units;
use bbo::Quote;

use crate::trade::TradeSummary;
use crate::orderbook::{
    OrderBookL2, TopN, fetch_depth_snapshot_blocking_with_body, on_binance_depth_update, MAX_LEVELS,
};
use anyhow::{Context, anyhow};
use itoa::Buffer as ItoaBuf;
use libc::{poll, pollfd, POLLIN};
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};
use rustls::pki_types::ServerName;
use sonic_rs::{JsonContainerTrait, JsonValueTrait};
use std::fs::{self, File};
use std::io::{BufWriter, Read, Write};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use rand::Rng;

#[derive(Clone, Copy, Debug)]
enum Market {
    PerpUsdt,
    PerpUsdc,
}

#[derive(Default)]
struct State {
    perp_usdt_top10: TopN,
    perp_usdc_top10: TopN,
    perp_usdt_trade: TradeSummary,
    perp_usdc_trade: TradeSummary,
    quotes: [Quote; 2],
    last_written_prices: [Option<(i64, i64)>; 2],
}

fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_millis() as u64
}

fn symbol_to_stream_symbol(symbol: &str) -> String {
    symbol.trim().to_ascii_lowercase()
}

fn trunc_str(s: &str, lim: usize) -> &str {
    if s.len() <= lim {
        s
    } else {
        &s[..lim]
    }
}

// ---- WebSocket framing helpers ----

#[inline]
fn create_masked_control_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    const MASK_KEY: [u8; 4] = [0x12, 0x34, 0x56, 0x78];
    let len = payload.len();
    let mut out = Vec::with_capacity(2 + 8 + 4 + len);
    out.push(0x80 | (opcode & 0x0F));
    match len {
        0..=125 => out.push(0x80 | (len as u8)),
        126..=65535 => {
            out.push(0x80 | 126);
            out.extend_from_slice(&(len as u16).to_be_bytes());
        }
        _ => {
            out.push(0x80 | 127);
            out.extend_from_slice(&(len as u64).to_be_bytes());
        }
    }
    out.extend_from_slice(&MASK_KEY);
    for (i, &b) in payload.iter().enumerate() {
        out.push(b ^ MASK_KEY[i & 3]);
    }
    out
}

#[derive(Debug)]
enum ParseErr {
    NeedMore,
    MaskedFromServer,
    TooBig,
}

fn parse_ws_frame<'a>(buf: &'a [u8]) -> Result<(usize, u8, &'a [u8]), ParseErr> {
    if buf.len() < 2 {
        return Err(ParseErr::NeedMore);
    }
    let b0 = buf[0];
    let b1 = buf[1];
    let opcode = b0 & 0x0F;
    let masked = (b1 & 0x80) != 0;
    let mut len = (b1 & 0x7F) as usize;
    let mut idx = 2;
    if len == 126 {
        if buf.len() < idx + 2 {
            return Err(ParseErr::NeedMore);
        }
        len = u16::from_be_bytes([buf[idx], buf[idx + 1]]) as usize;
        idx += 2;
    } else if len == 127 {
        if buf.len() < idx + 8 {
            return Err(ParseErr::NeedMore);
        }
        let l = u64::from_be_bytes(buf[idx..idx + 8].try_into().unwrap());
        if l > 16 * 1024 * 1024 {
            return Err(ParseErr::TooBig);
        }
        len = l as usize;
        idx += 8;
    }
    if masked {
        return Err(ParseErr::MaskedFromServer);
    }
    if buf.len() < idx + len {
        return Err(ParseErr::NeedMore);
    }
    Ok((idx + len, opcode, &buf[idx..idx + len]))
}

fn connect_binance_ws_blocking(
    host: &'static str,
    path: &str,
) -> anyhow::Result<StreamOwned<ClientConnection, std::net::TcpStream>> {
    let roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server_name = ServerName::try_from(host).map_err(|_| anyhow!("invalid DNS name: {host}"))?;
    let conn = ClientConnection::new(Arc::new(cfg), server_name)?;

    let tcp = std::net::TcpStream::connect((host, 443))?;
    tcp.set_read_timeout(Some(Duration::from_secs(3)))?;
    tcp.set_write_timeout(Some(Duration::from_secs(3)))?;
    tcp.set_nonblocking(false)?;

    let mut tls = StreamOwned::new(conn, tcp);
    let req = format!(
        "GET {path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Sec-WebSocket-Version: 13\r\n\r\n"
    );
    tls.write_all(req.as_bytes())?;
    tls.flush()?;

    let mut hdr = Vec::with_capacity(2048);
    let mut tmp = [0u8; 1024];
    loop {
        let n = tls.read(&mut tmp)?;
        if n == 0 {
            anyhow::bail!("Binance WS handshake EOF");
        }
        hdr.extend_from_slice(&tmp[..n]);
        if hdr.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if hdr.len() > 8192 {
            anyhow::bail!("Binance WS handshake too big");
        }
    }
    if !hdr.starts_with(b"HTTP/1.1 101") {
        anyhow::bail!("Binance WS upgrade failed: {}", String::from_utf8_lossy(&hdr));
    }

    tls.get_ref().set_nonblocking(true)?;
    Ok(tls)
}

fn header() -> String {
    let mut cols: Vec<String> = Vec::new();
    cols.push("local_time".into());
    cols.push("binance_event_time".into());
    cols.push("binance_transaction_time".into());
    cols.push("event_type".into());

    for m in ["perp_usdt", "perp_usdc"] {
        for i in 1..=10 {
            cols.push(format!("{m}_bid_price_{i}"));
        }
        for i in 1..=10 {
            cols.push(format!("{m}_ask_price_{i}"));
        }
        for i in 1..=10 {
            cols.push(format!("{m}_bid_size_{i}"));
        }
        for i in 1..=10 {
            cols.push(format!("{m}_ask_size_{i}"));
        }
    }

    for m in ["perp_usdt", "perp_usdc"] {
        cols.push(format!("{m}_price"));
        cols.push(format!("{m}_quantity"));
        cols.push(format!("{m}_trade_id"));
        cols.push(format!("{m}_is_buyer_maker"));
    }
    for m in ["perp_usdt", "perp_usdc"] {
        for field in ["best_bid_price", "best_ask_price", "best_bid_size", "best_ask_size", "best_update_id"] {
            cols.push(format!("{m}_{field}"));
        }
    }
    cols.join(",")
}

// All metadata belongs to the message that triggered this row, not the cached state.
#[derive(Clone, Copy, Debug)]
struct MessageMeta {
    received_at_ms: u64,
    event_time_ms: u64,
    transaction_time_ms: u64,
    event_type: &'static str,
}

impl Market {
    fn index(self) -> usize { match self { Self::PerpUsdt => 0, Self::PerpUsdc => 1 } }
    fn bbo_event_type(self) -> &'static str { match self { Self::PerpUsdt => "usdt_bbo", Self::PerpUsdc => "usdc_bbo" } }
    fn event_type(self, is_trade: bool) -> &'static str {
        match (self, is_trade) {
            (Self::PerpUsdt, true) => "usdt_trade",
            (Self::PerpUsdc, true) => "usdc_trade",
            (Self::PerpUsdt, false) => "usdt_book",
            (Self::PerpUsdc, false) => "usdc_book",
        }
    }
}

fn write_row(w: &mut BufWriter<File>, st: &mut State, meta: MessageMeta) -> std::io::Result<()> {
    let mut b1 = ItoaBuf::new();
    let mut b2 = ItoaBuf::new();
    let mut b3 = ItoaBuf::new();
    write!(
        w, "{},{},{},{}",
        b1.format(meta.received_at_ms),
        b2.format(meta.event_time_ms),
        b3.format(meta.transaction_time_ms),
        meta.event_type,
    )?;

    fn write_top10(w: &mut BufWriter<File>, t: &TopN) -> std::io::Result<()> {
        for i in 0..10 {
            write!(w, ",{}", t.bid_px[i])?;
        }
        for i in 0..10 {
            write!(w, ",{}", t.ask_px[i])?;
        }
        for i in 0..10 {
            write!(w, ",{}", t.bid_sz[i])?;
        }
        for i in 0..10 {
            write!(w, ",{}", t.ask_sz[i])?;
        }
        Ok(())
    }
    write_top10(w, &st.perp_usdt_top10)?;
    write_top10(w, &st.perp_usdc_top10)?;

    fn write_trade(w: &mut BufWriter<File>, a: &TradeSummary) -> std::io::Result<()> {
        write!(w, ",{}", a.price)?;
        write!(w, ",{}", a.quantity)?;
        write!(w, ",{}", a.trade_id)?;
        write!(w, ",{}", if a.is_buyer_maker { 1 } else { 0 })?;
        Ok(())
    }
    write_trade(w, &st.perp_usdt_trade)?;
    write_trade(w, &st.perp_usdc_trade)?;

    for quote in st.quotes {
        write!(w, ",{},{},{},{},{}", quote.bid, quote.ask, quote.bid_size, quote.ask_size, quote.update_id)?;
    }
    writeln!(w)?;
    st.last_written_prices = st.quotes.map(|q| q.prices());
    Ok(())
}

fn write_zero_row(w: &mut BufWriter<File>, n_cols: usize, st: &mut State) -> std::io::Result<()> {
    st.last_written_prices = [None; 2];
    st.quotes = [Quote::default(); 2];
    // A "marker" row: all zeros, same number of columns as `header()`.
    if n_cols == 0 {
        return Ok(());
    }
    write!(w, "0")?;
    for _ in 1..n_cols {
        write!(w, ",0")?;
    }
    writeln!(w)?;
    Ok(())
}

#[allow(non_snake_case)]
#[derive(Debug, Clone)]
struct DepthUpdate {
    E: u64,
    T: u64,
    U: u64,
    u: u64,
    pu: Option<u64>,
    bids: Vec<(i64, i64)>,
    asks: Vec<(i64, i64)>,
}

#[allow(non_snake_case)]
fn parse_depth_update(v: &sonic_rs::Value, _raw_payload: &[u8], units: Units) -> Result<DepthUpdate, String> {
    let payload = v.get("data").unwrap_or(v);
    let et = payload.get("e").and_then(|x| x.as_str()).unwrap_or("");
    if et != "depthUpdate" {
        return Err("not a depthUpdate event".to_string());
    }
    let E = payload.get("E").and_then(|x| x.as_u64()).ok_or("missing depth E")?;
    let T = payload.get("T").and_then(|x| x.as_u64()).ok_or("missing depth T")?;
    let U = payload.get("U").and_then(|x| x.as_u64()).ok_or("missing depth U")?;
    let u = payload.get("u").and_then(|x| x.as_u64()).ok_or("missing depth u")?;
    let pu = Some(payload.get("pu").and_then(|x| x.as_u64()).ok_or("missing depth pu")?);
    if U > u { return Err("invalid depth update range".into()); }

    fn parse_side(x: Option<&sonic_rs::Value>, units: Units) -> Result<Vec<(i64, i64)>, String> {
        let arr = x.and_then(|a| a.as_array()).ok_or("missing depth side")?;
        arr.iter().map(|row| {
            let px = row.get(0).and_then(|v| v.as_str()).ok_or("missing depth price")?;
            let qty = row.get(1).and_then(|v| v.as_str()).ok_or("missing depth size")?;
            Ok((units.price.units(px).map_err(|e| e.to_string())?, units.size.units(qty).map_err(|e| e.to_string())?))
        }).collect()
    }
    let bids = parse_side(payload.get("b"), units)?;
    let asks = parse_side(payload.get("a"), units)?;
    Ok(DepthUpdate {
        E,
        T,
        U,
        u,
        pu,
        bids,
        asks,
    })
}

// ---- MarketConn: handles depth orderbook for perp markets ----
// Connects to: wss://fstream.binance.com/public/stream?streams={symbol}@depth

struct MarketConn {
    recovery: Rc<Cell<bool>>,
    units: Units,
    market: Market,
    http_host: &'static str,
    http_path: &'static str,
    symbol: String,

    tls: StreamOwned<ClientConnection, std::net::TcpStream>,
    ws_buf: Vec<u8>,
    tmp: [u8; 16 * 1024],

    // reconnect/backoff
    connected_at: Instant,
    last_rx_at: Instant,
    last_ping_at: Instant,
    last_err_log_at: Instant,
    suppressed_err_logs: u64,

    // ---- Binance orderbook sequencing ----
    book_ready: bool,
    snap_last_update_id: u64,
    prev_u: u64,
    book: OrderBookL2<MAX_LEVELS>,
    buffered: Vec<DepthUpdate>,
    sync_started_at: Instant,

    // ---- debug / one-time logs ----
    printed_first_depth_once: bool,

    // ---- warmup after reconnect ----
    warmup_loop: bool,
}

impl MarketConn {
    fn new(market: Market, symbol: &str, units: Units, recovery: Rc<Cell<bool>>) -> anyhow::Result<Self> {
        let symbol = symbol.trim().to_ascii_uppercase();
        let stream_symbol = symbol_to_stream_symbol(&symbol);
        // Both PerpUsdt and PerpUsdc use the fstream endpoint.
        // Depth@0ms now uses the /public/stream path.
        let ws_host = "fstream.binance.com";
        let http_host = "fapi.binance.com";
        let http_path = "/fapi/v1/depth";
        let ws_path = format!("/public/stream?streams={s}@depth@0ms", s = stream_symbol);
        eprintln!(
            "[ws] connecting {:?} {} host={} path={}",
            market, symbol, ws_host, ws_path
        );
        let tls = connect_binance_ws_blocking(ws_host, &ws_path)?;
        eprintln!("[ws] connected {:?} {}", market, symbol);

        let c = Self {
            recovery,
            units,
            market,
            http_host,
            http_path,
            symbol,
            tls,
            ws_buf: Vec::with_capacity(64 * 1024),
            tmp: [0u8; 16 * 1024],
            connected_at: Instant::now(),
            last_rx_at: Instant::now(),
            last_ping_at: Instant::now(),
            last_err_log_at: Instant::now() - Duration::from_secs(3600),
            suppressed_err_logs: 0,
            book_ready: false,
            snap_last_update_id: 0,
            prev_u: 0,
            book: OrderBookL2::<MAX_LEVELS>::new(),
            buffered: Vec::with_capacity(1024),
            sync_started_at: Instant::now(),
            printed_first_depth_once: false,
            warmup_loop: false,
        };

        // IMPORTANT (Binance recommended):
        // Do NOT fetch the snapshot immediately here.
        // Start WS first, buffer depth diffs, then fetch snapshot AFTER seeing first depth event.U.
        Ok(c)
    }

    fn reset_book_sync(&mut self) {
        self.book_ready = false;
        self.prev_u = 0;
        self.snap_last_update_id = 0;
        self.book.clear();
        self.buffered.clear();
        self.sync_started_at = Instant::now();
    }

    fn log_ob_err(&mut self, msg: &str) {
        let now = Instant::now();
        if now.duration_since(self.last_err_log_at) < Duration::from_secs(1) {
            self.suppressed_err_logs = self.suppressed_err_logs.saturating_add(1);
            return;
        }
        if self.suppressed_err_logs > 0 {
            eprintln!(
                "[ob] {:?} {} (suppressed {} msgs) {msg}",
                self.market, self.symbol, self.suppressed_err_logs
            );
            self.suppressed_err_logs = 0;
        } else {
            eprintln!("[ob] {:?} {} {msg}", self.market, self.symbol);
        }
        self.last_err_log_at = now;
    }

    fn fetch_snapshot_after_first_u(&mut self, first_u: u64) -> anyhow::Result<()> {
        eprintln!("[snapshot] {:?} {} fetching after symbol refresh", self.market, self.symbol);
        let (last, bids, asks, _body) = fetch_depth_snapshot_blocking_with_body(
            self.http_host, self.http_path, &self.symbol, 1000, self.units,
        )?;
        if last < first_u {
            anyhow::bail!("snapshot lastUpdateId={last} precedes first event U={first_u}");
        }
        self.book.clear();
        on_binance_depth_update(&self.symbol, &mut self.book, last, last, None, &bids, &asks, 0);
        self.snap_last_update_id = last;
        self.book_ready = false;
        self.prev_u = 0;
        self.sync_started_at = Instant::now();
        Ok(())
    }

    fn bridge_timed_out(&self) -> bool {
        !self.book_ready && self.sync_started_at.elapsed() >= recovery::BRIDGE_TIMEOUT
    }

    fn request_reconnect(&self) {
        eprintln!("[recovery] {:?} {} requested reconnect", self.market, self.symbol);
        self.recovery.set(true);
    }

    fn try_bootstrap_from_buffer(&mut self) -> bool {
        if self.book_ready || self.snap_last_update_id == 0 {
            return false;
        }
        if self.buffered.is_empty() {
            return false;
        }

        // Perp rule: drop diffs where u < snap_last_update_id
        let mut drop_n = 0usize;
        for ev in &self.buffered {
            if ev.u < self.snap_last_update_id {
                drop_n += 1;
            } else {
                break;
            }
        }
        if drop_n > 0 {
            self.buffered.drain(0..drop_n);
        }
        if self.buffered.is_empty() {
            return false;
        }

        let bi = match recovery::bridge_status(
            self.snap_last_update_id,
            self.buffered.iter().map(|ev| (ev.U, ev.u)),
            self.sync_started_at.elapsed(),
        ) {
            recovery::Bridge::Ready(index) => index,
            recovery::Bridge::Waiting => return false,
            recovery::Bridge::Reconnect => {
                self.log_ob_err("snapshot cannot be bridged (gap or timeout) -> reconnect");
                self.recovery.set(true);
                return false;
            }
        };
        if bi > 0 {
            self.buffered.drain(0..bi);
        }

        // Apply remaining buffered events from bridge onward, enforcing contiguity.
        let drained: Vec<DepthUpdate> = self.buffered.drain(..).collect();
        for ev in drained {
            if self.prev_u != 0 {
                if let Some(pu) = ev.pu {
                    if pu != self.prev_u {
                        self.log_ob_err("bootstrap contiguity gap (pu) -> resync");
                        self.reset_book_sync();
                        self.recovery.set(true);
                        return false;
                    }
                } else if ev.U != self.prev_u + 1 {
                    self.log_ob_err("bootstrap contiguity gap (no pu) -> resync");
                    self.reset_book_sync();
                    self.recovery.set(true);
                    return false;
                }
            }
            on_binance_depth_update(&self.symbol, &mut self.book, ev.U, ev.u, ev.pu, &ev.bids, &ev.asks, ev.E);
            self.prev_u = ev.u;
        }
        self.book_ready = true;
        true
    }

    #[inline]
    fn handle_depth_update(&mut self, ev: DepthUpdate) -> bool {
        // Returns true if the book was updated.
        // Perp always uses snapshot-based approach (no direct apply without snapshot).
        if !self.book_ready {
            if self.snap_last_update_id == 0 {
                if self.buffered.len() >= recovery::MAX_BUFFERED_DEPTH {
                    self.log_ob_err("pre-snapshot buffer overflow -> resync");
                    self.recovery.set(true);
                    return false;
                }
                let first_u = ev.U;
                self.buffered.push(ev);
                if let Err(e) = self.fetch_snapshot_after_first_u(first_u) {
                    eprintln!("[snapshot] {:?} {} failed: {e:#} -> refresh and reconnect", self.market, self.symbol);
                    self.recovery.set(true);
                    return false;
                }
                return self.try_bootstrap_from_buffer();
            }

            if self.buffered.len() >= recovery::MAX_BUFFERED_DEPTH {
                self.log_ob_err("pre-bridge buffer overflow -> reconnect");
                self.recovery.set(true);
                self.reset_book_sync();
                return false;
            }
            self.buffered.push(ev);
            return self.try_bootstrap_from_buffer();
        }

        // normal mode: enforce contiguity
        if let Some(pu) = ev.pu {
            if pu != self.prev_u {
                self.log_ob_err(&format!(
                    "contiguity gap: pu={} != prev_u={} (U={}, u={}, lastUpdateId={}) => resync",
                    pu, self.prev_u, ev.U, ev.u, self.snap_last_update_id
                ));
                return false;
            }
        } else if self.prev_u != 0 && ev.U != self.prev_u + 1 {
            self.log_ob_err(&format!(
                "contiguity gap (no pu): U={} != prev_u+1={} (u={}, lastUpdateId={}) => resync",
                ev.U,
                self.prev_u + 1,
                ev.u,
                self.snap_last_update_id
            ));
            return false;
        }

        on_binance_depth_update(&self.symbol, &mut self.book, ev.U, ev.u, ev.pu, &ev.bids, &ev.asks, ev.E);
        self.prev_u = ev.u;
        true
    }

    #[inline]
    fn maybe_send_ping(&mut self, now: Instant) {
        const PING_EVERY: Duration = Duration::from_secs(30);
        if now.duration_since(self.last_ping_at) < PING_EVERY {
            return;
        }
        let ping = create_masked_control_frame(0x09, &[]);
        let _ = self.tls.write_all(&ping);
        let _ = self.tls.flush();
        self.last_ping_at = now;
    }
}

// ---- Depth10Conn: handles depth10@100ms orderbook for perp markets ----
// Connects to: wss://fstream.binance.com/public/stream?streams={symbol}@depth10@100ms

struct Depth10Conn {
    recovery: Rc<Cell<bool>>,
    market: Market,
    symbol: String,

    tls: StreamOwned<ClientConnection, std::net::TcpStream>,
    ws_buf: Vec<u8>,
    tmp: [u8; 16 * 1024],

    connected_at: Instant,
    last_rx_at: Instant,
    last_ping_at: Instant,

    book: OrderBookL2<10>,
    last_u: u64,
    printed_first_depth10_once: bool,
    paused_u: Option<u64>,
    paused_at: Instant,

    // ---- warmup after reconnect ----
    warmup_loop: bool,
}

impl Depth10Conn {
    fn new(market: Market, symbol: &str, recovery: Rc<Cell<bool>>) -> anyhow::Result<Self> {
        let symbol = symbol.trim().to_ascii_uppercase();
        let stream_symbol = symbol_to_stream_symbol(&symbol);
        // depth10@100ms now uses the /public/stream path on fstream.
        let ws_host = "fstream.binance.com";
        let ws_path = format!("/public/stream?streams={s}@depth10@100ms", s = stream_symbol);
        eprintln!(
            "[ws] connecting depth10 {:?} {} host={} path={}",
            market, symbol, ws_host, ws_path
        );
        let tls = connect_binance_ws_blocking(ws_host, &ws_path)?;
        eprintln!("[ws] connected depth10 {:?} {}", market, symbol);

        Ok(Self {
            recovery,
            market,
            symbol,
            tls,
            ws_buf: Vec::with_capacity(64 * 1024),
            tmp: [0u8; 16 * 1024],
            connected_at: Instant::now(),
            last_rx_at: Instant::now(),
            last_ping_at: Instant::now(),
            book: OrderBookL2::<10>::new(),
            last_u: 0,
            printed_first_depth10_once: false,
            paused_u: None,
            paused_at: Instant::now(),
            warmup_loop: false,
        })
    }

    fn request_reconnect(&self) {
        eprintln!("[recovery] {:?} {} requested reconnect", self.market, self.symbol);
        self.recovery.set(true);
    }

    #[inline]
    fn maybe_send_ping(&mut self, now: Instant) {
        const PING_EVERY: Duration = Duration::from_secs(30);
        if now.duration_since(self.last_ping_at) < PING_EVERY {
            return;
        }
        let ping = create_masked_control_frame(0x09, &[]);
        let _ = self.tls.write_all(&ping);
        let _ = self.tls.flush();
        self.last_ping_at = now;
    }
}

// ---- TradeConn: handles trade for perp markets ----
// Connects to: wss://fstream.binance.com/public/stream?streams={symbol}@trade

struct TradeConn {
    recovery: Rc<Cell<bool>>,
    market: Market,
    symbol: String,

    tls: StreamOwned<ClientConnection, std::net::TcpStream>,
    ws_buf: Vec<u8>,
    tmp: [u8; 16 * 1024],

    connected_at: Instant,
    last_rx_at: Instant,
    last_ping_at: Instant,

    printed_first_trade_once: bool,

    // ---- warmup after reconnect ----
    warmup_loop: bool,
}

impl TradeConn {
    fn new(market: Market, symbol: &str, recovery: Rc<Cell<bool>>) -> anyhow::Result<Self> {
        let symbol = symbol.trim().to_ascii_uppercase();
        let stream_symbol = symbol_to_stream_symbol(&symbol);
        let ws_host = "fstream.binance.com";
        let ws_path = format!("/public/stream?streams={s}@trade/{s}@bookTicker", s = stream_symbol);
        eprintln!(
            "[ws] connecting trade {:?} {} host={} path={}",
            market, symbol, ws_host, ws_path
        );
        let tls = connect_binance_ws_blocking(ws_host, &ws_path)?;
        eprintln!("[ws] connected trade {:?} {}", market, symbol);

        Ok(Self {
            recovery,
            market,
            symbol,
            tls,
            ws_buf: Vec::with_capacity(64 * 1024),
            tmp: [0u8; 16 * 1024],
            connected_at: Instant::now(),
            last_rx_at: Instant::now(),
            last_ping_at: Instant::now(),
            printed_first_trade_once: false,
            warmup_loop: false,
        })
    }

    fn request_reconnect(&self) {
        eprintln!("[recovery] {:?} {} requested reconnect", self.market, self.symbol);
        self.recovery.set(true);
    }

    #[inline]
    fn maybe_send_ping(&mut self, now: Instant) {
        const PING_EVERY: Duration = Duration::from_secs(30);
        if now.duration_since(self.last_ping_at) < PING_EVERY {
            return;
        }
        let ping = create_masked_control_frame(0x09, &[]);
        let _ = self.tls.write_all(&ping);
        let _ = self.tls.flush();
        self.last_ping_at = now;
    }
}

fn top10_equal(a: &TopN, b: &TopN) -> bool {
    a.bid_px == b.bid_px && a.ask_px == b.ask_px && a.bid_sz == b.bid_sz && a.ask_sz == b.ask_sz
}

fn maybe_compare_depth0_vs_depth10(depth0: &MarketConn, depth10: &Depth10Conn) -> bool {
    // Returns true if mismatch detected (caller should reconnect depth).
    if depth10.last_u == 0 {
        return false;
    }
    if !depth0.book_ready {
        return false;
    }
    if depth10.last_u < depth0.prev_u {
        return false;
    }
    if depth10.last_u > depth0.prev_u {
        return false;
    }

    // u matches
    if depth0.book.bid_depth() < 10 || depth0.book.ask_depth() < 10 {
        return false;
    }
    if depth10.book.bid_depth() < 10 || depth10.book.ask_depth() < 10 {
        return false;
    }

    let a = depth0.book.top_n_arrays(10);
    let b = depth10.book.top_n_arrays(10);
    if !top10_equal(&a, &b) {
        eprintln!(
            "[compare] mismatch {:?} {} u={} depth0_depth=(bids={}, asks={}) depth10_depth=(bids={}, asks={})",
            depth0.market,
            depth0.symbol,
            depth0.prev_u,
            depth0.book.bid_depth(),
            depth0.book.ask_depth(),
            depth10.book.bid_depth(),
            depth10.book.ask_depth()
        );
        eprintln!("[compare] depth0 top10 bid_px={:?}", a.bid_px);
        eprintln!("[compare] depth0 top10 ask_px={:?}", a.ask_px);
        eprintln!("[compare] depth0 top10 bid_sz={:?}", a.bid_sz);
        eprintln!("[compare] depth0 top10 ask_sz={:?}", a.ask_sz);
        eprintln!("[compare] depth10 top10 bid_px={:?}", b.bid_px);
        eprintln!("[compare] depth10 top10 ask_px={:?}", b.ask_px);
        eprintln!("[compare] depth10 top10 bid_sz={:?}", b.bid_sz);
        eprintln!("[compare] depth10 top10 ask_sz={:?}", b.ask_sz);
        return true;
    }
    false
}

fn try_resolve_depth10_pause(depth0: &MarketConn, depth10: &mut Depth10Conn) -> Option<bool> {
    let Some(target_u) = depth10.paused_u else {
        return None;
    };

    const MAX_PAUSE: Duration = Duration::from_secs(2);
    if depth10.paused_at.elapsed() > MAX_PAUSE {
        depth10.paused_u = None;
        return Some(true);
    }

    if !depth0.book_ready {
        return Some(false);
    }
    if depth0.prev_u < target_u {
        return Some(false);
    }

    if depth0.prev_u == target_u {
        let mismatch = maybe_compare_depth0_vs_depth10(depth0, depth10);
        if mismatch {
            // Leave reconnect decision to caller.
        }
    }
    depth10.paused_u = None;
    Some(true)
}

struct Session {
    units: [Units; 2],
    metadata: serde_json::Map<String, serde_json::Value>,
    perp_usdt: MarketConn,
    perp_usdc: MarketConn,
    perp_usdt_d10: Depth10Conn,
    perp_usdc_d10: Depth10Conn,
    perp_usdt_trade: TradeConn,
    perp_usdc_trade: TradeConn,
}

fn connect_session(symbols: &[String; 2], recovery: Rc<Cell<bool>>) -> anyhow::Result<Session> {
    let perp_usdt_symbol = &symbols[0];
    let perp_usdc_symbol = &symbols[1];
    let exchange_info = orderbook::tls_http_get("fapi.binance.com", "/fapi/v1/exchangeInfo", 10000)?;
    let exchange: sonic_rs::Value = sonic_rs::from_slice(&exchange_info)?;
    let exchange_symbols = exchange.get("symbols").and_then(|v| v.as_array()).context("missing exchange symbols")?;
    let mut metadata = serde_json::Map::new();
    let mut symbol_units = Vec::new();
    for symbol in [perp_usdt_symbol, perp_usdc_symbol] {
        let entry = exchange_symbols.iter().find(|v| v.get("symbol").and_then(|v| v.as_str()) == Some(symbol.as_str()))
            .with_context(|| format!("missing symbol {symbol}"))?;
        symbol_units.push(Units::from_symbol(entry)?);
        metadata.insert(symbol.to_string(), serde_json::from_str::<serde_json::Value>(&sonic_rs::to_string(entry)?)?["filters"].clone());
    }
    let units = [symbol_units[0], symbol_units[1]];
    eprintln!("[recovery] symbol information refreshed for {} and {}", symbols[0], symbols[1]);
    let mut session = Session {
        units, metadata,
        perp_usdt: MarketConn::new(Market::PerpUsdt, perp_usdt_symbol, units[0], recovery.clone())?,
        perp_usdc: MarketConn::new(Market::PerpUsdc, perp_usdc_symbol, units[1], recovery.clone())?,
        perp_usdt_d10: Depth10Conn::new(Market::PerpUsdt, perp_usdt_symbol, recovery.clone())?,
        perp_usdc_d10: Depth10Conn::new(Market::PerpUsdc, perp_usdc_symbol, recovery.clone())?,
        perp_usdt_trade: TradeConn::new(Market::PerpUsdt, perp_usdt_symbol, recovery.clone())?,
        perp_usdc_trade: TradeConn::new(Market::PerpUsdc, perp_usdc_symbol, recovery)?,
    };
    session.perp_usdt.sync_started_at = Instant::now();
    session.perp_usdc.sync_started_at = Instant::now();
    Ok(session)
}

fn connect_session_with_retry(symbols: &[String; 2], recovery: Rc<Cell<bool>>) -> Session {
    let mut backoff_ms = 250;
    loop {
        match connect_session(symbols, recovery.clone()) {
            Ok(session) => return session,
            Err(e) => {
                eprintln!("[recovery] refresh/connect failed: {e:#}; retrying in ~{backoff_ms}ms");
                let jitter = rand::thread_rng().gen_range(0..(backoff_ms / 2).max(1));
                std::thread::sleep(Duration::from_millis(backoff_ms + jitter));
                backoff_ms = (backoff_ms * 3 / 2).min(10_000);
            }
        }
    }
}

fn open_output(base: &str, metadata: &serde_json::Map<String, serde_json::Value>) -> anyhow::Result<(BufWriter<File>, String)> {
    fs::create_dir_all("logs")?;
    let filename = format!("logs/{}_binance_{}.csv", now_epoch_ms(), base);
    fs::write(format!("{filename}.units.json"), serde_json::to_vec_pretty(&serde_json::json!({
        "format": "integer_ticks_lots_v1", "timestamp_unit": "unix_milliseconds",
        "price_filter": "PRICE_FILTER.tickSize", "quantity_filter": "LOT_SIZE.stepSize", "symbols": metadata,
    }))?)?;
    let mut w = BufWriter::new(File::create(&filename)?);
    writeln!(w, "{}", header())?;
    w.flush()?;
    Ok((w, filename))
}

fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let args: Vec<String> = std::env::args().collect();
    // Input base symbol like "BTC". We will collect:
    // - <BASE>USDT perp (depth via /public/stream, trade via /public/stream)
    // - <BASE>USDC perp (depth via /public/stream, trade via /public/stream)
    // - depth10@100ms for both perps via /public/stream
    let base = args.get(1).cloned().unwrap_or_else(|| "BTC".to_string());
    let base = base.trim().to_ascii_uppercase();
    let perp_usdt_symbol = format!("{base}USDT");
    let perp_usdc_symbol = format!("{base}USDC");

    let symbols = [perp_usdt_symbol.clone(), perp_usdc_symbol.clone()];
    let recovery = Rc::new(Cell::new(false));
    let Session {
        mut units, metadata, mut perp_usdt, mut perp_usdc,
        mut perp_usdt_d10, mut perp_usdc_d10, mut perp_usdt_trade, mut perp_usdc_trade,
    } = connect_session_with_retry(&symbols, recovery.clone());
    let (mut w, filename) = open_output(&base, &metadata)?;
    let n_cols = header().split(',').count();
    let mut st = State::default();
    let mut rows: u64 = 0;

    let mut fds = [
        pollfd {
            fd: perp_usdt.tls.get_ref().as_raw_fd(),
            events: POLLIN,
            revents: 0,
        },
        pollfd {
            fd: perp_usdc.tls.get_ref().as_raw_fd(),
            events: POLLIN,
            revents: 0,
        },
        pollfd {
            fd: perp_usdt_d10.tls.get_ref().as_raw_fd(),
            events: POLLIN,
            revents: 0,
        },
        pollfd {
            fd: perp_usdc_d10.tls.get_ref().as_raw_fd(),
            events: POLLIN,
            revents: 0,
        },
        pollfd {
            fd: perp_usdt_trade.tls.get_ref().as_raw_fd(),
            events: POLLIN,
            revents: 0,
        },
        pollfd {
            fd: perp_usdc_trade.tls.get_ref().as_raw_fd(),
            events: POLLIN,
            revents: 0,
        },
    ];

    let mut last_flush = Instant::now();

    eprintln!(
        "[start] binance_data_collector base={} perp_usdt={} perp_usdc={} output={}",
        base, perp_usdt_symbol, perp_usdc_symbol, filename
    );

    loop {
        if perp_usdt.bridge_timed_out() || perp_usdc.bridge_timed_out() {
            eprintln!("[recovery] no valid snapshot bridge within 10 seconds");
            recovery.set(true);
        }
        if recovery.get() {
            write_zero_row(&mut w, n_cols, &mut st)?;
            w.flush()?;
            // Rebuild all streams as one generation; their cached integer units must agree.
            let session = connect_session_with_retry(&symbols, recovery.clone());
            units = session.units;
            perp_usdt = session.perp_usdt;
            perp_usdc = session.perp_usdc;
            perp_usdt_d10 = session.perp_usdt_d10;
            perp_usdc_d10 = session.perp_usdc_d10;
            perp_usdt_trade = session.perp_usdt_trade;
            perp_usdc_trade = session.perp_usdc_trade;
            let (next_writer, next_filename) = open_output(&base, &session.metadata)?;
            w = next_writer;
            st = State::default();
            rows = 0;
            last_flush = Instant::now();
            recovery.set(false);
            eprintln!("[recovery] streams reopened with refreshed units; output={next_filename}");
        }
        let now = Instant::now();

        // Check if any connection is in warmup mode (first loop after reconnect).
        // Flags are cleared HERE (not at the bottom) so that a reconnect() called
        // mid-iteration sets warmup_loop=true AFTER this clear, meaning the flag
        // survives to the next iteration's drain phase and actually suppresses output.
        let any_warmup = perp_usdt.warmup_loop || perp_usdc.warmup_loop
            || perp_usdt_d10.warmup_loop || perp_usdc_d10.warmup_loop
            || perp_usdt_trade.warmup_loop || perp_usdc_trade.warmup_loop;
        perp_usdt.warmup_loop = false;
        perp_usdc.warmup_loop = false;
        perp_usdt_d10.warmup_loop = false;
        perp_usdc_d10.warmup_loop = false;
        perp_usdt_trade.warmup_loop = false;
        perp_usdc_trade.warmup_loop = false;

        // keepalive pings
        perp_usdt.maybe_send_ping(now);
        perp_usdc.maybe_send_ping(now);
        perp_usdt_d10.maybe_send_ping(now);
        perp_usdc_d10.maybe_send_ping(now);
        perp_usdt_trade.maybe_send_ping(now);
        perp_usdc_trade.maybe_send_ping(now);

        // periodic connection rotation (Binance connection limits friendly)
        let rotate_after = Duration::from_secs(5 * 3600 + 50 * 60);
        let perp_usdt_rotate = now.duration_since(perp_usdt.connected_at) > rotate_after;
        let perp_usdc_rotate = now.duration_since(perp_usdc.connected_at) > rotate_after;
        let perp_usdt_trade_rotate = now.duration_since(perp_usdt_trade.connected_at) > rotate_after;
        let perp_usdc_trade_rotate = now.duration_since(perp_usdc_trade.connected_at) > rotate_after;

        // deadman timer: reconnect if no bytes received for a while
        let idle_timeout = Duration::from_secs(90);
        let perp_usdt_idle = now.duration_since(perp_usdt.last_rx_at) > idle_timeout;
        let perp_usdc_idle = now.duration_since(perp_usdc.last_rx_at) > idle_timeout;
        let perp_usdt_d10_idle = now.duration_since(perp_usdt_d10.last_rx_at) > idle_timeout;
        let perp_usdc_d10_idle = now.duration_since(perp_usdc_d10.last_rx_at) > idle_timeout;
        let perp_usdt_trade_idle = now.duration_since(perp_usdt_trade.last_rx_at) > idle_timeout;
        let perp_usdc_trade_idle = now.duration_since(perp_usdc_trade.last_rx_at) > idle_timeout;

        // refresh fds in case of reconnect
        fds[0].fd = perp_usdt.tls.get_ref().as_raw_fd();
        fds[1].fd = perp_usdc.tls.get_ref().as_raw_fd();
        fds[2].fd = perp_usdt_d10.tls.get_ref().as_raw_fd();
        fds[3].fd = perp_usdc_d10.tls.get_ref().as_raw_fd();
        fds[4].fd = perp_usdt_trade.tls.get_ref().as_raw_fd();
        fds[5].fd = perp_usdc_trade.tls.get_ref().as_raw_fd();

        let _ = unsafe { poll(fds.as_mut_ptr(), fds.len() as u64, 10) };

        // ---- Drain depth10 sockets and keep last_u/book updated ----
        for d10 in [&mut perp_usdt_d10, &mut perp_usdc_d10] {
            // If this depth10 stream is ahead, pause draining until depth0 catches up.
            let pause_state = match d10.market {
                Market::PerpUsdt => try_resolve_depth10_pause(&perp_usdt, d10),
                Market::PerpUsdc => try_resolve_depth10_pause(&perp_usdc, d10),
            };
            if let Some(false) = pause_state {
                continue;
            }

            'read: loop {
                if recovery.get() { break 'read; }
                match d10.tls.read(&mut d10.tmp) {
                    Ok(0) => {
                        eprintln!("[ws] EOF depth10 {:?} {} -> reconnect", d10.market, d10.symbol);
                        d10.request_reconnect();
                        break 'read;
                    }
                    Ok(n) => {
                        d10.last_rx_at = Instant::now();
                        d10.ws_buf.extend_from_slice(&d10.tmp[..n]);
                        loop {
                            if recovery.get() { break; }
                            let Ok((consumed, opcode, payload)) = parse_ws_frame(&d10.ws_buf) else {
                                break;
                            };

                            let mut pause_now = false;
                            let mut pause_target = 0u64;

                            match opcode {
                                0x1 => {
                                    let payload_txt = std::str::from_utf8(payload).unwrap_or("<non-utf8>");
                                    let v: sonic_rs::Value = match sonic_rs::from_slice(payload) {
                                        Ok(v) => v,
                                        Err(_) => {
                                            eprintln!(
                                                "[ws] depth10 {:?} {} invalid json: {}",
                                                d10.market,
                                                d10.symbol,
                                                trunc_str(payload_txt, 4096)
                                            );
                                            recovery.set(true);
                                            break;
                                        }
                                    };
                                    let payload_v = v.get("data").unwrap_or(&v);
                                    let stream = v.get("stream").and_then(|x| x.as_str()).unwrap_or("");
                                    let evt = payload_v.get("e").and_then(|x| x.as_str()).unwrap_or("");
                                    if evt == "depthUpdate" {
                                        match parse_depth_update(&v, payload, units[d10.market.index()]) {
                                            Ok(upd) => {
                                                let bids_best_to_worst: Vec<(i64, i64)> = upd.bids.clone();
                                                let asks_best_to_worst: Vec<(i64, i64)> = upd.asks.clone();
                                                d10.book.from_snapshot(upd.E, &bids_best_to_worst, &asks_best_to_worst);
                                                d10.last_u = upd.u;
                                                if !d10.printed_first_depth10_once {
                                                    eprintln!(
                                                        "[first-depth10] {:?} {} stream={} E={} U={} u={} pu={:?} bids={} asks={}",
                                                        d10.market,
                                                        d10.symbol,
                                                        stream,
                                                        upd.E,
                                                        upd.U,
                                                        upd.u,
                                                        upd.pu,
                                                        upd.bids.len(),
                                                        upd.asks.len()
                                                    );
                                                    d10.printed_first_depth10_once = true;
                                                }

                                                match d10.market {
                                                    Market::PerpUsdt => {
                                                        if upd.u > perp_usdt.prev_u {
                                                            pause_now = true;
                                                            pause_target = upd.u;
                                                        } else if upd.u == perp_usdt.prev_u
                                                            && maybe_compare_depth0_vs_depth10(&perp_usdt, d10)
                                                        {
                                                            eprintln!("[compare] perp_usdt depth mismatch -> reconnect");
                                                            perp_usdt.request_reconnect();
                                                        }
                                                    }
                                                    Market::PerpUsdc => {
                                                        if upd.u > perp_usdc.prev_u {
                                                            pause_now = true;
                                                            pause_target = upd.u;
                                                        } else if upd.u == perp_usdc.prev_u
                                                            && maybe_compare_depth0_vs_depth10(&perp_usdc, d10)
                                                        {
                                                            eprintln!("[compare] perp_usdc depth mismatch -> reconnect");
                                                            perp_usdc.request_reconnect();
                                                        }
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                eprintln!(
                                                    "[parse-error] depth10 {:?} {} depthUpdate parse failed: {} raw_payload={}",
                                                    d10.market,
                                                    d10.symbol,
                                                    e,
                                                    trunc_str(payload_txt, 4096)
                                                );
                                                recovery.set(true);
                                            }
                                        }
                                    }
                                }
                                0x9 => {
                                    let pong = create_masked_control_frame(0x0A, payload);
                                    let _ = d10.tls.write_all(&pong);
                                    let _ = d10.tls.flush();
                                }
                                0x8 => {
                                    eprintln!(
                                        "[ws] close depth10 {:?} {} ({} bytes)",
                                        d10.market,
                                        d10.symbol,
                                        payload.len()
                                    );
                                    d10.request_reconnect();
                                }
                                _ => {}
                            }

                            d10.ws_buf.drain(..consumed);
                            if pause_now {
                                d10.paused_u = Some(pause_target);
                                d10.paused_at = Instant::now();
                                break 'read;
                            }
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break 'read,
                    Err(_) => {
                        eprintln!(
                            "[ws] read error depth10 {:?} {} -> reconnect",
                            d10.market, d10.symbol
                        );
                        d10.request_reconnect();
                        break 'read;
                    }
                }
            }
        }

        // ---- Drain perp depth sockets ----
        for c in [&mut perp_usdt, &mut perp_usdc] {
            loop {
                if recovery.get() { break; }
                match c.tls.read(&mut c.tmp) {
                    Ok(0) => {
                        eprintln!("[ws] EOF {:?} {} -> reconnect", c.market, c.symbol);
                        c.request_reconnect();
                        break;
                    }
                    Ok(n) => {
                        let received_at_ms = now_epoch_ms();
                        c.last_rx_at = Instant::now();
                        c.ws_buf.extend_from_slice(&c.tmp[..n]);
                        loop {
                            if recovery.get() { break; }
                            let Ok((consumed, opcode, payload)) = parse_ws_frame(&c.ws_buf) else {
                                break;
                            };
                            let mut do_reconnect_after_drain = false;
                            let mut need_resync_after_drain = false;

                            match opcode {
                                0x1 => {
                                    let payload_txt = std::str::from_utf8(payload).unwrap_or("<non-utf8>");
                                    let v: sonic_rs::Value = match sonic_rs::from_slice(payload) {
                                        Ok(v) => v,
                                        Err(_) => {
                                            eprintln!(
                                                "[ws] {:?} {} invalid json: {}",
                                                c.market,
                                                c.symbol,
                                                trunc_str(payload_txt, 4096)
                                            );
                                            recovery.set(true);
                                            break;
                                        }
                                    };
                                    let payload_v = v.get("data").unwrap_or(&v);
                                    let stream = v.get("stream").and_then(|x| x.as_str()).unwrap_or("");
                                    let evt = payload_v.get("e").and_then(|x| x.as_str()).unwrap_or("");
                                    if evt == "depthUpdate" {
                                        match parse_depth_update(&v, payload, units[c.market.index()]) {
                                            Ok(upd) => {
                                                let applied = c.handle_depth_update(upd.clone());
                                                if !c.printed_first_depth_once {
                                                    eprintln!(
                                                        "[first-depth] {:?} {} stream={} E={} U={} u={} pu={:?} bids={} asks={} applied={} ready={}",
                                                        c.market,
                                                        c.symbol,
                                                        stream,
                                                        upd.E,
                                                        upd.U,
                                                        upd.u,
                                                        upd.pu,
                                                        upd.bids.len(),
                                                        upd.asks.len(),
                                                        applied,
                                                        c.book_ready
                                                    );
                                                    c.printed_first_depth_once = true;
                                                }
                                                if applied {
                                                    let top10 = c.book.top_n_arrays(10);
                                                    if c.book.bid_depth() < 10 || c.book.ask_depth() < 10 {
                                                        eprintln!(
                                                            "[depth-check] {:?} {} bid_depth={} ask_depth={} < 10 => reconnect depth",
                                                            c.market,
                                                            c.symbol,
                                                            c.book.bid_depth(),
                                                            c.book.ask_depth()
                                                        );
                                                        need_resync_after_drain = true;
                                                    }
                                                    match c.market {
                                                        Market::PerpUsdt => {
                                                            st.perp_usdt_top10 = top10;
                                                            if let Some(target_u) = perp_usdt_d10.paused_u {
                                                                if c.prev_u >= target_u {
                                                                    if c.prev_u == target_u
                                                                        && maybe_compare_depth0_vs_depth10(c, &perp_usdt_d10)
                                                                    {
                                                                        eprintln!(
                                                                            "[compare] {:?} {} mismatch => reconnect depth",
                                                                            c.market, c.symbol
                                                                        );
                                                                        need_resync_after_drain = true;
                                                                    }
                                                                    perp_usdt_d10.paused_u = None;
                                                                }
                                                            }
                                                        }
                                                        Market::PerpUsdc => {
                                                            st.perp_usdc_top10 = top10;
                                                            if let Some(target_u) = perp_usdc_d10.paused_u {
                                                                if c.prev_u >= target_u {
                                                                    if c.prev_u == target_u
                                                                        && maybe_compare_depth0_vs_depth10(c, &perp_usdc_d10)
                                                                    {
                                                                        eprintln!(
                                                                            "[compare] {:?} {} mismatch => reconnect depth",
                                                                            c.market, c.symbol
                                                                        );
                                                                        need_resync_after_drain = true;
                                                                    }
                                                                    perp_usdc_d10.paused_u = None;
                                                                }
                                                            }
                                                        }
                                                    }
                                                    st.quotes[c.market.index()].update_if_newer(Quote::from_top(&top10, upd.u));
                                                    if !any_warmup && !recovery.get() {
                                                        write_row(&mut w, &mut st, MessageMeta {
                                                            received_at_ms,
                                                            event_time_ms: upd.E,
                                                            transaction_time_ms: upd.T,
                                                            event_type: c.market.event_type(false),
                                                        })?;
                                                        rows += 1;
                                                    }
                                                } else if c.book_ready {
                                                    eprintln!(
                                                        "[depth] {:?} {} contiguity/resync triggered (see [ob] logs) -> reconnect",
                                                        c.market, c.symbol
                                                    );
                                                    need_resync_after_drain = true;
                                                }
                                            }
                                            Err(e) => {
                                                eprintln!(
                                                    "[parse-error] {:?} {} depthUpdate parse failed: {} raw_payload={}",
                                                    c.market,
                                                    c.symbol,
                                                    e,
                                                    trunc_str(payload_txt, 4096)
                                                );
                                                do_reconnect_after_drain = true;
                                            }
                                        }
                                    } else {
                                        // subscribe ack / other events ignored
                                    }
                                }
                                0x9 => {
                                    let pong = create_masked_control_frame(0x0A, payload);
                                    let _ = c.tls.write_all(&pong);
                                    let _ = c.tls.flush();
                                }
                                0x8 => {
                                    eprintln!("[ws] close {:?} {} ({} bytes)", c.market, c.symbol, payload.len());
                                    do_reconnect_after_drain = true;
                                }
                                _ => {}
                            }

                            c.ws_buf.drain(..consumed);
                            if do_reconnect_after_drain {
                                c.request_reconnect();
                                break;
                            }
                            if need_resync_after_drain {
                                c.request_reconnect();
                                break;
                            }
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => {
                        eprintln!("[ws] read error {:?} {} -> reconnect", c.market, c.symbol);
                        c.request_reconnect();
                        break;
                    }
                }
            }
        }

        // ---- Drain trade sockets (/public/stream endpoint) ----
        for trade in [&mut perp_usdt_trade, &mut perp_usdc_trade] {
            loop {
                if recovery.get() { break; }
                match trade.tls.read(&mut trade.tmp) {
                    Ok(0) => {
                        eprintln!("[ws] EOF trade {:?} {} -> reconnect", trade.market, trade.symbol);
                        trade.request_reconnect();
                        break;
                    }
                    Ok(n) => {
                        let received_at_ms = now_epoch_ms();
                        trade.last_rx_at = Instant::now();
                        trade.ws_buf.extend_from_slice(&trade.tmp[..n]);
                        loop {
                            if recovery.get() { break; }
                            let Ok((consumed, opcode, payload)) = parse_ws_frame(&trade.ws_buf) else {
                                break;
                            };
                            let mut do_reconnect_after_drain = false;

                            match opcode {
                                0x1 => {
                                    let payload_txt = std::str::from_utf8(payload).unwrap_or("<non-utf8>");
                                    let v: sonic_rs::Value = match sonic_rs::from_slice(payload) {
                                        Ok(v) => v,
                                        Err(_) => {
                                            eprintln!(
                                                "[ws] trade {:?} {} invalid json: {}",
                                                trade.market,
                                                trade.symbol,
                                                trunc_str(payload_txt, 4096)
                                            );
                                            recovery.set(true);
                                            break;
                                        }
                                    };
                                    let payload_v = v.get("data").unwrap_or(&v);
                                    let stream = v.get("stream").and_then(|x| x.as_str()).unwrap_or("");
                                    let evt = payload_v.get("e").and_then(|x| x.as_str()).unwrap_or("");
                                    if evt == "bookTicker" {
                                        let parsed = (|| -> anyhow::Result<_> {
                                            Ok((bbo::parse_quote(payload_v, units[trade.market.index()])?,
                                                payload_v.get("E").and_then(|v| v.as_u64()).context("BBO missing E")?,
                                                payload_v.get("T").and_then(|v| v.as_u64()).context("BBO missing T")?))
                                        })();
                                        let Some((quote, event_time_ms, transaction_time_ms)) = recovery::recover_on_error(parsed, &recovery, "BBO parse/conversion failed") else {
                                            break;
                                        };
                                        let fresh = st.quotes[trade.market.index()].update_if_newer(quote);
                                        if fresh && !any_warmup && !recovery.get() && quote.changed_from(st.last_written_prices[trade.market.index()]) {
                                            write_row(&mut w, &mut st, MessageMeta {
                                                received_at_ms,
                                                event_time_ms,
                                                transaction_time_ms,
                                                event_type: trade.market.bbo_event_type(),
                                            })?;
                                            rows += 1;
                                        }
                                    } else if evt == "trade" {
                                        let Some(summary) = recovery::recover_on_error(
                                            crate::trade::parse_trade_message(&v, units[trade.market.index()]),
                                            &recovery, "trade parse/conversion failed",
                                        ) else { break; };
                                        if !trade.printed_first_trade_once {
                                            eprintln!(
                                                "[first-trade] {:?} {} stream={} payload={}",
                                                trade.market,
                                                trade.symbol,
                                                stream,
                                                trunc_str(payload_txt, 4096)
                                            );
                                            eprintln!(
                                                "[first-trade] {:?} {} E={} T={} p={} q={} t={} m={}",
                                                trade.market,
                                                trade.symbol,
                                                summary.event_time_ms,
                                                summary.transaction_time_ms,
                                                summary.price,
                                                summary.quantity,
                                                summary.trade_id,
                                                summary.is_buyer_maker
                                            );
                                            trade.printed_first_trade_once = true;
                                        }
                                        match trade.market {
                                            Market::PerpUsdt => {
                                                st.perp_usdt_trade = summary;
                                            }
                                            Market::PerpUsdc => {
                                                st.perp_usdc_trade = summary;
                                            }
                                        }
                                        if !any_warmup && !recovery.get() {
                                            write_row(&mut w, &mut st, MessageMeta {
                                                received_at_ms,
                                                event_time_ms: summary.event_time_ms,
                                                transaction_time_ms: summary.transaction_time_ms,
                                                event_type: trade.market.event_type(true),
                                            })?;
                                            rows += 1;
                                        }
                                    } else {
                                        // subscribe ack / other events ignored
                                    }
                                }
                                0x9 => {
                                    let pong = create_masked_control_frame(0x0A, payload);
                                    let _ = trade.tls.write_all(&pong);
                                    let _ = trade.tls.flush();
                                }
                                0x8 => {
                                    eprintln!(
                                        "[ws] close trade {:?} {} ({} bytes)",
                                        trade.market, trade.symbol, payload.len()
                                    );
                                    do_reconnect_after_drain = true;
                                }
                                _ => {}
                            }

                            trade.ws_buf.drain(..consumed);
                            if do_reconnect_after_drain {
                                trade.request_reconnect();
                                break;
                            }
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => {
                        eprintln!(
                            "[ws] read error trade {:?} {} -> reconnect",
                            trade.market, trade.symbol
                        );
                        trade.request_reconnect();
                        break;
                    }
                }
            }
        }

        // Handle rotate / idle for depth@0ms connections
        if perp_usdt_rotate || perp_usdt_idle {
            eprintln!(
                "[ws] perp_usdt {}",
                if perp_usdt_rotate { "rotate -> reconnect" } else { "idle -> reconnect" }
            );
            perp_usdt.request_reconnect();
        }
        if perp_usdc_rotate || perp_usdc_idle {
            eprintln!(
                "[ws] perp_usdc {}",
                if perp_usdc_rotate { "rotate -> reconnect" } else { "idle -> reconnect" }
            );
            perp_usdc.request_reconnect();
        }

        // Handle idle for depth10@100ms connections
        if perp_usdt_d10_idle || now.duration_since(perp_usdt_d10.connected_at) > rotate_after {
            eprintln!("[ws] perp_usdt_d10 idle -> reconnect");
            perp_usdt_d10.request_reconnect();
        }
        if perp_usdc_d10_idle || now.duration_since(perp_usdc_d10.connected_at) > rotate_after {
            eprintln!("[ws] perp_usdc_d10 idle -> reconnect");
            perp_usdc_d10.request_reconnect();
        }

        // Handle rotate / idle for trade connections
        if perp_usdt_trade_rotate || perp_usdt_trade_idle {
            eprintln!(
                "[ws] perp_usdt_trade {}",
                if perp_usdt_trade_rotate { "rotate -> reconnect" } else { "idle -> reconnect" }
            );
            perp_usdt_trade.request_reconnect();
        }
        if perp_usdc_trade_rotate || perp_usdc_trade_idle {
            eprintln!(
                "[ws] perp_usdc_trade {}",
                if perp_usdc_trade_rotate { "rotate -> reconnect" } else { "idle -> reconnect" }
            );
            perp_usdc_trade.request_reconnect();
        }

        if rows % 100 == 0 || last_flush.elapsed() > Duration::from_secs(1) {
            let _ = w.flush();
            last_flush = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_use_triggering_message_metadata_even_when_other_market_is_newer() {
        let path = std::env::temp_dir().join(format!("binance-row-test-{}.csv", std::process::id()));
        let mut w = BufWriter::new(File::create(&path).unwrap());
        let mut st = State::default();
        st.perp_usdc_trade.event_time_ms = 9999;
        writeln!(w, "{}", header()).unwrap();
        for (market, is_trade, label) in [
            (Market::PerpUsdt, true, "usdt_trade"),
            (Market::PerpUsdc, true, "usdc_trade"),
            (Market::PerpUsdt, false, "usdt_book"),
            (Market::PerpUsdc, false, "usdc_book"),
        ] {
            assert_eq!(market.event_type(is_trade), label);
            write_row(&mut w, &mut st, MessageMeta {
                received_at_ms: 3000,
                event_time_ms: 2000,
                transaction_time_ms: 1999,
                event_type: market.event_type(is_trade),
            }).unwrap();
        }
        write_zero_row(&mut w, header().split(',').count(), &mut st).unwrap();
        w.flush().unwrap();
        let csv = fs::read_to_string(&path).unwrap();
        fs::remove_file(path).unwrap();
        let lines: Vec<_> = csv.lines().collect();
        for line in &lines[1..5] {
            assert!(line.starts_with("3000,2000,1999,"));
        }
        for line in &lines[1..] {
            assert_eq!(line.split(',').count(), lines[0].split(',').count());
        }
        assert!(lines[5].split(',').all(|v| v == "0"));
    }

    #[test]
    fn depth_keeps_distinct_event_and_transaction_times() {
        let raw = br#"{"data":{"e":"depthUpdate","E":2000,"T":1999,"U":10,"u":11,"pu":9,"b":[],"a":[]}}"#;
        let v = sonic_rs::from_slice(raw).unwrap();
        let update = parse_depth_update(&v, raw, Units { price: units::Step::new("0.1").unwrap(), size: units::Step::new("0.001").unwrap() }).unwrap();
        assert_eq!((update.E, update.T), (2000, 1999));
    }
    #[test]
    fn bbo_and_trade_conversion_errors_request_reconnect_without_exiting() {
        let requested = Cell::new(false);
        let unit = Units { price: units::Step::new("0.1").unwrap(), size: units::Step::new("0.001").unwrap() };
        let quote = sonic_rs::from_str(r#"{"b":"100.01","a":"100.1","B":"1","A":"2","u":1}"#).unwrap();
        assert!(recovery::recover_on_error(bbo::parse_quote(&quote, unit), &requested, "BBO test").is_none());
        assert!(requested.replace(false));
        let trade = sonic_rs::from_str(r#"{"e":"trade","E":1,"T":1,"p":"100.01","q":"1","t":1,"m":true}"#).unwrap();
        assert!(recovery::recover_on_error(trade::parse_trade_message(&trade, unit), &requested, "trade test").is_none());
        assert!(requested.get());
    }

    #[test]
    fn live_recovery_refreshes_units_and_rebuilds_snapshot_after_failures() {
        if std::env::var_os("BINANCE_LIVE_RECOVERY").is_none() { return; }
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let symbols = ["BTCUSDT".to_string(), "BTCUSDC".to_string()];
        let requested = Rc::new(Cell::new(false));
        let mut session = connect_session_with_retry(&symbols, requested.clone());
        // Inject an off-tick BBO value through the production error handler.
        let bad = sonic_rs::from_str(r#"{"b":"100.000000001","a":"101","B":"1","A":"1","u":1}"#).unwrap();
        assert!(recovery::recover_on_error(bbo::parse_quote(&bad, session.units[0]), &requested, "injected BBO").is_none());
        assert!(requested.get());
        session = connect_session_with_retry(&symbols, requested.clone());
        requested.set(false);
        session.perp_usdt.fetch_snapshot_after_first_u(0).unwrap();
        let target = session.perp_usdt.snap_last_update_id;
        assert!(target > 0 && session.perp_usdt.book.bid_depth() > 0);
        // An update that starts after the snapshot proves the bridge was missed.
        session.perp_usdt.buffered.push(DepthUpdate {
            E: 1, T: 1, U: target + 1, u: target + 2, pu: Some(target), bids: vec![], asks: vec![],
        });
        assert!(!session.perp_usdt.try_bootstrap_from_buffer());
        assert!(requested.get());
        session = connect_session_with_retry(&symbols, requested.clone());
        requested.set(false);
        session.perp_usdt.fetch_snapshot_after_first_u(0).unwrap();
        let target = session.perp_usdt.snap_last_update_id;
        session.perp_usdt.buffered.push(DepthUpdate {
            E: 1, T: 1, U: target, u: target, pu: Some(target - 1), bids: vec![], asks: vec![],
        });
        assert!(session.perp_usdt.try_bootstrap_from_buffer());
        assert!(session.perp_usdt.book_ready);
        assert!(!requested.get());
        // A later continuity gap also schedules recovery through the connection.
        assert!(!session.perp_usdt.handle_depth_update(DepthUpdate {
            E: 2, T: 2, U: target + 2, u: target + 3, pu: Some(target + 1), bids: vec![], asks: vec![],
        }));
        session.perp_usdt.request_reconnect();
        assert!(requested.get());
        session = connect_session_with_retry(&symbols, requested.clone());
        session.perp_usdt.sync_started_at = Instant::now() - recovery::BRIDGE_TIMEOUT;
        assert!(session.perp_usdt.bridge_timed_out());
        println!("Live recovery passed: off-tick BBO, refreshed filters, snapshot gap, valid bridge, contiguity gap, and bridge timeout");
    }

}

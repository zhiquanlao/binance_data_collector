mod aggTrade;
mod orderbook;

use crate::aggTrade::AggTradeSummary;
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
    perp_usdt_depth_time: u64,
    perp_usdt_agg_time: u64,
    perp_usdc_depth_time: u64,
    perp_usdc_agg_time: u64,
    perp_usdt_top10: TopN,
    perp_usdc_top10: TopN,
    perp_usdt_agg: AggTradeSummary,
    perp_usdc_agg: AggTradeSummary,
}

fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_millis() as u64
}

fn max4(a: u64, b: u64, c: u64, d: u64) -> u64 {
    a.max(b).max(c).max(d)
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
    cols.push("binance_time".into());

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
        cols.push(format!("{m}_normal_quantity"));
        cols.push(format!("{m}_first_trade_id"));
        cols.push(format!("{m}_last_trade_id"));
        cols.push(format!("{m}_is_buyer_maker"));
    }
    cols.join(",")
}

fn write_row(w: &mut BufWriter<File>, st: &State) -> std::io::Result<()> {
    let local_time = now_epoch_ms();
    let binance_time = max4(
        st.perp_usdt_depth_time,
        st.perp_usdt_agg_time,
        st.perp_usdc_depth_time,
        st.perp_usdc_agg_time,
    );

    let mut b1 = ItoaBuf::new();
    let mut b2 = ItoaBuf::new();
    write!(w, "{},{}", b1.format(local_time), b2.format(binance_time))?;

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

    fn write_agg(w: &mut BufWriter<File>, a: &AggTradeSummary) -> std::io::Result<()> {
        write!(w, ",{}", a.price)?;
        write!(w, ",{}", a.quantity)?;
        write!(w, ",{}", a.normal_quantity)?;
        write!(w, ",{}", a.first_trade_id)?;
        write!(w, ",{}", a.last_trade_id)?;
        write!(w, ",{}", if a.is_buyer_maker { 1 } else { 0 })?;
        Ok(())
    }
    write_agg(w, &st.perp_usdt_agg)?;
    write_agg(w, &st.perp_usdc_agg)?;

    writeln!(w)?;
    Ok(())
}

fn write_zero_row(w: &mut BufWriter<File>, n_cols: usize) -> std::io::Result<()> {
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
    U: u64,
    u: u64,
    pu: Option<u64>,
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
}

#[allow(non_snake_case)]
fn parse_depth_update(v: &sonic_rs::Value, raw_payload: &[u8]) -> Result<DepthUpdate, String> {
    let payload = v.get("data").unwrap_or(v);
    let et = payload.get("e").and_then(|x| x.as_str()).unwrap_or("");
    if et != "depthUpdate" {
        return Err("not a depthUpdate event".to_string());
    }
    let E = payload.get("E").and_then(|x| x.as_u64()).unwrap_or(0);
    let U = payload.get("U").and_then(|x| x.as_u64()).unwrap_or(0);
    let u = payload.get("u").and_then(|x| x.as_u64()).unwrap_or(0);
    let pu = payload.get("pu").and_then(|x| x.as_u64());

    fn parse_side(x: Option<&sonic_rs::Value>, side_name: &str, raw_payload: &[u8]) -> Result<Vec<(f64, f64)>, String> {
        let mut out = Vec::new();
        let Some(arr) = x.and_then(|a| a.as_array()) else { return Ok(out) };
        for (idx, row) in arr.iter().enumerate() {
            let Some(cols) = row.as_array() else { continue };
            if cols.len() < 2 {
                continue;
            }
            let p_str = cols[0].as_str();
            let q_str = cols[1].as_str();

            let (p, p_ok) = if let Some(s) = p_str {
                match s.parse::<f64>() {
                    Ok(val) => {
                        if val > 0.0 {
                            (val, true)
                        } else {
                            (0.0, false)
                        }
                    }
                    Err(_) => {
                        let raw_snippet = String::from_utf8_lossy(&raw_payload[..raw_payload.len().min(1000)]);
                        eprintln!(
                            "[parse-error] {} price parse failed: side={} idx={} raw_price={:?} parsed_msg={}",
                            side_name, side_name, idx, s, raw_snippet
                        );
                        return Err(format!("price parse failed at {}[{}]: {:?}", side_name, idx, s));
                    }
                }
            } else {
                let raw_snippet = String::from_utf8_lossy(&raw_payload[..raw_payload.len().min(1000)]);
                eprintln!(
                    "[parse-error] {} price missing: side={} idx={} parsed_msg={}",
                    side_name, side_name, idx, raw_snippet
                );
                return Err(format!("price missing at {}[{}]", side_name, idx));
            };

            let q = if let Some(s) = q_str {
                match s.parse::<f64>() {
                    Ok(val) => val,
                    Err(_) => {
                        let raw_snippet = String::from_utf8_lossy(&raw_payload[..raw_payload.len().min(1000)]);
                        eprintln!(
                            "[parse-error] {} quantity parse failed: side={} idx={} raw_qty={:?} parsed_msg={}",
                            side_name, side_name, idx, s, raw_snippet
                        );
                        return Err(format!("quantity parse failed at {}[{}]: {:?}", side_name, idx, s));
                    }
                }
            } else {
                let raw_snippet = String::from_utf8_lossy(&raw_payload[..raw_payload.len().min(1000)]);
                eprintln!(
                    "[parse-error] {} quantity missing: side={} idx={} parsed_msg={}",
                    side_name, side_name, idx, raw_snippet
                );
                return Err(format!("quantity missing at {}[{}]", side_name, idx));
            };

            if !p_ok && p == 0.0 {
                eprintln!(
                    "[parse-warning] {} zero price detected: side={} idx={} qty={} (this might be a deletion)",
                    side_name, side_name, idx, q
                );
            }

            out.push((p, q));
        }
        Ok(out)
    }

    let bids = parse_side(payload.get("b"), "bids", raw_payload)?;
    let asks = parse_side(payload.get("a"), "asks", raw_payload)?;
    Ok(DepthUpdate {
        E,
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
    market: Market,
    ws_host: &'static str,
    http_host: &'static str,
    http_path: &'static str,
    symbol: String,
    ws_path: String,

    tls: StreamOwned<ClientConnection, std::net::TcpStream>,
    ws_buf: Vec<u8>,
    tmp: [u8; 16 * 1024],

    // reconnect/backoff
    backoff_ms: u64,
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

    // ---- debug / one-time logs ----
    printed_snapshot_json_once: bool,
    printed_first_depth_once: bool,

    // ---- warmup after reconnect ----
    warmup_loop: bool,
}

impl MarketConn {
    fn new(market: Market, symbol: &str) -> anyhow::Result<Self> {
        let symbol = symbol.trim().to_ascii_uppercase();
        let stream_symbol = symbol_to_stream_symbol(&symbol);
        // Both PerpUsdt and PerpUsdc use the fstream endpoint.
        // Depth@0ms now uses the /public/stream path.
        let ws_host = "fstream.binance.com";
        let http_host = "fapi.binance.com";
        let http_path = "/fapi/v1/depth";
        let ws_path = format!("/public/stream?streams={s}@depth", s = stream_symbol);
        eprintln!(
            "[ws] connecting {:?} {} host={} path={}",
            market, symbol, ws_host, ws_path
        );
        let tls = connect_binance_ws_blocking(ws_host, &ws_path)?;
        eprintln!("[ws] connected {:?} {}", market, symbol);

        let c = Self {
            market,
            ws_host,
            http_host,
            http_path,
            symbol,
            ws_path,
            tls,
            ws_buf: Vec::with_capacity(64 * 1024),
            tmp: [0u8; 16 * 1024],
            backoff_ms: 100,
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
            printed_snapshot_json_once: false,
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

    fn fetch_snapshot_after_first_u_with_retry(&mut self, first_u: u64) {
        let mut backoff_ms: u64 = 250;
        let mut rng = rand::thread_rng();
        let mut printed_full = false;
        let mut last_log_at = Instant::now() - Duration::from_secs(3600);
        let mut suppressed: u64 = 0;
        loop {
            match fetch_depth_snapshot_blocking_with_body(
                self.http_host,
                self.http_path,
                &self.symbol,
                1000,
            ) {
                Ok((last, bids, asks, body)) => {
                    if last < first_u {
                        eprintln!(
                            "[snapshot] {:?} {} too old: lastUpdateId={} < first_event.U={} (refetching...)",
                            self.market, self.symbol, last, first_u
                        );
                        continue;
                    }

                    if !self.printed_snapshot_json_once {
                        eprintln!(
                            "[snapshot] {:?} {} fetched lastUpdateId={} ({} bytes)",
                            self.market,
                            self.symbol,
                            last,
                            body.len()
                        );
                        const LIM: usize = 64 * 1024;
                        let s = String::from_utf8_lossy(&body);
                        eprintln!(
                            "[snapshot] {:?} {} json={}",
                            self.market,
                            self.symbol,
                            trunc_str(&s, LIM)
                        );
                        self.printed_snapshot_json_once = true;
                    }

                    self.book.clear();
                    on_binance_depth_update(&self.symbol, &mut self.book, last, last, None, &bids, &asks, 0);
                    self.snap_last_update_id = last;
                    self.book_ready = false;
                    self.prev_u = 0;
                    return;
                }
                Err(e) => {
                    if !printed_full {
                        eprintln!("[snapshot] {:?} {} failed: {e:#}", self.market, self.symbol);
                        printed_full = true;
                    } else {
                        let now = Instant::now();
                        if now.duration_since(last_log_at) < Duration::from_secs(1) {
                            suppressed = suppressed.saturating_add(1);
                        } else {
                            if suppressed > 0 {
                                eprintln!(
                                    "[snapshot] {:?} {} still failing (suppressed {} msgs); retry in ~{}ms",
                                    self.market, self.symbol, suppressed, backoff_ms
                                );
                                suppressed = 0;
                            } else {
                                eprintln!(
                                    "[snapshot] {:?} {} still failing; retry in ~{}ms",
                                    self.market, self.symbol, backoff_ms
                                );
                            }
                            last_log_at = now;
                        }
                    }
                    let jitter = rng.gen_range(0..(backoff_ms / 2).max(1));
                    std::thread::sleep(Duration::from_millis(backoff_ms + jitter));
                    backoff_ms = (backoff_ms * 3 / 2).min(10_000);
                }
            }
        }
    }

    fn reconnect(&mut self) -> anyhow::Result<()> {
        eprintln!(
            "[ws] reconnecting {:?} {} host={} path={}",
            self.market, self.symbol, self.ws_host, self.ws_path
        );
        let tls = connect_binance_ws_blocking(self.ws_host, &self.ws_path)?;
        self.tls = tls;
        self.ws_buf.clear();
        self.connected_at = Instant::now();
        self.last_rx_at = Instant::now();
        self.last_ping_at = Instant::now();
        self.backoff_ms = 100;
        self.book.clear();
        self.reset_book_sync();
        self.warmup_loop = true;
        eprintln!("[ws] reconnected {:?} {}", self.market, self.symbol);
        Ok(())
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

        // Perp: first event where U <= lastUpdateId <= u
        let target = self.snap_last_update_id;
        let mut bi = None;
        for (i, ev) in self.buffered.iter().enumerate() {
            if ev.U <= target && ev.u >= target {
                bi = Some(i);
                break;
            }
        }
        let Some(bi) = bi else {
            return false;
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
                        return false;
                    }
                } else if ev.U != self.prev_u + 1 {
                    self.log_ob_err("bootstrap contiguity gap (no pu) -> resync");
                    self.reset_book_sync();
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
                if self.buffered.len() >= 8192 {
                    self.log_ob_err("pre-snapshot buffer overflow -> resync");
                    self.reset_book_sync();
                }
                let first_u = ev.U;
                self.buffered.push(ev);
                self.fetch_snapshot_after_first_u_with_retry(first_u);
                return self.try_bootstrap_from_buffer();
            }

            if self.buffered.len() >= 8192 {
                self.log_ob_err("pre-bridge buffer overflow -> resync");
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
    market: Market,
    ws_host: &'static str,
    symbol: String,
    ws_path: String,

    tls: StreamOwned<ClientConnection, std::net::TcpStream>,
    ws_buf: Vec<u8>,
    tmp: [u8; 16 * 1024],

    backoff_ms: u64,
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
    fn new(market: Market, symbol: &str) -> anyhow::Result<Self> {
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
            market,
            ws_host,
            symbol,
            ws_path,
            tls,
            ws_buf: Vec::with_capacity(64 * 1024),
            tmp: [0u8; 16 * 1024],
            backoff_ms: 100,
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

    fn reconnect(&mut self) -> anyhow::Result<()> {
        eprintln!(
            "[ws] reconnecting depth10 {:?} {} host={} path={}",
            self.market, self.symbol, self.ws_host, self.ws_path
        );
        let tls = connect_binance_ws_blocking(self.ws_host, &self.ws_path)?;
        self.tls = tls;
        self.ws_buf.clear();
        self.connected_at = Instant::now();
        self.last_rx_at = Instant::now();
        self.last_ping_at = Instant::now();
        self.backoff_ms = 100;
        self.book.clear();
        self.last_u = 0;
        self.paused_u = None;
        self.paused_at = Instant::now();
        self.warmup_loop = true;
        eprintln!("[ws] reconnected depth10 {:?} {}", self.market, self.symbol);
        Ok(())
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

// ---- AggTradeConn: handles aggTrade for perp markets ----
// Connects to the original endpoint: wss://fstream.binance.com/stream?streams={symbol}@aggTrade

struct AggTradeConn {
    market: Market,
    ws_host: &'static str,
    symbol: String,
    ws_path: String,

    tls: StreamOwned<ClientConnection, std::net::TcpStream>,
    ws_buf: Vec<u8>,
    tmp: [u8; 16 * 1024],

    backoff_ms: u64,
    connected_at: Instant,
    last_rx_at: Instant,
    last_ping_at: Instant,

    printed_first_aggtrade_once: bool,

    // ---- warmup after reconnect ----
    warmup_loop: bool,
}

impl AggTradeConn {
    fn new(market: Market, symbol: &str) -> anyhow::Result<Self> {
        let symbol = symbol.trim().to_ascii_uppercase();
        let stream_symbol = symbol_to_stream_symbol(&symbol);
        let ws_host = "fstream.binance.com";
        let ws_path = format!("/stream?streams={s}@aggTrade", s = stream_symbol);
        eprintln!(
            "[ws] connecting aggTrade {:?} {} host={} path={}",
            market, symbol, ws_host, ws_path
        );
        let tls = connect_binance_ws_blocking(ws_host, &ws_path)?;
        eprintln!("[ws] connected aggTrade {:?} {}", market, symbol);

        Ok(Self {
            market,
            ws_host,
            symbol,
            ws_path,
            tls,
            ws_buf: Vec::with_capacity(64 * 1024),
            tmp: [0u8; 16 * 1024],
            backoff_ms: 100,
            connected_at: Instant::now(),
            last_rx_at: Instant::now(),
            last_ping_at: Instant::now(),
            printed_first_aggtrade_once: false,
            warmup_loop: false,
        })
    }

    fn reconnect(&mut self) -> anyhow::Result<()> {
        eprintln!(
            "[ws] reconnecting aggTrade {:?} {} host={} path={}",
            self.market, self.symbol, self.ws_host, self.ws_path
        );
        let tls = connect_binance_ws_blocking(self.ws_host, &self.ws_path)?;
        self.tls = tls;
        self.ws_buf.clear();
        self.connected_at = Instant::now();
        self.last_rx_at = Instant::now();
        self.last_ping_at = Instant::now();
        self.backoff_ms = 100;
        self.warmup_loop = true;
        eprintln!("[ws] reconnected aggTrade {:?} {}", self.market, self.symbol);
        Ok(())
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

fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let args: Vec<String> = std::env::args().collect();
    // Input base symbol like "BTC". We will collect:
    // - <BASE>USDT perp (depth via /public/stream, aggTrade via /stream)
    // - <BASE>USDC perp (depth via /public/stream, aggTrade via /stream)
    // - depth10@100ms for both perps via /public/stream
    let base = args.get(1).cloned().unwrap_or_else(|| "BTC".to_string());
    let base = base.trim().to_ascii_uppercase();
    let perp_usdt_symbol = format!("{base}USDT");
    let perp_usdc_symbol = format!("{base}USDC");

    fs::create_dir_all("logs").context("create logs/")?;
    let ts = now_epoch_ms();
    let filename = format!("logs/{}_binance_{}.csv", ts, base);

    let f = File::create(&filename).with_context(|| format!("create {filename}"))?;
    let mut w = BufWriter::new(f);
    let header_line = header();
    let n_cols = header_line.split(',').count();
    writeln!(w, "{}", header_line)?;
    w.flush()?;

    let mut st = State::default();
    let mut rows: u64 = 0;

    // depth connections (via /public/stream)
    let mut perp_usdt =
        MarketConn::new(Market::PerpUsdt, &perp_usdt_symbol).context("perp_usdt connect")?;
    let mut perp_usdc =
        MarketConn::new(Market::PerpUsdc, &perp_usdc_symbol).context("perp_usdc connect")?;

    // depth10@100ms connections (via /public/stream)
    let mut perp_usdt_d10 =
        Depth10Conn::new(Market::PerpUsdt, &perp_usdt_symbol).context("perp_usdt depth10 connect")?;
    let mut perp_usdc_d10 =
        Depth10Conn::new(Market::PerpUsdc, &perp_usdc_symbol).context("perp_usdc depth10 connect")?;

    // aggTrade connections (via original /stream)
    let mut perp_usdt_agg =
        AggTradeConn::new(Market::PerpUsdt, &perp_usdt_symbol).context("perp_usdt aggTrade connect")?;
    let mut perp_usdc_agg =
        AggTradeConn::new(Market::PerpUsdc, &perp_usdc_symbol).context("perp_usdc aggTrade connect")?;

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
            fd: perp_usdt_agg.tls.get_ref().as_raw_fd(),
            events: POLLIN,
            revents: 0,
        },
        pollfd {
            fd: perp_usdc_agg.tls.get_ref().as_raw_fd(),
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
        let now = Instant::now();

        // Check if any connection is in warmup mode (first loop after reconnect).
        // Flags are cleared HERE (not at the bottom) so that a reconnect() called
        // mid-iteration sets warmup_loop=true AFTER this clear, meaning the flag
        // survives to the next iteration's drain phase and actually suppresses output.
        let any_warmup = perp_usdt.warmup_loop || perp_usdc.warmup_loop
            || perp_usdt_d10.warmup_loop || perp_usdc_d10.warmup_loop
            || perp_usdt_agg.warmup_loop || perp_usdc_agg.warmup_loop;
        perp_usdt.warmup_loop = false;
        perp_usdc.warmup_loop = false;
        perp_usdt_d10.warmup_loop = false;
        perp_usdc_d10.warmup_loop = false;
        perp_usdt_agg.warmup_loop = false;
        perp_usdc_agg.warmup_loop = false;

        // keepalive pings
        perp_usdt.maybe_send_ping(now);
        perp_usdc.maybe_send_ping(now);
        perp_usdt_d10.maybe_send_ping(now);
        perp_usdc_d10.maybe_send_ping(now);
        perp_usdt_agg.maybe_send_ping(now);
        perp_usdc_agg.maybe_send_ping(now);

        // periodic connection rotation (Binance connection limits friendly)
        let rotate_after = Duration::from_secs(5 * 3600 + 50 * 60);
        let perp_usdt_rotate = now.duration_since(perp_usdt.connected_at) > rotate_after;
        let perp_usdc_rotate = now.duration_since(perp_usdc.connected_at) > rotate_after;
        let perp_usdt_agg_rotate = now.duration_since(perp_usdt_agg.connected_at) > rotate_after;
        let perp_usdc_agg_rotate = now.duration_since(perp_usdc_agg.connected_at) > rotate_after;

        // deadman timer: reconnect if no bytes received for a while
        let idle_timeout = Duration::from_secs(90);
        let perp_usdt_idle = now.duration_since(perp_usdt.last_rx_at) > idle_timeout;
        let perp_usdc_idle = now.duration_since(perp_usdc.last_rx_at) > idle_timeout;
        let perp_usdt_agg_idle = now.duration_since(perp_usdt_agg.last_rx_at) > idle_timeout;
        let perp_usdc_agg_idle = now.duration_since(perp_usdc_agg.last_rx_at) > idle_timeout;

        // refresh fds in case of reconnect
        fds[0].fd = perp_usdt.tls.get_ref().as_raw_fd();
        fds[1].fd = perp_usdc.tls.get_ref().as_raw_fd();
        fds[2].fd = perp_usdt_d10.tls.get_ref().as_raw_fd();
        fds[3].fd = perp_usdc_d10.tls.get_ref().as_raw_fd();
        fds[4].fd = perp_usdt_agg.tls.get_ref().as_raw_fd();
        fds[5].fd = perp_usdc_agg.tls.get_ref().as_raw_fd();

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
                match d10.tls.read(&mut d10.tmp) {
                    Ok(0) => {
                        eprintln!("[ws] EOF depth10 {:?} {} -> reconnect", d10.market, d10.symbol);
                        let mut rng = rand::thread_rng();
                        loop {
                            let jitter = rng.gen_range(0..(d10.backoff_ms / 2).max(1));
                            std::thread::sleep(Duration::from_millis(d10.backoff_ms + jitter));
                            match d10.reconnect() {
                                Ok(_) => break,
                                Err(_) => {
                                    d10.backoff_ms = (d10.backoff_ms * 3 / 2).min(10_000);
                                    continue;
                                }
                            }
                        }
                        break 'read;
                    }
                    Ok(n) => {
                        d10.last_rx_at = Instant::now();
                        d10.ws_buf.extend_from_slice(&d10.tmp[..n]);
                        loop {
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
                                            d10.ws_buf.drain(..consumed);
                                            continue;
                                        }
                                    };
                                    let payload_v = v.get("data").unwrap_or(&v);
                                    let stream = v.get("stream").and_then(|x| x.as_str()).unwrap_or("");
                                    let evt = payload_v.get("e").and_then(|x| x.as_str()).unwrap_or("");
                                    if evt == "depthUpdate" {
                                        match parse_depth_update(&v, payload) {
                                            Ok(upd) => {
                                                let bids_best_to_worst: Vec<(f64, f64)> = upd.bids.clone();
                                                let asks_best_to_worst: Vec<(f64, f64)> = upd.asks.clone();
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
                                                            let mut rng = rand::thread_rng();
                                                            loop {
                                                                let jitter = rng.gen_range(0..(perp_usdt.backoff_ms / 2).max(1));
                                                                std::thread::sleep(Duration::from_millis(perp_usdt.backoff_ms + jitter));
                                                                match perp_usdt.reconnect() {
                                                                    Ok(_) => {
                                                                        st.perp_usdt_top10 = TopN::default();
                                                                        let _ = write_zero_row(&mut w, n_cols);
                                                                        rows += 1;
                                                                        break;
                                                                    }
                                                                    Err(_) => {
                                                                        perp_usdt.backoff_ms = (perp_usdt.backoff_ms * 3 / 2).min(10_000);
                                                                        continue;
                                                                    }
                                                                }
                                                            }
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
                                                            let mut rng = rand::thread_rng();
                                                            loop {
                                                                let jitter = rng.gen_range(0..(perp_usdc.backoff_ms / 2).max(1));
                                                                std::thread::sleep(Duration::from_millis(perp_usdc.backoff_ms + jitter));
                                                                match perp_usdc.reconnect() {
                                                                    Ok(_) => {
                                                                        st.perp_usdc_top10 = TopN::default();
                                                                        let _ = write_zero_row(&mut w, n_cols);
                                                                        rows += 1;
                                                                        break;
                                                                    }
                                                                    Err(_) => {
                                                                        perp_usdc.backoff_ms = (perp_usdc.backoff_ms * 3 / 2).min(10_000);
                                                                        continue;
                                                                    }
                                                                }
                                                            }
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
                                    let _ = d10.reconnect();
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
                        let _ = d10.reconnect();
                        break 'read;
                    }
                }
            }
        }

        // ---- Drain perp depth sockets ----
        for c in [&mut perp_usdt, &mut perp_usdc] {
            loop {
                match c.tls.read(&mut c.tmp) {
                    Ok(0) => {
                        eprintln!("[ws] EOF {:?} {} -> reconnect", c.market, c.symbol);
                        let mut rng = rand::thread_rng();
                        loop {
                            let jitter = rng.gen_range(0..(c.backoff_ms / 2).max(1));
                            std::thread::sleep(Duration::from_millis(c.backoff_ms + jitter));
                            match c.reconnect() {
                                Ok(_) => {
                                    match c.market {
                                        Market::PerpUsdt => st.perp_usdt_top10 = TopN::default(),
                                        Market::PerpUsdc => st.perp_usdc_top10 = TopN::default(),
                                    }
                                    let _ = write_zero_row(&mut w, n_cols);
                                    rows += 1;
                                    break;
                                }
                                Err(_) => {
                                    c.backoff_ms = (c.backoff_ms * 3 / 2).min(10_000);
                                    continue;
                                }
                            }
                        }
                        break;
                    }
                    Ok(n) => {
                        c.last_rx_at = Instant::now();
                        c.ws_buf.extend_from_slice(&c.tmp[..n]);
                        loop {
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
                                            c.ws_buf.drain(..consumed);
                                            continue;
                                        }
                                    };
                                    let payload_v = v.get("data").unwrap_or(&v);
                                    let stream = v.get("stream").and_then(|x| x.as_str()).unwrap_or("");
                                    let evt = payload_v.get("e").and_then(|x| x.as_str()).unwrap_or("");
                                    if evt == "depthUpdate" {
                                        match parse_depth_update(&v, payload) {
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
                                                            st.perp_usdt_depth_time = upd.E;
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
                                                            st.perp_usdc_depth_time = upd.E;
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
                                                    if !any_warmup {
                                                        write_row(&mut w, &st)?;
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
                                let mut rng = rand::thread_rng();
                                loop {
                                    let jitter = rng.gen_range(0..(c.backoff_ms / 2).max(1));
                                    std::thread::sleep(Duration::from_millis(c.backoff_ms + jitter));
                                    match c.reconnect() {
                                        Ok(_) => {
                                            match c.market {
                                                Market::PerpUsdt => st.perp_usdt_top10 = TopN::default(),
                                                Market::PerpUsdc => st.perp_usdc_top10 = TopN::default(),
                                            }
                                            let _ = write_zero_row(&mut w, n_cols);
                                            rows += 1;
                                            break;
                                        }
                                        Err(_) => {
                                            c.backoff_ms = (c.backoff_ms * 3 / 2).min(10_000);
                                            continue;
                                        }
                                    }
                                }
                                break;
                            }
                            if need_resync_after_drain {
                                let mut rng = rand::thread_rng();
                                loop {
                                    let jitter = rng.gen_range(0..(c.backoff_ms / 2).max(1));
                                    std::thread::sleep(Duration::from_millis(c.backoff_ms + jitter));
                                    match c.reconnect() {
                                        Ok(_) => {
                                            match c.market {
                                                Market::PerpUsdt => st.perp_usdt_top10 = TopN::default(),
                                                Market::PerpUsdc => st.perp_usdc_top10 = TopN::default(),
                                            }
                                            let _ = write_zero_row(&mut w, n_cols);
                                            rows += 1;
                                            break;
                                        }
                                        Err(_) => {
                                            c.backoff_ms = (c.backoff_ms * 3 / 2).min(10_000);
                                            continue;
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => {
                        eprintln!("[ws] read error {:?} {} -> reconnect", c.market, c.symbol);
                        let mut rng = rand::thread_rng();
                        loop {
                            let jitter = rng.gen_range(0..(c.backoff_ms / 2).max(1));
                            std::thread::sleep(Duration::from_millis(c.backoff_ms + jitter));
                            match c.reconnect() {
                                Ok(_) => {
                                    match c.market {
                                        Market::PerpUsdt => st.perp_usdt_top10 = TopN::default(),
                                        Market::PerpUsdc => st.perp_usdc_top10 = TopN::default(),
                                    }
                                    let _ = write_zero_row(&mut w, n_cols);
                                    rows += 1;
                                    break;
                                }
                                Err(_) => {
                                    c.backoff_ms = (c.backoff_ms * 3 / 2).min(10_000);
                                    continue;
                                }
                            }
                        }
                        break;
                    }
                }
            }
        }

        // ---- Drain aggTrade sockets (original /stream endpoint) ----
        for agg in [&mut perp_usdt_agg, &mut perp_usdc_agg] {
            loop {
                match agg.tls.read(&mut agg.tmp) {
                    Ok(0) => {
                        eprintln!("[ws] EOF aggTrade {:?} {} -> reconnect", agg.market, agg.symbol);
                        let mut rng = rand::thread_rng();
                        loop {
                            let jitter = rng.gen_range(0..(agg.backoff_ms / 2).max(1));
                            std::thread::sleep(Duration::from_millis(agg.backoff_ms + jitter));
                            match agg.reconnect() {
                                Ok(_) => break,
                                Err(_) => {
                                    agg.backoff_ms = (agg.backoff_ms * 3 / 2).min(10_000);
                                    continue;
                                }
                            }
                        }
                        break;
                    }
                    Ok(n) => {
                        agg.last_rx_at = Instant::now();
                        agg.ws_buf.extend_from_slice(&agg.tmp[..n]);
                        loop {
                            let Ok((consumed, opcode, payload)) = parse_ws_frame(&agg.ws_buf) else {
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
                                                "[ws] aggTrade {:?} {} invalid json: {}",
                                                agg.market,
                                                agg.symbol,
                                                trunc_str(payload_txt, 4096)
                                            );
                                            agg.ws_buf.drain(..consumed);
                                            continue;
                                        }
                                    };
                                    let payload_v = v.get("data").unwrap_or(&v);
                                    let stream = v.get("stream").and_then(|x| x.as_str()).unwrap_or("");
                                    let evt = payload_v.get("e").and_then(|x| x.as_str()).unwrap_or("");
                                    if evt == "aggTrade" {
                                        let summary = crate::aggTrade::summarize_aggtrade_message(&v);
                                        if !agg.printed_first_aggtrade_once {
                                            eprintln!(
                                                "[first-aggTrade] {:?} {} stream={} payload={}",
                                                agg.market,
                                                agg.symbol,
                                                stream,
                                                trunc_str(payload_txt, 4096)
                                            );
                                            eprintln!(
                                                "[first-aggTrade] {:?} {} E={} p={} q={} nq={} f={} l={} m={}",
                                                agg.market,
                                                agg.symbol,
                                                summary.event_time_ms,
                                                summary.price,
                                                summary.quantity,
                                                summary.normal_quantity,
                                                summary.first_trade_id,
                                                summary.last_trade_id,
                                                summary.is_buyer_maker
                                            );
                                            agg.printed_first_aggtrade_once = true;
                                        }
                                        match agg.market {
                                            Market::PerpUsdt => {
                                                st.perp_usdt_agg_time = summary.event_time_ms;
                                                st.perp_usdt_agg = summary;
                                            }
                                            Market::PerpUsdc => {
                                                st.perp_usdc_agg_time = summary.event_time_ms;
                                                st.perp_usdc_agg = summary;
                                            }
                                        }
                                        if !any_warmup {
                                            write_row(&mut w, &st)?;
                                            rows += 1;
                                        }
                                    } else {
                                        // subscribe ack / other events ignored
                                    }
                                }
                                0x9 => {
                                    let pong = create_masked_control_frame(0x0A, payload);
                                    let _ = agg.tls.write_all(&pong);
                                    let _ = agg.tls.flush();
                                }
                                0x8 => {
                                    eprintln!(
                                        "[ws] close aggTrade {:?} {} ({} bytes)",
                                        agg.market, agg.symbol, payload.len()
                                    );
                                    do_reconnect_after_drain = true;
                                }
                                _ => {}
                            }

                            agg.ws_buf.drain(..consumed);
                            if do_reconnect_after_drain {
                                let mut rng = rand::thread_rng();
                                loop {
                                    let jitter = rng.gen_range(0..(agg.backoff_ms / 2).max(1));
                                    std::thread::sleep(Duration::from_millis(agg.backoff_ms + jitter));
                                    match agg.reconnect() {
                                        Ok(_) => break,
                                        Err(_) => {
                                            agg.backoff_ms = (agg.backoff_ms * 3 / 2).min(10_000);
                                            continue;
                                        }
                                    }
                                }
                                break;
                            }
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => {
                        eprintln!(
                            "[ws] read error aggTrade {:?} {} -> reconnect",
                            agg.market, agg.symbol
                        );
                        let mut rng = rand::thread_rng();
                        loop {
                            let jitter = rng.gen_range(0..(agg.backoff_ms / 2).max(1));
                            std::thread::sleep(Duration::from_millis(agg.backoff_ms + jitter));
                            match agg.reconnect() {
                                Ok(_) => break,
                                Err(_) => {
                                    agg.backoff_ms = (agg.backoff_ms * 3 / 2).min(10_000);
                                    continue;
                                }
                            }
                        }
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
            if perp_usdt.reconnect().is_ok() {
                st.perp_usdt_top10 = TopN::default();
                let _ = write_zero_row(&mut w, n_cols);
                rows += 1;
            }
        }
        if perp_usdc_rotate || perp_usdc_idle {
            eprintln!(
                "[ws] perp_usdc {}",
                if perp_usdc_rotate { "rotate -> reconnect" } else { "idle -> reconnect" }
            );
            if perp_usdc.reconnect().is_ok() {
                st.perp_usdc_top10 = TopN::default();
                let _ = write_zero_row(&mut w, n_cols);
                rows += 1;
            }
        }

        // Handle rotate / idle for aggTrade connections
        if perp_usdt_agg_rotate || perp_usdt_agg_idle {
            eprintln!(
                "[ws] perp_usdt_agg {}",
                if perp_usdt_agg_rotate { "rotate -> reconnect" } else { "idle -> reconnect" }
            );
            let _ = perp_usdt_agg.reconnect();
        }
        if perp_usdc_agg_rotate || perp_usdc_agg_idle {
            eprintln!(
                "[ws] perp_usdc_agg {}",
                if perp_usdc_agg_rotate { "rotate -> reconnect" } else { "idle -> reconnect" }
            );
            let _ = perp_usdc_agg.reconnect();
        }

        if rows % 100 == 0 || last_flush.elapsed() > Duration::from_secs(1) {
            let _ = w.flush();
            last_flush = Instant::now();
        }
    }
}

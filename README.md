# Binance Perp Data Collector

A low-latency, perpetual-futures market data collector for Binance USD-M futures.  
Streams live order book (depth + depth10@100ms) and individual trades (trade) for multiple symbols simultaneously, writes tick-level CSV data locally, converts to Parquet, and uploads to Google Cloud Storage.

---

## Table of Contents / 目录

- [English](#english)
  - [Architecture Overview](#architecture-overview)
  - [Data Collected](#data-collected)
  - [Orderbook Data Structure](#orderbook-data-structure)
  - [CSV Format & Reconnect Markers](#csv-format--reconnect-markers)
  - [Requirements](#requirements)
  - [How to Run](#how-to-run)
  - [Configuration](#configuration)
  - [File Layout](#file-layout)
- [中文说明](#中文说明)
  - [架构概览](#架构概览)
  - [采集数据内容](#采集数据内容)
  - [订单簿数据结构](#订单簿数据结构)
  - [CSV 格式与重连标记](#csv-格式与重连标记)
  - [环境依赖](#环境依赖)
  - [如何运行](#如何运行)
  - [配置项](#配置项)
  - [文件结构](#文件结构)

---

## English

### Architecture Overview

The project has two layers:

| Layer | File | Role |
|---|---|---|
| **Core collector** | `src/main.rs` (Rust) | Opens 6 WebSocket connections per symbol, maintains the order book in memory, writes tick-level CSV rows |
| **Orchestrator** | `data_collection.py` (Python) | Manages one Rust process per symbol, monitors disk usage, converts CSV → Parquet, uploads to GCS |

**Per-symbol WebSocket connections (6 total):**

| Connection | Endpoint |
|---|---|
| `perp_usdt` — depth | `fstream.binance.com/public/stream?streams={sym}usdt@depth@0ms` |
| `perp_usdc` — depth | `fstream.binance.com/public/stream?streams={sym}usdc@depth@0ms` |
| `perp_usdt_d10` — depth10@100ms | `fstream.binance.com/public/stream?streams={sym}usdt@depth10@100ms` |
| `perp_usdc_d10` — depth10@100ms | `fstream.binance.com/public/stream?streams={sym}usdc@depth10@100ms` |
| `perp_usdt_trade` — trade + bookTicker | `fstream.binance.com/public/stream?streams={sym}usdt@trade/{sym}usdt@bookTicker` |
| `perp_usdc_trade` — trade + bookTicker | `fstream.binance.com/public/stream?streams={sym}usdc@trade/{sym}usdc@bookTicker` |

The depth and depth10@100ms streams are compared on every matching update sequence number (`u`). A mismatch requests recovery of all six streams for the base symbol, with freshly loaded symbol filters and new REST snapshots.

---

### Data Collected

For each symbol (e.g. BTC → BTCUSDT perp + BTCUSDC perp):

- **Top-10 order book** (bid/ask price & size, levels 1–10) — updated on every depth event
- **Individual trade** — price, quantity, trade ID, buyer-maker flag
- **Timestamps** — `local_time` (local receipt time immediately after the socket read, ms), `binance_event_time` (`E`), and `binance_transaction_time` (`T`, trade/transaction time), all Unix milliseconds. Messages completed in the same read share its receipt time. Each row has an `event_type`: `usdc_trade`, `usdc_book`, `usdt_book`, `usdt_trade`, `usdc_bbo`, or `usdt_bbo`

---

### Orderbook Data Structure

The in-memory order book uses a **fast, cache-friendly structure with no branch-prediction failures on updates**.

The design is based on the approach presented in:

> **"When Nanoseconds Matter: Ultrafast Trading Systems in C++ – David Gross – CppCon 2024"**

Key properties:
- Fixed-size arrays (no heap allocation on the hot path)
- Price levels stored in sorted order so top-N extraction is a simple slice copy
- Updates either insert, modify, or delete a level using branchless index arithmetic
- Separate bid and ask arrays; each fits within a small number of cache lines

---

### CSV Format & Reconnect Markers

Each CSV file covers one symbol and one run of the collector. The header is:

```
local_time, binance_event_time, binance_transaction_time, event_type,
perp_usdt_bid_price_{1..10}, perp_usdt_ask_price_{1..10},
perp_usdt_bid_size_{1..10},  perp_usdt_ask_size_{1..10},
perp_usdc_bid_price_{1..10}, perp_usdc_ask_price_{1..10},
perp_usdc_bid_size_{1..10},  perp_usdc_ask_size_{1..10},
perp_usdt_price, perp_usdt_quantity, perp_usdt_trade_id, perp_usdt_is_buyer_maker,
perp_usdc_price, perp_usdc_quantity, perp_usdc_trade_id, perp_usdc_is_buyer_maker,
perp_usdt_best_bid_price, perp_usdt_best_ask_price, perp_usdt_best_bid_size, perp_usdt_best_ask_size, perp_usdt_best_update_id,
perp_usdc_best_bid_price, perp_usdc_best_ask_price, perp_usdc_best_bid_size, perp_usdc_best_ask_size, perp_usdc_best_update_id
```

All price and size/quantity columns are signed 64-bit integer counts: price / `PRICE_FILTER.tickSize` and quantity / `LOT_SIZE.stepSize`, loaded separately for each symbol from `/fapi/v1/exchangeInfo`. Parsing and division use checked decimal integer arithmetic throughout, with no floating point or rounding. Off-step values are rejected. Multiply a stored count by its step to recover the original value. Each CSV has a `<filename>.csv.units.json` sidecar containing its symbol filters; conversion embeds this JSON in Parquet metadata under `binance_units` and removes the sidecar after successful upload. Legacy decimal CSV files still use the old float schema.

The `best_*` columns contain the freshest valid quote for each market, including bid/ask prices, sizes, and `best_update_id`. Both depth and `bookTicker` use the exchange order-book update ID: an older update cannot replace a newer quote, and BBO takes precedence on equal IDs. A BBO row (`usdt_bbo` / `usdc_bbo`) is written only when either price differs from that market's effective prices in the last written row. Size-only changes update the cached sizes but do not emit rows. Changes in bid or ask prices with an unchanged midpoint do emit rows. Trade rows retain the freshest effective quotes. Reconnect markers clear the price baseline. The 10-level book columns continue to reflect depth updates only and may lag the effective quote.

Recoverable conversion, JSON parsing, snapshot, and connection failures request recovery instead of terminating the collector or silently skipping a trade. Recovery re-fetches `/fapi/v1/exchangeInfo` for both markets before reopening the streams or requesting any REST snapshot. Refresh/connection failures retry with jittered backoff, capped at approximately 10 seconds plus jitter. All six streams and cached values for the base symbol are rebuilt together. Each successful recovery starts a new CSV segment and units sidecar, so updated tick/lot steps cannot change the interpretation of earlier rows.

A depth snapshot is accepted only after a valid futures bridge (`U <= lastUpdateId <= u`) and contiguous subsequent updates (`pu == previous u`). Missing the bridge, failing to bridge within 10 seconds, exceeding the 8,192-event buffer, or detecting a sequence gap requests another refresh and reconnect. The timeout is checked even if no new messages arrive. Snapshot fetch/parse failures also restart this recovery sequence.

**Reconnect marker rows:** whenever a reconnect occurs (mismatch detected, EOF, idle timeout, or scheduled rotation), a row of all **zeros** is written. Use `local_time == 0` as the filter condition to identify these marker rows in downstream analysis. The marker closes the old segment before recovery opens a new file.

A row is emitted on every applied depth update, trade event, or bid/ask-price-changing BBO event (while recovery is not pending). The three timestamps refer only to the triggering message; they are never a maximum over streams. The depth10 stream only validates the book and does not emit rows. The state snapshot written is always the latest known value for each field — i.e. the most recent depth book and the most recent trade summary at the time of the triggering event.

---

### Requirements

**Rust collector:**
- Rust toolchain (`rustup` / `cargo`), edition 2021
- Builds with `cargo build --release`

**Python orchestrator:**
```
pyarrow
google-cloud-storage
```

Install with:
```bash
pip install pyarrow google-cloud-storage
```

GCS authentication: set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to your service account key path, or use Application Default Credentials on a GCP VM.

---

### How to Run

**Step 1 — Build the Rust binary:**
```bash
cargo build --release
```
The binary is placed at `target/release/binance_data_collector`.

**Step 2 — Configure `data_collection.py`:**

Open `data_collection.py` and edit the top-level constants to match your environment:

```python
RUST_BIN    = "~/binance_data_collector/target/release/binance_data_collector"
BUCKET_NAME = "binance_hft_data"   # your GCS bucket
GCS_PREFIX  = "binance_csv_parquet"
SYMBOLS     = ["BTC", "ETH", "SOL", "XRP"]
```

**Step 3 — Run the orchestrator:**
```bash
python data_collection.py
```

`data_collection.py` will:
1. Process any leftover CSV files from a previous run and upload them to GCS.
2. Launch one Rust collector process per symbol (staggered by 60 s to avoid burst connections).
3. Monitor disk usage every 5 s. When the disk exceeds 70 % usage, it stops the collectors, converts all CSV files to Parquet, uploads them to GCS, then restarts the collectors.
4. Automatically restart all collectors if any one exits unexpectedly.

To run as a persistent background service, consider wrapping with `nohup`, `screen`, `tmux`, or a `systemd` unit.

---

### Configuration

All tunable parameters are constants at the top of `data_collection.py`:

| Parameter | Default | Description |
|---|---|---|
| `RUST_BIN` | `~/binance_data_collector/target/release/binance_data_collector` | Path to compiled Rust binary |
| `SYMBOLS` | `["BTC","ETH","SOL","XRP"]` | Base symbols to collect |
| `BUCKET_NAME` | `"binance_hft_data"` | GCS bucket for uploads |
| `GCS_PREFIX` | `"binance_csv_parquet"` | GCS object prefix |
| `DISK_THRESHOLD` | `0.70` | Disk usage fraction that triggers flush |
| `CHECK_EVERY_SEC` | `5` | Main-loop polling interval (seconds) |
| `START_STAGGER_SEC` | `60` | Delay between starting each symbol collector |
| `RESTART_COOLDOWN_SEC` | `60` | Wait time before restarting after a flush |
| `DROP_TAIL_LINES_PRE_PARSE` | `200` | Tail lines dropped before Arrow parsing (guards against partial writes) |

---

### File Layout

```
binance_data_collector/
├── src/
│   ├── main.rs          # Core WebSocket collector (Rust)
│   ├── orderbook.rs     # Cache-friendly order book implementation
│   ├── trade.rs         # Individual trade message parser
│   ├── bbo.rs           # Effective quote freshness and bid/ask price filter
│   ├── units.rs         # Exact decimal tick/lot conversion
│   └── recovery.rs      # Snapshot bridge guards and recoverable parse errors
├── data_collection.py   # Python orchestrator (run this)
├── Cargo.toml
├── Cargo.lock
├── .gitignore
└── README.md
```

Runtime output (git-ignored):
```
logs/
├── <timestamp>_binance_<SYMBOL>.csv   # live tick data
├── runner_<SYMBOL>.out / .err         # stdout/stderr of each Rust process
├── data_collection.log                # orchestrator log
└── bad_csv/                           # quarantined corrupt CSV files
```

---

## 中文说明

### 架构概览

项目分为两层：

| 层级 | 文件 | 职责 |
|---|---|---|
| **核心采集器** | `src/main.rs`（Rust） | 为每个交易对维护 6 条 WebSocket 连接，在内存中维护订单簿，将 tick 级别数据写入 CSV |
| **调度器** | `data_collection.py`（Python） | 管理每个交易对对应的 Rust 进程，监控磁盘用量，将 CSV 转换为 Parquet 并上传至 GCS |

**每个交易对的 WebSocket 连接（共 6 条）：**

| 连接 | 端点 |
|---|---|
| `perp_usdt` — depth | `fstream.binance.com/public/stream?streams={sym}usdt@depth@0ms` |
| `perp_usdc` — depth | `fstream.binance.com/public/stream?streams={sym}usdc@depth@0ms` |
| `perp_usdt_d10` — depth10@100ms | `fstream.binance.com/public/stream?streams={sym}usdt@depth10@100ms` |
| `perp_usdc_d10` — depth10@100ms | `fstream.binance.com/public/stream?streams={sym}usdc@depth10@100ms` |
| `perp_usdt_trade` — trade + bookTicker | `fstream.binance.com/public/stream?streams={sym}usdt@trade/{sym}usdt@bookTicker` |
| `perp_usdc_trade` — trade + bookTicker | `fstream.binance.com/public/stream?streams={sym}usdc@trade/{sym}usdc@bookTicker` |

每次 depth 和 depth10@100ms 的更新序列号（`u`）匹配时都会进行比对。若发现不一致，则自动触发重连并重新拉取 depth 快照。

---

### 采集数据内容

针对每个交易对（例如 BTC → BTCUSDT 永续 + BTCUSDC 永续）：

- **订单簿前 10 档**（买卖价格及数量，1–10 档）— 每次 depth 事件后更新
- **逐笔成交** — 成交价、成交量、成交 ID（`t`）、买方为挂单方标志
- **时间戳** — `local_time`（socket 读取后立即记录的本地接收时间，同一次读取完成的消息共用该时间）、`binance_event_time`（当前消息的 `E`）和 `binance_transaction_time`（当前消息的 `T`），单位均为 Unix 毫秒。`event_type` 为 `usdc_trade`、`usdc_book`、`usdt_book`、`usdt_trade`、`usdc_bbo` 或 `usdt_bbo`

---

### 订单簿数据结构

内存订单簿采用**高速、缓存友好的结构，更新时不会引入分支预测失败**。

设计思路来源于以下演讲：

> **"When Nanoseconds Matter: Ultrafast Trading Systems in C++ – David Gross – CppCon 2024"**

核心特点：
- 固定大小数组，热路径上无堆内存分配
- 价格档位以排序方式存储，提取前 N 档仅需简单的切片复制
- 更新操作（插入、修改、删除）使用无分支索引运算实现
- 买卖盘分别存储于独立数组，每个数组均能放入少量 CPU 缓存行中

---

### CSV 格式与重连标记

每个 CSV 文件对应一个交易对的一次运行。表头格式如下：

```
local_time, binance_event_time, binance_transaction_time, event_type,
perp_usdt_bid_price_{1..10}, perp_usdt_ask_price_{1..10},
perp_usdt_bid_size_{1..10},  perp_usdt_ask_size_{1..10},
perp_usdc_bid_price_{1..10}, perp_usdc_ask_price_{1..10},
perp_usdc_bid_size_{1..10},  perp_usdc_ask_size_{1..10},
perp_usdt_price, perp_usdt_quantity, perp_usdt_trade_id, perp_usdt_is_buyer_maker,
perp_usdc_price, perp_usdc_quantity, perp_usdc_trade_id, perp_usdc_is_buyer_maker,
perp_usdt_best_bid_price, perp_usdt_best_ask_price, perp_usdt_best_bid_size, perp_usdt_best_ask_size, perp_usdt_best_update_id,
perp_usdc_best_bid_price, perp_usdc_best_ask_price, perp_usdc_best_bid_size, perp_usdc_best_ask_size, perp_usdc_best_update_id
```

所有价格和数量列均保存为整数 tick/lot 数量，使用各交易对 `PRICE_FILTER.tickSize` 和 `LOT_SIZE.stepSize` 进行精确十进制整数除法，不使用浮点或四舍五入。步长保存在 CSV 的 `.units.json` 附件中，并写入 Parquet 的 `binance_units` 元数据。`best_*` 列记录最新有效最优买卖价格和数量；仅当 bookTicker 买价或卖价与上一行对应交易对的最新有效价格不同，才写入 BBO 行。仅数量变化不会写行。使用更新 ID 防止过期 depth 覆盖更新的 BBO；`best_update_id` 保存有效报价的更新 ID。

转换、JSON 解析、快照或连接错误会触发恢复，不退出采集器。每次恢复先重新获取 USDT/USDC 交易对过滤信息，再重建所有 6 条连接和 REST 快照，并创建新的 CSV 及步长附件，避免同一文件混用不同步长。若无法桥接快照（`U <= lastUpdateId <= u`）、等待超过 10 秒、缓冲超过 8192 条或后续 `pu` 不连续，则再次恢复。

**重连标记行：** 每当发生重连（发现不一致、连接断开、空闲超时或定时轮换）时，会写入一行全为**零**的数据作为标记。在下游分析中，可通过 `local_time == 0` 来筛选这些标记行。

每次收到有效的 depth 更新或 trade 事件时写出一行。写出的快照始终是各字段的最新已知值——即触发事件时最新的订单簿和最新的成交摘要。

---

### 环境依赖

**Rust 采集器：**
- Rust 工具链（`rustup` / `cargo`），2021 edition
- 使用 `cargo build --release` 编译

**Python 调度器：**
```
pyarrow
google-cloud-storage
```

安装依赖：
```bash
pip install pyarrow google-cloud-storage
```

GCS 鉴权：设置环境变量 `GOOGLE_APPLICATION_CREDENTIALS` 指向服务账号密钥文件，或在 GCP 虚拟机上使用应用默认凭据（ADC）。

---

### 如何运行

**第一步 — 编译 Rust 二进制文件：**
```bash
cargo build --release
```
二进制文件生成于 `target/release/binance_data_collector`。

**第二步 — 配置 `data_collection.py`：**

打开 `data_collection.py`，根据实际环境修改顶部的常量：

```python
RUST_BIN    = "~/binance_data_collector/target/release/binance_data_collector"
BUCKET_NAME = "binance_hft_data"   # 你的 GCS bucket 名称
GCS_PREFIX  = "binance_csv_parquet"
SYMBOLS     = ["BTC", "ETH", "SOL", "XRP"]
```

**第三步 — 启动调度器：**
```bash
python data_collection.py
```

`data_collection.py` 将自动完成以下工作：
1. 处理上次运行遗留的 CSV 文件并上传至 GCS。
2. 为每个交易对启动一个 Rust 采集进程（间隔 60 秒，避免连接并发过高）。
3. 每隔 5 秒监控磁盘用量。当磁盘使用率超过 70% 时，停止采集器，将 CSV 转换为 Parquet 并上传至 GCS，然后重启采集器。
4. 若任意采集进程意外退出，自动重启所有采集器。

如需持续在后台运行，建议配合 `nohup`、`screen`、`tmux` 或 `systemd` 使用。

---

### 配置项

所有可调参数均为 `data_collection.py` 顶部的常量：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `RUST_BIN` | `~/binance_data_collector/target/release/binance_data_collector` | 编译后 Rust 二进制文件路径 |
| `SYMBOLS` | `["BTC","ETH","SOL","XRP"]` | 待采集的基础币种 |
| `BUCKET_NAME` | `"binance_hft_data"` | GCS bucket 名称 |
| `GCS_PREFIX` | `"binance_csv_parquet"` | GCS 对象前缀 |
| `DISK_THRESHOLD` | `0.70` | 触发刷盘的磁盘使用率阈值 |
| `CHECK_EVERY_SEC` | `5` | 主循环轮询间隔（秒） |
| `START_STAGGER_SEC` | `60` | 各交易对采集器启动间隔（秒） |
| `RESTART_COOLDOWN_SEC` | `60` | 刷盘后重启前的等待时间（秒） |
| `DROP_TAIL_LINES_PRE_PARSE` | `200` | Arrow 解析前丢弃的尾部行数（防止部分写入导致解析失败） |

---

### 文件结构

```
binance_data_collector/
├── src/
│   ├── main.rs          # 核心 WebSocket 采集器（Rust）
│   ├── orderbook.rs     # 缓存友好型订单簿实现
│   ├── trade.rs         # 逐笔成交消息解析
│   ├── bbo.rs           # 有效最优报价与买卖价格变化过滤
│   ├── units.rs         # 精确十进制 tick/lot 转换
│   └── recovery.rs      # 快照桥接保护与可恢复解析错误
├── data_collection.py   # Python 调度器（运行此文件）
├── Cargo.toml
├── Cargo.lock
├── .gitignore
└── README.md
```

运行时输出（已加入 .gitignore）：
```
logs/
├── <timestamp>_binance_<SYMBOL>.csv   # 实时 tick 数据
├── runner_<SYMBOL>.out / .err         # 各 Rust 进程的标准输出/错误
├── data_collection.log                # 调度器日志
└── bad_csv/                           # 隔离的损坏 CSV 文件
```

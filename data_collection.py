#!/usr/bin/env python3
import logging
import os
import signal
import shutil
import subprocess
import time
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from google.cloud import storage


# -------------------- Config --------------------

RUST_BIN = "~/binance_data_collector/target/release/binance_data_collector"
LOG_DIR = Path("logs")
BAD_DIR = LOG_DIR / "bad_csv"
BUCKET_NAME = "binance_hft_data"
GCS_PREFIX = "binance_csv_parquet"

SYMBOLS = ["BTC", "ETH", "SOL", "XRP"]

DISK_THRESHOLD = 0.70
CHECK_EVERY_SEC = 5
STOP_GRACE_SEC = 10

START_STAGGER_SEC = 60
RESTART_COOLDOWN_SEC = 60

PARQUET_COMPRESSION = "zstd"
PARQUET_SUFFIX = ".parquet"

# IMPORTANT:
# We must truncate tail lines BEFORE Arrow parses, otherwise Arrow throws first.
DROP_TAIL_LINES_PRE_PARSE = 200  # safer than 100; tune as needed
CSV_BLOCK_SIZE = 1 << 20         # 1 MiB


# -------------------- Logging --------------------

def setup_logging() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / "data_collection.log"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)

    # Avoid duplicate handlers if script is reloaded
    logger.handlers.clear()
    logger.addHandler(fh)
    logger.addHandler(sh)


# -------------------- Helpers --------------------

def disk_used_fraction(path: Path) -> float:
    usage = shutil.disk_usage(path)
    return usage.used / usage.total


def start_collectors(symbols: List[str], stagger_sec: int = 0) -> Dict[str, subprocess.Popen]:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    procs: Dict[str, subprocess.Popen] = {}
    for i, sym in enumerate(symbols):
        out_path = LOG_DIR / f"runner_{sym}.out"
        err_path = LOG_DIR / f"runner_{sym}.err"
        out_f = open(out_path, "ab", buffering=0)
        err_f = open(err_path, "ab", buffering=0)

        logging.info("Starting collector for %s", sym)
        p = subprocess.Popen(
            [RUST_BIN, sym],
            stdout=out_f,
            stderr=err_f,
            cwd=str(LOG_DIR.parent),
            env=os.environ.copy(),
        )
        procs[sym] = p

        if stagger_sec > 0 and i != len(symbols) - 1:
            time.sleep(stagger_sec)

    return procs


def stop_collectors(procs: Dict[str, subprocess.Popen]) -> None:
    logging.info("Stopping collectors (SIGTERM)...")
    for p in procs.values():
        if p.poll() is None:
            try:
                p.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass

    deadline = time.time() + STOP_GRACE_SEC
    for p in procs.values():
        while p.poll() is None and time.time() < deadline:
            time.sleep(0.1)

    still = [p for p in procs.values() if p.poll() is None]
    if still:
        logging.warning("Some collectors did not exit in time; sending SIGKILL...")
        for p in still:
            try:
                p.kill()
            except ProcessLookupError:
                pass


def list_csv_files() -> List[Path]:
    if not LOG_DIR.exists():
        return []
    return sorted(LOG_DIR.glob("*.csv"), key=lambda p: p.stat().st_mtime)


def expected_num_columns_from_header(csv_path: Path) -> int:
    with open(csv_path, "rb") as f:
        header = f.readline()
    # crude but correct given your CSV header contains no quotes/commas in names
    return header.count(b",") + 1


def build_column_types_from_header(csv_path: Path) -> Dict[str, pa.DataType]:
    with open(csv_path, "rb") as f:
        header = f.readline().decode("utf-8", errors="replace").strip()
    cols = header.split(",")

    types: Dict[str, pa.DataType] = {}
    for c in cols:
        if c in ("local_time", "binance_time"):
            types[c] = pa.int64()
            continue

        if (
            c.endswith("_trade_count")
            or c.endswith("_taker_buy_count")
            or c.endswith("_taker_sell_count")
            or c.endswith("_first_trade_id")
            or c.endswith("_last_trade_id")
            or c.endswith("_transact_time")
        ):
            types[c] = pa.int64()
            continue

        if c.endswith("_is_buyer_maker"):
            types[c] = pa.int8()
            continue

        types[c] = pa.float64()

    return types


def csv_reader(csv_path: Path) -> Any:
    column_types = build_column_types_from_header(csv_path)

    read_opts = pacsv.ReadOptions(
        use_threads=True,
        block_size=CSV_BLOCK_SIZE,
        autogenerate_column_names=False,
    )
    parse_opts = pacsv.ParseOptions(
        delimiter=",",
        quote_char='"',
        double_quote=True,
        escape_char=False,
        newlines_in_values=False,
    )
    convert_opts = pacsv.ConvertOptions(
        column_types=column_types,
        strings_can_be_null=True,
    )

    return pacsv.open_csv(
        csv_path,
        read_options=read_opts,
        parse_options=parse_opts,
        convert_options=convert_opts,
    )


# -------------------- Tail truncation BEFORE parsing --------------------

def truncate_to_last_newline_inplace(csv_path: Path) -> bool:
    """
    Ensure file ends exactly at a newline boundary.
    Returns True if modified, False otherwise.
    """
    with open(csv_path, "rb+") as f:
        f.seek(0, os.SEEK_END)
        end = f.tell()
        if end <= 0:
            return False

        f.seek(end - 1)
        last = f.read(1)
        if last == b"\n":
            return False  # already aligned

        # scan backwards for the last '\n'
        chunk = 4 * 1024 * 1024
        pos = end
        while pos > 0:
            r = min(chunk, pos)
            pos -= r
            f.seek(pos)
            data = f.read(r)
            i = data.rfind(b"\n")
            if i != -1:
                cut = pos + i + 1
                f.truncate(cut)
                return True

        # no newline at all -> truncate to empty (corrupt file)
        f.truncate(0)
        return True


def drop_last_n_lines_inplace(csv_path: Path, n_lines: int) -> Tuple[bool, int]:
    """
    Drop the last n_lines lines by truncating at the (n_lines+1)th newline from the end.
    Assumes file ends with newline (call truncate_to_last_newline_inplace first).
    Returns (modified, dropped_lines_estimate).
    """
    if n_lines <= 0:
        return (False, 0)

    with open(csv_path, "rb+") as f:
        f.seek(0, os.SEEK_END)
        end = f.tell()
        if end <= 0:
            return (False, 0)

        # locate header end (first newline) so we never truncate above it
        f.seek(0)
        header = f.readline()
        header_end = len(header)
        if header_end <= 0:
            return (False, 0)

        # if file only has header, nothing to do
        if end <= header_end:
            return (False, 0)

        target_newlines = n_lines + 1  # drop N lines => cut at (N+1)th newline from end
        chunk = 4 * 1024 * 1024
        pos = end
        found = 0

        while pos > header_end:
            r = min(chunk, pos - header_end)
            pos -= r
            f.seek(pos)
            data = f.read(r)

            # count newlines from the right
            i = len(data)
            while True:
                j = data.rfind(b"\n", 0, i)
                if j == -1:
                    break
                found += 1
                if found == target_newlines:
                    cut = pos + j + 1
                    if cut < header_end:
                        cut = header_end
                    f.truncate(cut)
                    return (True, n_lines)
                i = j

        # Not enough lines -> keep only header
        f.truncate(header_end)
        return (True, -1)


def sanitize_csv_tail_inplace(csv_path: Path, drop_tail_lines: int) -> None:
    """
    Makes CSV Arrow-parseable by:
      1) truncating incomplete last line (if any)
      2) dropping last drop_tail_lines (extra safety)
    """
    orig_size = csv_path.stat().st_size
    changed1 = truncate_to_last_newline_inplace(csv_path)
    changed2, dropped = drop_last_n_lines_inplace(csv_path, drop_tail_lines)
    new_size = csv_path.stat().st_size

    if changed1 or changed2:
        logging.info(
            "Sanitized tail: %s (size %d -> %d bytes, drop_tail_lines=%d, dropped=%s)",
            csv_path.name, orig_size, new_size, drop_tail_lines, str(dropped),
        )


# -------------------- Fallback repair for non-tail corruption --------------------

def repair_csv_by_column_count(
    src_path: Path,
    dst_path: Path,
    expected_cols: int,
    drop_tail_lines: int,
) -> int:
    """
    Stream-repair: keep only rows with correct column count, and drop last drop_tail_lines lines.
    Returns number of skipped bad rows.
    """
    skipped = 0
    tail: Deque[bytes] = deque()

    with open(src_path, "rb") as fin, open(dst_path, "wb") as fout:
        header = fin.readline()
        if not header:
            return 0
        fout.write(header)

        for line in fin:
            # keep a tail buffer so we can drop last N lines without reading file twice
            tail.append(line)
            if len(tail) <= drop_tail_lines:
                continue

            emit = tail.popleft()
            if emit.count(b",") + 1 != expected_cols:
                skipped += 1
                continue
            fout.write(emit)

    return skipped


# -------------------- CSV -> Parquet --------------------

def csv_to_parquet(csv_path: Path, parquet_path: Path) -> None:
    reader = csv_reader(csv_path)
    writer: Optional[pq.ParquetWriter] = None

    t0 = time.monotonic()
    rows = 0

    try:
        while True:
            try:
                batch = reader.read_next_batch()
            except StopIteration:
                break

            if writer is None:
                writer = pq.ParquetWriter(
                    parquet_path,
                    batch.schema,
                    compression=PARQUET_COMPRESSION,
                    use_dictionary=True,
                    write_statistics=True,
                )

            rows += batch.num_rows
            writer.write_table(pa.Table.from_batches([batch]))
    finally:
        if writer is not None:
            writer.close()

    logging.info(
        "Converted to parquet: %s -> %s (rows=%d, elapsed=%.2fs)",
        csv_path.name, parquet_path.name, rows, time.monotonic() - t0
    )


def upload_file_to_gcs(client: storage.Client, local_path: Path, bucket_name: str, blob_name: str) -> None:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(str(local_path))


def process_and_upload_all_csv(client: storage.Client) -> None:
    csv_files = list_csv_files()
    if not csv_files:
        logging.info("No CSV files to process.")
        return

    BAD_DIR.mkdir(parents=True, exist_ok=True)

    for csv_path in csv_files:
        parquet_path = csv_path.with_suffix(PARQUET_SUFFIX)
        logging.info("Processing CSV: %s (%.2f GiB)", csv_path.name, csv_path.stat().st_size / (1024**3))

        try:
            # 1) sanitize tail before Arrow parsing
            sanitize_csv_tail_inplace(csv_path, DROP_TAIL_LINES_PRE_PARSE)

            # 2) attempt Arrow conversion
            csv_to_parquet(csv_path, parquet_path)

        except Exception as e:
            logging.exception("Arrow conversion failed for %s: %s", csv_path.name, str(e))

            # Fallback: repair by filtering rows with wrong column count
            try:
                exp_cols = expected_num_columns_from_header(csv_path)
                repaired = csv_path.with_suffix(".repaired.csv")
                logging.info("Attempting repair: expected_cols=%d -> %s", exp_cols, repaired.name)

                skipped = repair_csv_by_column_count(
                    src_path=csv_path,
                    dst_path=repaired,
                    expected_cols=exp_cols,
                    drop_tail_lines=DROP_TAIL_LINES_PRE_PARSE,
                )
                logging.warning("Repair complete for %s: skipped_bad_rows=%d", csv_path.name, skipped)

                # convert repaired file
                csv_to_parquet(repaired, parquet_path)

                # remove repaired after successful conversion
                repaired.unlink(missing_ok=True)

            except Exception as e2:
                logging.exception("Repair also failed for %s: %s", csv_path.name, str(e2))

                # quarantine the CSV so pipeline continues
                ts = int(time.time())
                bad_path = BAD_DIR / f"{csv_path.stem}_{ts}.bad.csv"
                logging.error("Quarantining bad CSV: %s -> %s", csv_path.name, bad_path.name)
                try:
                    csv_path.rename(bad_path)
                except Exception:
                    # if rename fails, just leave it
                    pass
                # do not crash the whole pipeline
                continue

        # 3) upload parquet
        try:
            blob_name = f"{GCS_PREFIX}/{parquet_path.name}"
            logging.info("Uploading to GCS: %s -> gs://%s/%s", parquet_path.name, BUCKET_NAME, blob_name)
            upload_file_to_gcs(client, parquet_path, BUCKET_NAME, blob_name)
        except Exception as e:
            logging.exception("Upload failed for %s: %s", parquet_path.name, str(e))
            # do not delete local files if upload failed
            continue

        # 4) cleanup
        csv_path.unlink(missing_ok=True)
        parquet_path.unlink(missing_ok=True)
        logging.info("Done: %s", csv_path.name)


def any_collector_exited(procs: Dict[str, subprocess.Popen]) -> bool:
    return any(p.poll() is not None for p in procs.values())


# -------------------- Main loop --------------------

def main():
    setup_logging()

    if not Path(RUST_BIN).exists():
        raise RuntimeError(f"RUST_BIN not found: {RUST_BIN}")

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    client = storage.Client()

    logging.info("Startup: processing any leftover CSV files first.")
    process_and_upload_all_csv(client)

    logging.info("Starting collectors with stagger=%ds", START_STAGGER_SEC)
    procs = start_collectors(SYMBOLS, stagger_sec=START_STAGGER_SEC)

    while True:
        used = disk_used_fraction(LOG_DIR)
        logging.info("Disk used: %.1f%% (threshold=%.1f%%)", used * 100.0, DISK_THRESHOLD * 100.0)

        if used >= DISK_THRESHOLD:
            logging.warning("Disk threshold hit. Stopping collectors and flushing pipeline.")
            stop_collectors(procs)
            process_and_upload_all_csv(client)

            logging.info("Cooldown %ds before restart.", RESTART_COOLDOWN_SEC)
            time.sleep(RESTART_COOLDOWN_SEC)

            logging.info("Restarting collectors with stagger=%ds", START_STAGGER_SEC)
            procs = start_collectors(SYMBOLS, stagger_sec=START_STAGGER_SEC)

        if any_collector_exited(procs):
            logging.warning("A collector exited unexpectedly. Restarting all collectors.")
            stop_collectors(procs)
            time.sleep(RESTART_COOLDOWN_SEC)
            procs = start_collectors(SYMBOLS, stagger_sec=START_STAGGER_SEC)

        time.sleep(CHECK_EVERY_SEC)


if __name__ == "__main__":
    main()

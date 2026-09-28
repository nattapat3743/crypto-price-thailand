"""
bk_01_ingestion.py
================
Stage 1: Ingestion — ดึงแท่งเทียน 1 นาทีของเหรียญยอดนิยมบน Bitkub ลง Data Lake ชั้น Bronze

การทำงาน (รันทุกชั่วโมง นาทีที่ :05):
1. refresh_pairs  : ดึงสถิติ 24 ชม. ของทุกคู่ เลือก TOP_N คู่ THB ที่ volume สูงสุด
                    (ไม่นับ stablecoin อย่าง USDT/USDC เพราะราคาคงที่ ไม่มีการปั่น)
                    + เก็บภาพรวมตลาดลง bronze/ticker และตาราง market_ticker
2. ingest_candles : ดึงแท่งเทียนต่อจากจุดล่าสุดของแต่ละเหรียญ (จำไว้ในตาราง ingest_state)
                    - รันครั้งแรก = backfill ย้อนหลัง BACKFILL_DAYS วัน (ค่าเริ่มต้น 365 วัน, ~10 นาที)
                    - รันครั้งถัดไป = ดึงแค่ชั่วโมงล่าสุด (~1 นาที)
                    ดึงซ้อนแท่งสุดท้ายไว้ 1 แท่งเสมอ เพราะแท่งของนาทีปัจจุบันอาจยังไม่ปิด
                    (silver จะ dedupe โดยเก็บแถวที่ดึงมาล่าสุด)

ข้อมูลดิบไม่ผ่าน XCom — เขียนเป็น Parquet ลง lake โดยตรง
"""

import time
from datetime import datetime, timedelta, timezone

import pyarrow as pa
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from bitkub_lake import (
    BACKFILL_DAYS,
    BRONZE_CANDLES,
    BRONZE_TICKER,
    CHUNK_DAYS,
    REQUEST_PAUSE_S,
    TOP_N,
    duckdb_conn,
    fetch_candles,
    fetch_symbols,
    fetch_ticker,
)

POSTGRES_CONN_ID = "postgres_target"
STABLECOINS = {"USDT", "USDC", "DAI", "BUSD", "TUSD", "FDUSD", "PYUSD", "RLUSD", "USD1", "USDP", "USDE"}

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS pairs (
    symbol TEXT PRIMARY KEY,
    name TEXT,
    listed_at TIMESTAMP,              -- วันที่เปิดเทรดบน Bitkub (ใช้บอกว่าเป็นเหรียญใหม่)
    volume_thb_24h FLOAT,
    rank_24h INT,
    selected BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'UTC')
);
CREATE TABLE IF NOT EXISTS ingest_state (
    symbol TEXT PRIMARY KEY,
    last_ts BIGINT NOT NULL,          -- unix time ของแท่งล่าสุดที่ดึงมาแล้ว
    rows_total BIGINT NOT NULL DEFAULT 0,
    files INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'UTC')
);
CREATE TABLE IF NOT EXISTS ingest_log (
    id SERIAL PRIMARY KEY,
    run_at TIMESTAMP NOT NULL,
    symbol TEXT NOT NULL,
    from_ts TIMESTAMP, to_ts TIMESTAMP,
    api_calls INT, rows INT, duration_ms INT,
    object_key TEXT,
    is_backfill BOOLEAN
);
CREATE INDEX IF NOT EXISTS ix_ingest_log_run ON ingest_log (run_at);
CREATE TABLE IF NOT EXISTS market_ticker (
    symbol TEXT PRIMARY KEY,
    last FLOAT, percent_change FLOAT, high_24h FLOAT, low_24h FLOAT,
    volume_thb_24h FLOAT, snapshot_at TIMESTAMP
);
"""


def refresh_pairs(**kwargs):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    symbols = {s["symbol"]: s for s in fetch_symbols()}
    ticker = fetch_ticker()

    # ภาพรวมตลาดเก็บลง bronze ด้วย (ประวัติ % เปลี่ยน 24 ชม. ทุกชั่วโมง)
    rows = []
    for t in ticker:
        rows.append((t["symbol"], float(t.get("last") or 0), float(t.get("percent_change") or 0),
                     float(t.get("high_24_hr") or 0), float(t.get("low_24_hr") or 0),
                     float(t.get("quote_volume") or 0), now))
    cols = list(zip(*rows)) if rows else [[]] * 7
    table = pa.table({
        "symbol": pa.array(cols[0], pa.string()), "last": pa.array(cols[1], pa.float64()),
        "percent_change": pa.array(cols[2], pa.float64()), "high_24h": pa.array(cols[3], pa.float64()),
        "low_24h": pa.array(cols[4], pa.float64()), "volume_thb_24h": pa.array(cols[5], pa.float64()),
        "snapshot_at": pa.array(cols[6], pa.timestamp("us")),
    })
    con = duckdb_conn()
    con.register("ticker_tbl", table)
    con.execute(f"COPY ticker_tbl TO '{BRONZE_TICKER}/dt={now:%Y-%m-%d}/{now:%H%M%S}.parquet' (FORMAT PARQUET);")
    con.close()

    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE market_ticker;")
        cur.executemany(
            "INSERT INTO market_ticker (symbol, last, percent_change, high_24h, low_24h, volume_thb_24h, snapshot_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)", rows,
        )

        # เลือก TOP_N คู่ THB ที่ไม่ใช่ stablecoin เรียงตาม volume 24 ชม.
        candidates = [
            r for r in rows
            if r[0].endswith("_THB") and r[0].split("_")[0] not in STABLECOINS and r[5] > 0
        ]
        candidates.sort(key=lambda r: r[5], reverse=True)
        top = {r[0]: i + 1 for i, r in enumerate(candidates[:TOP_N])}

        cur.execute("UPDATE pairs SET selected = FALSE, rank_24h = NULL;")
        for r in candidates:
            meta = symbols.get(r[0], {})
            listed = meta.get("created_at")
            cur.execute(
                """INSERT INTO pairs (symbol, name, listed_at, volume_thb_24h, rank_24h, selected, updated_at)
                   VALUES (%s, %s, %s::timestamptz AT TIME ZONE 'UTC', %s, %s, %s, %s)
                   ON CONFLICT (symbol) DO UPDATE SET name = EXCLUDED.name, listed_at = EXCLUDED.listed_at,
                       volume_thb_24h = EXCLUDED.volume_thb_24h, rank_24h = EXCLUDED.rank_24h,
                       selected = EXCLUDED.selected, updated_at = EXCLUDED.updated_at""",
                (r[0], meta.get("name"), listed, r[5], top.get(r[0]), r[0] in top, now),
            )
        # เหรียญที่เคยเก็บไว้แล้ว ให้เก็บต่อแม้หลุดจาก top (ข้อมูลจะได้ต่อเนื่อง)
        cur.execute("UPDATE pairs SET selected = TRUE WHERE symbol IN (SELECT symbol FROM ingest_state);")
    conn.commit()
    conn.close()
    print(f"เลือก {len(top)} คู่จากทั้งหมด {len(ticker)} คู่: {', '.join(list(top)[:10])} ...")


def ingest_candles(**kwargs):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    run_at = datetime.now(timezone.utc).replace(tzinfo=None)
    now_ts = int(time.time())
    selected = [r[0] for r in hook.get_records("SELECT symbol FROM pairs WHERE selected ORDER BY rank_24h NULLS LAST, symbol")]
    state = dict(hook.get_records("SELECT symbol, last_ts FROM ingest_state"))

    con = duckdb_conn()
    total_rows = 0
    for i, symbol in enumerate(selected, 1):
        started = time.time()
        is_backfill = symbol not in state
        start_ts = state.get(symbol, now_ts - BACKFILL_DAYS * 86400)
        rows, calls, cursor = [], 0, start_ts
        while cursor < now_ts:
            end = min(cursor + CHUNK_DAYS * 86400, now_ts)
            rows.extend(fetch_candles(symbol, cursor, end))
            calls += 1
            cursor = end
            time.sleep(REQUEST_PAUSE_S)
        if not rows:
            print(f"[{i}/{len(selected)}] {symbol}: ไม่มีการซื้อขายใหม่")
            continue

        t, o, h, l, c, v = zip(*rows)
        table = pa.table({
            "ts": pa.array([datetime.fromtimestamp(x, timezone.utc).replace(tzinfo=None) for x in t], pa.timestamp("s")),
            "open": pa.array(o, pa.float64()), "high": pa.array(h, pa.float64()),
            "low": pa.array(l, pa.float64()), "close": pa.array(c, pa.float64()),
            "volume": pa.array(v, pa.float64()),
            "ingested_at": pa.array([run_at] * len(rows), pa.timestamp("us")),
        })
        key = f"{BRONZE_CANDLES}/symbol={symbol}/{min(t)}_{max(t)}_{run_at:%Y%m%dT%H%M%S}.parquet"
        con.register("candles_tbl", table)
        con.execute(f"COPY candles_tbl TO '{key}' (FORMAT PARQUET, COMPRESSION ZSTD);")
        con.unregister("candles_tbl")

        duration_ms = int((time.time() - started) * 1000)
        hook.run(
            """INSERT INTO ingest_state (symbol, last_ts, rows_total, files, updated_at)
               VALUES (%s, %s, %s, 1, %s)
               ON CONFLICT (symbol) DO UPDATE SET last_ts = EXCLUDED.last_ts,
                   rows_total = ingest_state.rows_total + EXCLUDED.rows_total,
                   files = ingest_state.files + 1, updated_at = EXCLUDED.updated_at""",
            parameters=(symbol, max(t), len(rows), run_at),
        )
        hook.run(
            """INSERT INTO ingest_log (run_at, symbol, from_ts, to_ts, api_calls, rows, duration_ms, object_key, is_backfill)
               VALUES (%s, %s, to_timestamp(%s) AT TIME ZONE 'UTC', to_timestamp(%s) AT TIME ZONE 'UTC', %s, %s, %s, %s, %s)""",
            parameters=(run_at, symbol, min(t), max(t), calls, len(rows), duration_ms, key, is_backfill),
        )
        total_rows += len(rows)
        print(f"[{i}/{len(selected)}] {symbol}: {len(rows):,} แท่ง ({calls} calls, {duration_ms/1000:.1f}s)"
              f"{' — backfill' if is_backfill else ''}")
    con.close()
    print(f"รวม {total_rows:,} แท่งใหม่จาก {len(selected)} เหรียญ")


with DAG(
    dag_id="bitkub_ingestion_dag",
    description="Stage 1: ดึงแท่งเทียน 1 นาทีจาก Bitkub (backfill ครั้งแรก + รายชั่วโมง) -> Bronze",
    default_args={"owner": "bigdata", "retries": 2, "retry_delay": timedelta(minutes=3)},
    schedule="5 * * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bitkub", "stage-1", "ingestion"],
) as dag:
    create_tables = PostgresOperator(task_id="create_tables", postgres_conn_id=POSTGRES_CONN_ID, sql=CREATE_TABLES_SQL)
    pairs_task = PythonOperator(task_id="refresh_pairs", python_callable=refresh_pairs)
    candles_task = PythonOperator(task_id="ingest_candles", python_callable=ingest_candles,
                                  execution_timeout=timedelta(minutes=90))
    create_tables >> pairs_task >> candles_task

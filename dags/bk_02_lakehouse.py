"""
bk_02_lakehouse.py
================
Stage 2: Processing — แปลงแท่งเทียนดิบเป็นข้อมูลพร้อมวิเคราะห์ด้วย DuckDB แล้วตรวจจับ pump & dump

    Bronze (แท่งเทียนดิบ, มีแถวซ้ำจากการดึงซ้อน)
       │  build_silver (ทีละเหรียญ เพื่อคุม RAM):
       │    - dedupe (ts ซ้ำ เก็บแถวที่ดึงมาล่าสุด)
       │    - feature รายนาที: return, % เปลี่ยนใน 5 นาที, volume 5 นาทีเทียบค่าปกติ 7 วัน,
       │      สภาพคล่อง (สัดส่วนนาทีที่มีการซื้อขาย), ราคาสูง/ต่ำสุดใน 60 นาทีถัดไป
       ▼
    Silver (silver/features/symbol=XXX/data.parquet)
       │  build_gold:
       │    - gold_events         : เหตุการณ์ต้องสงสัย (ราคา +5% ใน 5 นาที และ volume >= 5 เท่า)
       │                            พร้อมจุดสูงสุด, % ที่ร่วงกลับ, เวลาที่ใช้ร่วงกลับครึ่งหนึ่ง
       │    - gold_event_candles  : แท่งเทียนรอบแต่ละเหตุการณ์ (ใช้วาดกราฟบน Dashboard)
       │    - gold_symbol_hourly  : สรุปรายชั่วโมงต่อเหรียญ (volume, ความผันผวน) -> ใช้พยากรณ์
       │    - gold_market_hourly  : สรุปรายชั่วโมงทั้งตลาด
       │    - gold_coin_stats     : สถิติรายเหรียญ (สภาพคล่อง, volume เฉลี่ย, ช่วงข้อมูล)
       ▼
    Gold (Parquet ใน lake + โหลดเข้า Postgres ให้ Dashboard)

เกณฑ์ใน gold ตั้งไว้ต่ำ (5% / 5 เท่า) เพื่อเก็บ "ผู้ต้องสงสัย" ให้ครบ
Dashboard กรองด้วยเกณฑ์จริงอีกชั้น (ค่าเริ่มต้น 10% / 10 เท่า ปรับได้บนหน้าเว็บ)
ทุก task เป็น idempotent (คำนวณใหม่ทั้งหมดทุกรอบ)
"""

import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from bitkub_lake import (
    BANGKOK_OFFSET_H,
    BRONZE_CANDLES,
    CANDIDATE_PUMP_PCT,
    CANDIDATE_VOL_X,
    DUMP_WINDOW_MIN,
    EVENT_GAP_MIN,
    GOLD,
    GOLD_DATASET_URI,
    SILVER_FEATURES,
    duckdb_conn,
    lake_glob_exists,
    pg_replace_table,
)

POSTGRES_CONN_ID = "postgres_target"

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS dq_symbol (
    symbol TEXT PRIMARY KEY,
    bronze_files INT, bronze_rows BIGINT, duplicates BIGINT, candles BIGINT,
    first_ts TIMESTAMP, last_ts TIMESTAMP,
    active_share FLOAT,                   -- สัดส่วนนาทีที่มีการซื้อขาย
    processed_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS gold_events (
    event_id TEXT PRIMARY KEY, symbol TEXT,
    start_ts TIMESTAMP, peak_ts TIMESTAMP, last_signal_ts TIMESTAMP,
    base_price FLOAT, peak_price FLOAT, pump_pct FLOAT,
    max_vol_x FLOAT, max_chg_5m_pct FLOAT, signal_minutes INT,
    min_low_after FLOAT, dump_pct FLOAT, retrace_share FLOAT,
    minutes_to_peak FLOAT, minutes_to_half_retrace FLOAT, is_pump_dump BOOLEAN,
    active_share_1d FLOAT, vol_thb_event FLOAT,
    hour_local INT, dow_local INT,
    concurrent_symbols INT,               -- เหรียญอื่นที่พุ่งพร้อมกันใน ±10 นาที (มาก = ข่าว/ทั้งตลาด ไม่ใช่ปั่นเหรียญเดียว)
    anomaly_score FLOAT                   -- เติมโดย bitkub_ml_pipeline_dag (Isolation Forest)
);
CREATE INDEX IF NOT EXISTS ix_gold_events_start ON gold_events (start_ts);
CREATE TABLE IF NOT EXISTS gold_event_candles (
    event_id TEXT, ts TIMESTAMP, open FLOAT, high FLOAT, low FLOAT, close FLOAT, vol_thb FLOAT
);
CREATE INDEX IF NOT EXISTS ix_gold_event_candles ON gold_event_candles (event_id);
CREATE TABLE IF NOT EXISTS gold_symbol_hourly (
    symbol TEXT, hour_utc TIMESTAMP, candles INT, vol_thb FLOAT,
    open FLOAT, high FLOAT, low FLOAT, close FLOAT,
    rv_pct FLOAT, range_pct FLOAT,
    PRIMARY KEY (symbol, hour_utc)
);
CREATE TABLE IF NOT EXISTS gold_market_hourly (
    hour_utc TIMESTAMP PRIMARY KEY, symbols INT, candles INT, vol_thb FLOAT, avg_rv_pct FLOAT
);
CREATE TABLE IF NOT EXISTS gold_coin_stats (
    symbol TEXT PRIMARY KEY, first_ts TIMESTAMP, last_ts TIMESTAMP, candles BIGINT,
    active_share FLOAT, avg_daily_vol_thb_30d FLOAT, last_close FLOAT
);
CREATE TABLE IF NOT EXISTS gold_refresh_log (
    id SERIAL PRIMARY KEY, refreshed_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'UTC'),
    silver_rows BIGINT, events INT, duration_s FLOAT
);
"""

SILVER_SQL = """
CREATE OR REPLACE TEMP TABLE f AS
WITH dedup AS (
    SELECT ts, open, high, low, close, volume, volume * close AS vol_thb
    FROM read_parquet('{glob}', hive_partitioning = false)
    QUALIFY row_number() OVER (PARTITION BY ts ORDER BY ingested_at DESC) = 1
)
SELECT ts, open, high, low, close, volume, vol_thb,
       close / nullif(lag(close) OVER w, 0) - 1                   AS ret_1m,      -- เทียบแท่งก่อนหน้าที่มีการซื้อขาย
       date_diff('minute', lag(ts) OVER w, ts)                     AS gap_min,
       first_value(open) OVER w5                                   AS open_5m,     -- ราคาเปิดเมื่อ 5 นาทีก่อน
       close / nullif(first_value(open) OVER w5, 0) - 1            AS chg_5m,
       sum(vol_thb) OVER w5                                        AS vol5_thb,
       sum(vol_thb) OVER w7d / 10080.0                             AS base_vpm_thb, -- volume ปกติต่อนาที (7 วันก่อนหน้า, นาทีว่างนับเป็น 0)
       count(*) OVER w1d / 1440.0                                  AS active_share_1d,
       (high - low) / nullif(low, 0)                               AS range_1m,
       max(high) OVER wf AS max_high_next60,
       min(low)  OVER wf AS min_low_next60,
       last_value(close) OVER wf15 AS close_next15,                            -- ใช้ในการทดลองทายทิศทางราคา
       min(ts) OVER () AS first_ts
FROM dedup
WINDOW w   AS (ORDER BY ts),
       w5  AS (ORDER BY ts RANGE BETWEEN INTERVAL 4 MINUTE PRECEDING AND CURRENT ROW),
       w7d AS (ORDER BY ts RANGE BETWEEN INTERVAL 7 DAY PRECEDING AND INTERVAL 5 MINUTE PRECEDING),
       w1d AS (ORDER BY ts RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW),
       wf  AS (ORDER BY ts RANGE BETWEEN INTERVAL 1 MINUTE FOLLOWING AND INTERVAL 60 MINUTE FOLLOWING),
       wf15 AS (ORDER BY ts RANGE BETWEEN INTERVAL 14 MINUTE FOLLOWING AND INTERVAL 15 MINUTE FOLLOWING);
"""


def build_silver(**kwargs):
    """ประมวลผลทีละเหรียญ (คุม RAM ให้พอกับ Docker Desktop) แล้วเขียน silver + data quality"""
    con = duckdb_conn()
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    symbols = [r[0] for r in hook.get_records("SELECT symbol FROM ingest_state ORDER BY symbol")]
    total = 0
    for symbol in symbols:
        glob = f"{BRONZE_CANDLES}/symbol={symbol}/*.parquet"
        if not lake_glob_exists(con, glob):
            continue
        files = con.execute("SELECT count(*) FROM glob(?)", [glob]).fetchone()[0]
        bronze_rows = con.execute(f"SELECT count(*) FROM read_parquet('{glob}', hive_partitioning = false)").fetchone()[0]
        con.execute(SILVER_SQL.format(glob=glob))
        con.execute(f"""
            COPY (
                SELECT '{symbol}' AS symbol, * EXCLUDE (first_ts),
                       -- volume เทียบค่าปกติ: ใช้ได้เมื่อมีประวัติย้อนหลังอย่างน้อย 1 วัน
                       CASE WHEN ts >= first_ts + INTERVAL 1 DAY
                            THEN vol5_thb / nullif(base_vpm_thb * 5, 0) END AS vol_x_5m
                FROM f ORDER BY ts
            ) TO '{SILVER_FEATURES}/symbol={symbol}/data.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)
        candles, first_ts, last_ts = con.execute("SELECT count(*), min(ts), max(ts) FROM f").fetchone()
        span_min = max((last_ts - first_ts).total_seconds() / 60 + 1, 1)
        hook.run(
            """INSERT INTO dq_symbol (symbol, bronze_files, bronze_rows, duplicates, candles, first_ts, last_ts,
                                      active_share, processed_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW() AT TIME ZONE 'UTC')
               ON CONFLICT (symbol) DO UPDATE SET bronze_files = EXCLUDED.bronze_files,
                   bronze_rows = EXCLUDED.bronze_rows, duplicates = EXCLUDED.duplicates,
                   candles = EXCLUDED.candles, first_ts = EXCLUDED.first_ts, last_ts = EXCLUDED.last_ts,
                   active_share = EXCLUDED.active_share, processed_at = EXCLUDED.processed_at""",
            parameters=(symbol, files, bronze_rows, bronze_rows - candles, candles, first_ts, last_ts,
                        candles / span_min),
        )
        total += candles
        print(f"{symbol}: bronze {bronze_rows:,} แถว ({files} ไฟล์) -> silver {candles:,} แท่ง "
              f"(ซ้ำ {bronze_rows - candles:,}, มีการซื้อขาย {candles / span_min:.0%} ของนาที)")
    con.close()
    print(f"silver รวม {total:,} แถว จาก {len(symbols)} เหรียญ")


def build_gold(**kwargs):
    started = time.time()
    con = duckdb_conn()
    silver_glob = f"{SILVER_FEATURES}/*/data.parquet"
    if not lake_glob_exists(con, silver_glob):
        print("ยังไม่มี silver — รอ ingestion/build_silver ก่อน")
        return
    con.execute(f"CREATE OR REPLACE VIEW silver AS SELECT * FROM read_parquet('{silver_glob}', hive_partitioning = false);")
    silver_rows = con.execute("SELECT count(*) FROM silver").fetchone()[0]

    # ---------- 1) หาเหตุการณ์ต้องสงสัย ----------
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE cand AS
        SELECT symbol, ts, open_5m, chg_5m, vol_x_5m, active_share_1d, vol5_thb
        FROM silver
        WHERE chg_5m * 100 >= {CANDIDATE_PUMP_PCT} AND vol_x_5m >= {CANDIDATE_VOL_X};
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ev AS
        WITH g AS (
            SELECT *, CASE WHEN ts - lag(ts) OVER (PARTITION BY symbol ORDER BY ts) <= INTERVAL {EVENT_GAP_MIN} MINUTE
                           THEN 0 ELSE 1 END AS is_new
            FROM cand
        ), g2 AS (
            SELECT *, sum(is_new) OVER (PARTITION BY symbol ORDER BY ts ROWS UNBOUNDED PRECEDING) AS eid FROM g
        )
        SELECT symbol, eid, min(ts) AS start_ts, max(ts) AS last_signal_ts,
               arg_min(open_5m, ts) AS base_price,                 -- ราคาก่อนเริ่มพุ่ง
               max(vol_x_5m) AS max_vol_x, max(chg_5m) * 100 AS max_chg_5m_pct,
               count(*) AS signal_minutes, arg_min(active_share_1d, ts) AS active_share_1d
        FROM g2 GROUP BY symbol, eid;
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE ev_peak AS
        SELECT e.symbol, e.eid, max(s.high) AS peak_price, arg_max(s.ts, s.high) AS peak_ts,
               sum(s.vol_thb) AS vol_thb_event
        FROM ev e JOIN silver s
          ON s.symbol = e.symbol
         AND s.ts BETWEEN e.start_ts - INTERVAL 5 MINUTE AND e.last_signal_ts + INTERVAL 30 MINUTE
        GROUP BY e.symbol, e.eid;
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ev_after AS
        SELECT p.symbol, p.eid, min(s.low) AS min_low_after,
               min(s.ts) FILTER (WHERE s.close <= e.base_price + 0.5 * (p.peak_price - e.base_price)) AS half_retrace_ts
        FROM ev_peak p
        JOIN ev e USING (symbol, eid)
        LEFT JOIN silver s
          ON s.symbol = p.symbol AND s.ts > p.peak_ts AND s.ts <= p.peak_ts + INTERVAL {DUMP_WINDOW_MIN} MINUTE
        GROUP BY p.symbol, p.eid;
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE g_events AS
        SELECT e.symbol || '-' || strftime(e.start_ts, '%Y%m%d%H%M') AS event_id,
               e.symbol, e.start_ts, p.peak_ts, e.last_signal_ts,
               e.base_price, p.peak_price, (p.peak_price / e.base_price - 1) * 100 AS pump_pct,
               e.max_vol_x, e.max_chg_5m_pct, e.signal_minutes,
               a.min_low_after,
               (a.min_low_after / p.peak_price - 1) * 100 AS dump_pct,
               (p.peak_price - a.min_low_after) / nullif(p.peak_price - e.base_price, 0) AS retrace_share,
               date_diff('second', e.start_ts, p.peak_ts) / 60.0 AS minutes_to_peak,
               date_diff('second', p.peak_ts, a.half_retrace_ts) / 60.0 AS minutes_to_half_retrace,
               a.half_retrace_ts IS NOT NULL AS is_pump_dump,
               e.active_share_1d, p.vol_thb_event,
               CAST(extract(hour FROM e.start_ts + INTERVAL {BANGKOK_OFFSET_H} HOUR) AS INT) AS hour_local,
               CAST(isodow(e.start_ts + INTERVAL {BANGKOK_OFFSET_H} HOUR) AS INT) AS dow_local,
               NULL::DOUBLE AS anomaly_score
        FROM ev e JOIN ev_peak p USING (symbol, eid) JOIN ev_after a USING (symbol, eid)
        WHERE e.base_price > 0;
    """)
    # เหตุการณ์ที่เกิดพร้อมกันหลายเหรียญ มักเป็นการเคลื่อนไหวของทั้งตลาด (ข่าว, BTC วิ่ง) ไม่ใช่การปั่นเหรียญเดียว
    con.execute("""
        CREATE OR REPLACE TEMP TABLE g_events AS
        SELECT e.* EXCLUDE (anomaly_score),
               (SELECT count(DISTINCT o.symbol) FROM g_events o
                 WHERE o.symbol <> e.symbol
                   AND o.start_ts BETWEEN e.start_ts - INTERVAL 10 MINUTE AND e.start_ts + INTERVAL 10 MINUTE
               )::INT AS concurrent_symbols,
               e.anomaly_score
        FROM g_events e;
    """)
    n_events = con.execute("SELECT count(*) FROM g_events").fetchone()[0]

    # ---------- 2) แท่งเทียนรอบเหตุการณ์ (60 นาทีก่อน ถึง 90 นาทีหลังจุดสูงสุด) ----------
    con.execute("""
        CREATE OR REPLACE TEMP TABLE g_event_candles AS
        SELECT e.event_id, s.ts, s.open, s.high, s.low, s.close, s.vol_thb
        FROM g_events e JOIN silver s
          ON s.symbol = e.symbol AND s.ts BETWEEN e.start_ts - INTERVAL 60 MINUTE AND e.peak_ts + INTERVAL 90 MINUTE;
    """)

    # ---------- 3) สรุปรายชั่วโมง (ใช้พยากรณ์ volume / ความผันผวน) ----------
    con.execute("""
        CREATE OR REPLACE TEMP TABLE g_symbol_hourly AS
        SELECT symbol, date_trunc('hour', ts) AS hour_utc, count(*) AS candles, sum(vol_thb) AS vol_thb,
               arg_min(open, ts) AS open, max(high) AS high, min(low) AS low, arg_max(close, ts) AS close,
               sqrt(sum(coalesce(ret_1m, 0) ^ 2)) * 100 AS rv_pct,        -- realized volatility ของชั่วโมง
               (max(high) / nullif(min(low), 0) - 1) * 100 AS range_pct
        FROM silver GROUP BY ALL;
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE g_market_hourly AS
        SELECT hour_utc, count(*) AS symbols, sum(candles) AS candles, sum(vol_thb) AS vol_thb, avg(rv_pct) AS avg_rv_pct
        FROM g_symbol_hourly GROUP BY hour_utc;
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE g_coin_stats AS
        SELECT symbol, min(ts) AS first_ts, max(ts) AS last_ts, count(*) AS candles,
               count(*) / (date_diff('minute', min(ts), max(ts)) + 1.0) AS active_share,
               sum(vol_thb) FILTER (WHERE ts >= (SELECT max(ts) FROM silver) - INTERVAL 30 DAY) / 30.0 AS avg_daily_vol_thb_30d,
               arg_max(close, ts) AS last_close
        FROM silver GROUP BY symbol;
    """)

    tables = {
        "g_events": ("events", "gold_events"),
        "g_event_candles": ("event_candles", "gold_event_candles"),
        "g_symbol_hourly": ("symbol_hourly", "gold_symbol_hourly"),
        "g_market_hourly": ("market_hourly", "gold_market_hourly"),
        "g_coin_stats": ("coin_stats", "gold_coin_stats"),
    }
    counts = {}
    conn = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()
    try:
        # เก็บ anomaly_score เดิมไว้ (ML DAG เติมให้) ไม่ให้หายตอน full refresh
        with conn.cursor() as cur:
            cur.execute("SELECT event_id, anomaly_score FROM gold_events WHERE anomaly_score IS NOT NULL")
            old_scores = cur.fetchall()
        for temp, (lake_name, pg_table) in tables.items():
            con.execute(f"COPY {temp} TO '{GOLD}/{lake_name}/snapshot.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);")
            counts[lake_name] = pg_replace_table(conn, pg_table, con.table(temp))
        with conn.cursor() as cur:
            cur.executemany("UPDATE gold_events SET anomaly_score = %s WHERE event_id = %s",
                            [(s, e) for e, s in old_scores])
            duration = time.time() - started
            cur.execute("INSERT INTO gold_refresh_log (silver_rows, events, duration_s) VALUES (%s, %s, %s)",
                        (int(silver_rows), int(n_events), duration))
        conn.commit()
    finally:
        conn.close()
        con.close()
    print(f"Gold เสร็จใน {duration:.1f} วินาที | silver {silver_rows:,} แถว | "
          + " | ".join(f"{k} {v:,}" for k, v in counts.items()))


with DAG(
    dag_id="bitkub_lakehouse_dag",
    description="Stage 2: Bronze -> Silver (feature) -> Gold (pump events, hourly) ด้วย DuckDB",
    default_args={"owner": "bigdata", "retries": 1, "retry_delay": timedelta(minutes=3)},
    schedule="25 * * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bitkub", "stage-2", "duckdb", "lakehouse"],
) as dag:
    create_tables = PostgresOperator(task_id="create_tables", postgres_conn_id=POSTGRES_CONN_ID, sql=CREATE_TABLES_SQL)
    silver = PythonOperator(task_id="build_silver", python_callable=build_silver,
                            execution_timeout=timedelta(minutes=40))
    gold = PythonOperator(task_id="build_gold", python_callable=build_gold,
                          execution_timeout=timedelta(minutes=30),
                          outlets=[Dataset(GOLD_DATASET_URI)])      # เสร็จแล้วปลุก bitkub_forecast_publish_dag
    create_tables >> silver >> gold

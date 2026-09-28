"""
bitkub_lake.py — โค้ดส่วนกลางของโปรเจกต์ Bitkub Pump Radar (ใช้ร่วมกันใน bk_01 / bk_02 / bk_03)
================
ไฟล์นี้ไม่ได้สร้าง DAG เอง เป็น helper ที่ DAG อื่น import ไปใช้
(Airflow ใส่โฟลเดอร์ dags/ ไว้ใน sys.path ให้อยู่แล้ว)

รวมไว้ 3 เรื่อง:
1) เรียก Bitkub Public API (ฟรี ไม่ต้องใช้ key)
2) เชื่อม DuckDB เข้ากับ Data Lake (SeaweedFS ผ่าน S3 API)
3) ค่าคงที่ของ lake + เกณฑ์การตรวจจับ pump

โครงสร้าง Data Lake (Medallion):

    s3://bitkub-lake/
      bronze/candles/symbol=BTC_THB/<from>_<to>.parquet   <- แท่งเทียน 1 นาทีดิบจาก API (1 ไฟล์ต่อการดึง 1 ครั้ง)
      bronze/ticker/dt=YYYY-MM-DD/<time>.parquet          <- ภาพรวมตลาด 24 ชม. ทุกชั่วโมง
      silver/features/symbol=BTC_THB/data.parquet         <- dedupe + feature รายนาที (return, volume ratio ฯลฯ)
      gold/<table>/snapshot.parquet                       <- ตารางสรุปพร้อมใช้ (โหลดเข้า Postgres ด้วย)
      ml/<model>/<run_id>/dataset.parquet                 <- dataset ที่ใช้เทรนแต่ละรอบ

เวลาทั้งหมดใน lake เป็น UTC (แปลงเป็นเวลาไทยตอนแสดงผลเท่านั้น)
"""

import os
import time

import requests

# -----------------------------------------------------------------
# Bitkub Public API — ไม่ต้องใช้ key
# -----------------------------------------------------------------
BITKUB_API = "https://api.bitkub.com"
TOP_N = int(os.environ.get("BITKUB_TOP_N", "50"))                  # เก็บกี่เหรียญ (เรียงตาม volume 24 ชม.)
BACKFILL_DAYS = int(os.environ.get("BITKUB_BACKFILL_DAYS", "365"))  # ดึงย้อนหลังกี่วันในการรันครั้งแรก
CHUNK_DAYS = 30                 # ดึงทีละ 30 วันต่อครั้ง (API คืนได้ ~43,000 แท่งต่อครั้ง)
REQUEST_PAUSE_S = 0.35          # เว้นจังหวะระหว่างการเรียก (API ไม่ระบุโควตาชัด -> เรียกแบบสุภาพ)

_session = requests.Session()
_session.headers["User-Agent"] = "bitkub-pump-radar/1.0 (student big data project)"


def api_get(path, params=None, timeout=30, retries=4):
    """GET พร้อม retry แบบ backoff (เจอ 429/5xx หรือเน็ตสะดุด จะรอแล้วลองใหม่)"""
    for attempt in range(retries):
        try:
            resp = _session.get(BITKUB_API + path, params=params, timeout=timeout)
            if resp.status_code == 429 or resp.status_code >= 500:
                raise requests.HTTPError(f"HTTP {resp.status_code}")
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as exc:
            if attempt == retries - 1:
                raise
            wait = 2 ** attempt * 2
            print(f"  {path} ล้มเหลว ({exc}) — รอ {wait} วินาทีแล้วลองใหม่")
            time.sleep(wait)


def fetch_symbols():
    """รายการคู่เหรียญทั้งหมด (มีวันที่เปิดเทรด created_at ใช้บอกว่าเป็นเหรียญใหม่หรือไม่)"""
    return api_get("/api/v3/market/symbols")["result"]


def fetch_ticker():
    """สถิติ 24 ชม. ของทุกคู่ (ราคาล่าสุด, % เปลี่ยน, volume)"""
    return api_get("/api/v3/market/ticker")


def fetch_candles(symbol, start_ts, end_ts, resolution="1"):
    """
    แท่งเทียนจาก endpoint tradingview/history
    คืนค่า list ของ (ts, open, high, low, close, volume) — มีแท่งเฉพาะนาทีที่มีการซื้อขาย
    """
    data = api_get("/tradingview/history", {
        "symbol": symbol, "resolution": resolution, "from": int(start_ts), "to": int(end_ts),
    })
    if not isinstance(data, dict) or data.get("s") != "ok" or not data.get("t"):
        return []
    return list(zip(data["t"], data["o"], data["h"], data["l"], data["c"], data["v"]))


# -----------------------------------------------------------------
# Data Lake บน SeaweedFS (S3 API) + DuckDB
# -----------------------------------------------------------------
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "seaweedfs:8333")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "lake")      # SeaweedFS ใน compose ไม่ได้เปิดระบบสิทธิ์
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "lake")
S3_USE_SSL = os.environ.get("S3_USE_SSL", "false").lower() == "true"
LAKE_BUCKET = os.environ.get("LAKE_BUCKET", "bitkub-lake")
# LAKE_ROOT ใช้ทดสอบบนเครื่องโดยไม่มี S3 (เช่น C:/tmp/lake) — ปกติเว้นว่างไว้
LAKE = os.environ.get("LAKE_ROOT") or f"s3://{LAKE_BUCKET}"

BRONZE_CANDLES = f"{LAKE}/bronze/candles"
BRONZE_TICKER = f"{LAKE}/bronze/ticker"
SILVER_FEATURES = f"{LAKE}/silver/features"
GOLD = f"{LAKE}/gold"
ML = f"{LAKE}/ml"
# Airflow Dataset: bk_02 ประกาศเมื่อโหลด gold เข้า Postgres เสร็จ -> bk_03 (publish) รันต่อทันที
GOLD_DATASET_URI = "postgres://postgres_target/etl_db/gold"

DUCKDB_MEMORY_LIMIT = os.environ.get("DUCKDB_MEMORY_LIMIT", "1GB")
DUCKDB_THREADS = int(os.environ.get("DUCKDB_THREADS", "2"))

# -----------------------------------------------------------------
# เกณฑ์การตรวจจับ (gold เก็บ "ผู้ต้องสงสัย" ด้วยเกณฑ์ต่ำ แล้วให้ Dashboard กรองเกณฑ์จริงอีกชั้น)
# -----------------------------------------------------------------
CANDIDATE_PUMP_PCT = 5.0        # ราคาขึ้น >= 5% ภายใน 5 นาที
CANDIDATE_VOL_X = 5.0           # และ volume 5 นาที >= 5 เท่าของค่าปกติ (เฉลี่ย 7 วัน)
DEFAULT_PUMP_PCT = 10.0         # เกณฑ์เริ่มต้นบน Dashboard
DEFAULT_VOL_X = 10.0
DUMP_WINDOW_MIN = 60            # ดูว่าราคาร่วงกลับภายในกี่นาทีหลังจุดสูงสุด
EVENT_GAP_MIN = 30              # สัญญาณห่างกันเกินนี้ = คนละเหตุการณ์

BANGKOK_OFFSET_H = 7


def duckdb_conn(memory_limit=None):
    """
    เปิด DuckDB (in-memory) ที่อ่าน/เขียน lake ได้
    - จำกัด RAM (Docker Desktop มักมีแค่ ~4 GB) ถ้าเกิน DuckDB จะ spill ลงดิสก์เอง
    - httpfs extension ถูกดาวน์โหลดครั้งแรกจาก extensions.duckdb.org
    """
    import duckdb

    con = duckdb.connect()
    con.execute(f"SET memory_limit = '{memory_limit or DUCKDB_MEMORY_LIMIT}';")
    con.execute(f"SET threads = {DUCKDB_THREADS};")
    con.execute("SET preserve_insertion_order = false;")
    con.execute("SET temp_directory = '/tmp/duckdb_spill';")
    con.execute("SET TimeZone = 'UTC';")
    if LAKE.startswith("s3://"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"""
            CREATE OR REPLACE SECRET lake (
                TYPE S3, KEY_ID '{S3_ACCESS_KEY}', SECRET '{S3_SECRET_KEY}',
                ENDPOINT '{S3_ENDPOINT}', URL_STYLE 'path',
                USE_SSL {str(S3_USE_SSL).lower()}, REGION 'us-east-1'
            );
        """)
    return con


def lake_glob_exists(con, pattern):
    """มีไฟล์ตรง glob ใน lake หรือไม่ (read_parquet จะ error ถ้าไม่เจอไฟล์เลย)"""
    return con.execute("SELECT count(*) FROM glob(?)", [pattern]).fetchone()[0] > 0


def pg_replace_table(conn, table, relation):
    """
    แทนที่ข้อมูลทั้งตารางใน Postgres ด้วยผลลัพธ์จาก DuckDB (TRUNCATE + COPY ใน transaction เดียว)
    ให้ DuckDB เขียน CSV เอง (ไม่ผ่าน pandas) จึงเร็วและไม่ขึ้นกับเวอร์ชัน pandas
    """
    import tempfile

    columns = relation.columns
    with tempfile.TemporaryDirectory() as tmp:
        csv_path = os.path.join(tmp, "data.csv")
        relation.write_csv(csv_path, header=False)
        with conn.cursor() as cur, open(csv_path, encoding="utf-8") as fh:
            cur.execute(f"TRUNCATE {table};")
            cur.copy_expert(f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '')", fh)
    conn.commit()
    return relation.count("*").fetchone()[0]

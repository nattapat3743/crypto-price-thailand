"""
api.py — Pump Radar API (/api/*)
================
อ่านตาราง gold / ML ที่ Airflow โหลดไว้ใน postgres_target แล้วเสิร์ฟเป็น JSON ให้หน้า Dashboard
ไม่เรียก Bitkub API เอง — ทุกตัวเลขมาจาก Data Lake

ทุก endpoint ที่เกี่ยวกับเหตุการณ์รับตัวกรองชุดเดียวกัน:
    days       ช่วงเวลาย้อนหลัง (0 = ทั้งหมด)
    min_pump   ราคาพุ่งอย่างน้อยกี่ %            (ค่าเริ่มต้น 10)
    min_volx   volume อย่างน้อยกี่เท่าของค่าปกติ   (ค่าเริ่มต้น 10)
    single     true = ตัดเหตุการณ์ที่เกิดพร้อมกันหลายเหรียญ (ข่าว/ทั้งตลาด) ออก
ตัวเลขทุกส่วนบนหน้าเว็บจึงตรงกันเสมอ

ถ้าตารางยังไม่ถูกสร้าง (DAG ยังไม่เคยรัน) จะคืนค่าว่างแทน error
"""

import os
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras
from fastapi import APIRouter, Depends, Query

router = APIRouter(prefix="/api")

PG = dict(
    host=os.environ.get("PG_HOST", "postgres_target"),
    port=int(os.environ.get("PG_PORT", "5432")),
    dbname=os.environ.get("PG_DB", "etl_db"),
    user=os.environ.get("PG_USER", "etluser"),
    password=os.environ.get("PG_PASSWORD", "etlpass"),
    connect_timeout=5,
)
NEW_LISTING_DAYS = 90          # เปิดเทรดไม่ถึง 90 วัน = "เหรียญใหม่"
LOW_LIQUIDITY_SHARE = 0.5      # มีการซื้อขายไม่ถึงครึ่งของนาที = "สภาพคล่องต่ำ"


def _query(sql, params=None, one=False):
    try:
        conn = psycopg2.connect(**PG)
    except psycopg2.OperationalError:
        return None if one else []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchone() if one else cur.fetchall()
    except (psycopg2.errors.UndefinedTable, psycopg2.errors.UndefinedColumn):
        return None if one else []
    finally:
        conn.close()


def _iso(v):
    return v.isoformat() if hasattr(v, "isoformat") else v


def _clean(rows):
    return [{k: _iso(v) for k, v in r.items()} for r in rows]


def _since(days):
    return datetime(1970, 1, 1) if days <= 0 else datetime.utcnow() - timedelta(days=days)


def _event_filter(days, min_pump, min_volx, single):
    sql = "e.start_ts >= %s AND e.pump_pct >= %s AND e.max_vol_x >= %s"
    if single:
        sql += " AND coalesce(e.concurrent_symbols, 0) < 2"
    return sql, [_since(days), min_pump, min_volx]


class F:
    """ตัวกรองชุดเดียวกันทุก endpoint"""
    def __init__(self, days: int = Query(90, ge=0), min_pump: float = Query(10.0, ge=0),
                 min_volx: float = Query(10.0, ge=0), single: bool = Query(True)):
        self.days, self.min_pump, self.min_volx, self.single = days, min_pump, min_volx, single

    def where(self):
        return _event_filter(self.days, self.min_pump, self.min_volx, self.single)



@router.get("/overview")
def overview(f: F = Depends()):
    where, params = f.where()
    lake = _query("""
        SELECT count(*) AS coins, coalesce(sum(bronze_rows), 0) AS bronze_rows, coalesce(sum(candles), 0) AS candles,
               coalesce(sum(duplicates), 0) AS duplicates, coalesce(sum(bronze_files), 0) AS bronze_files,
               min(first_ts) AS first_ts, max(last_ts) AS last_ts, avg(active_share) AS avg_active_share
        FROM dq_symbol""", one=True) or {}
    ingest = _query("""
        SELECT max(run_at) AS last_run, count(DISTINCT run_at) AS runs, coalesce(sum(api_calls), 0) AS api_calls
        FROM ingest_log""", one=True) or {}
    last_run = _query("""
        SELECT coalesce(sum(rows), 0) AS rows, coalesce(sum(api_calls), 0) AS calls, coalesce(sum(duration_ms), 0) AS ms
        FROM ingest_log WHERE run_at = (SELECT max(run_at) FROM ingest_log)""", one=True) or {}
    gold = _query("SELECT refreshed_at, duration_s, events FROM gold_refresh_log ORDER BY id DESC LIMIT 1", one=True) or {}
    models = _query("SELECT count(DISTINCT model_name) AS n FROM model_metrics WHERE deployed", one=True) or {}
    ev = _query(f"""
        SELECT count(*) AS events,
               count(*) FILTER (WHERE is_pump_dump) AS pump_dumps,
               avg(pump_pct) AS avg_pump_pct,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY minutes_to_half_retrace)
                   FILTER (WHERE is_pump_dump) AS median_minutes_to_dump,
               avg(dump_pct) AS avg_dump_pct,
               count(DISTINCT symbol) AS coins_hit
        FROM gold_events e WHERE {where}""", params, one=True) or {}
    top = _query(f"""
        SELECT symbol, count(*) AS n FROM gold_events e WHERE {where}
        GROUP BY symbol ORDER BY n DESC, symbol LIMIT 1""", params, one=True) or {}
    all_events = _query("SELECT count(*) AS n FROM gold_events", one=True) or {}
    return {
        "ready": bool(lake.get("coins")),
        "lake": {k: _iso(v) for k, v in lake.items()},
        "ingest": {**{k: _iso(v) for k, v in ingest.items()}, "last_run_rows": last_run.get("rows", 0),
                   "last_run_calls": last_run.get("calls", 0), "last_run_ms": last_run.get("ms", 0)},
        "gold": {k: _iso(v) for k, v in gold.items()},
        "models_deployed": models.get("n", 0),
        "events": {k: _iso(v) for k, v in ev.items()},
        "candidates_total": all_events.get("n", 0),
        "top_coin": top,
    }


@router.get("/events")
def events(f: F = Depends(), limit: int = Query(200, ge=1, le=1000)):
    where, params = f.where()
    rows = _query(f"""
        SELECT e.event_id, e.symbol, p.name, e.start_ts, e.peak_ts, e.base_price, e.peak_price,
               e.pump_pct, e.max_vol_x, e.max_chg_5m_pct, e.dump_pct, e.retrace_share,
               e.minutes_to_peak, e.minutes_to_half_retrace, e.is_pump_dump, e.active_share_1d,
               e.vol_thb_event, e.concurrent_symbols, e.anomaly_score,
               date_part('day', e.start_ts - p.listed_at) AS days_since_listing
        FROM gold_events e LEFT JOIN pairs p ON p.symbol = e.symbol
        WHERE {where}
        ORDER BY e.start_ts DESC LIMIT %s""", params + [limit])
    return _clean(rows)


@router.get("/event_candles")
def event_candles(event_id: str):
    rows = _query("SELECT ts, open, high, low, close, vol_thb FROM gold_event_candles WHERE event_id = %s ORDER BY ts",
                  (event_id,))
    return _clean(rows)


@router.get("/coins")
def coins(f: F = Depends(), limit: int = Query(12, ge=1, le=100)):
    where, params = f.where()
    rows = _query(f"""
        SELECT e.symbol, max(p.name) AS name, count(*) AS events,
               count(*) FILTER (WHERE e.is_pump_dump) AS pump_dumps,
               max(e.pump_pct) AS max_pump_pct,
               max(c.active_share) AS active_share, max(c.avg_daily_vol_thb_30d) AS avg_daily_vol_thb,
               max(date_part('day', (now() AT TIME ZONE 'UTC') - p.listed_at)) AS days_listed
        FROM gold_events e
        LEFT JOIN pairs p ON p.symbol = e.symbol
        LEFT JOIN gold_coin_stats c ON c.symbol = e.symbol
        WHERE {where}
        GROUP BY e.symbol ORDER BY events DESC, max_pump_pct DESC LIMIT %s""", params + [limit])
    for r in rows:
        tags = []
        if r["days_listed"] is not None and r["days_listed"] < NEW_LISTING_DAYS:
            tags.append("เหรียญใหม่")
        if r["active_share"] is not None and r["active_share"] < LOW_LIQUIDITY_SHARE:
            tags.append("สภาพคล่องต่ำ")
        r["tags"] = tags
    return _clean(rows)


@router.get("/heatmap")
def heatmap(f: F = Depends()):
    where, params = f.where()
    return _query(f"""
        SELECT dow_local, hour_local, count(*) AS n FROM gold_events e WHERE {where}
        GROUP BY 1, 2 ORDER BY 1, 2""", params)


@router.get("/market")
def market(days: int = Query(7, ge=1, le=365)):
    rows = _query("""
        SELECT hour_utc, vol_thb, avg_rv_pct, symbols FROM gold_market_hourly
        WHERE hour_utc >= (SELECT max(hour_utc) FROM gold_market_hourly) - %s * INTERVAL '1 day'
        ORDER BY hour_utc""", (days,))
    fc = _query("""SELECT target_hour_utc, predicted, lower, upper, generated_at FROM forecasts
                   WHERE model_name = 'volume_forecast' ORDER BY target_hour_utc""")
    return {"actual": _clean(rows), "forecast": _clean(fc)}


@router.get("/volatility")
def volatility(symbol: str = Query(None), days: int = Query(3, ge=1, le=60)):
    symbols = [r["symbol"] for r in _query(
        "SELECT DISTINCT symbol FROM forecasts WHERE model_name = 'volatility_forecast' ORDER BY symbol")]
    if not symbols:
        symbols = [r["symbol"] for r in _query(
            "SELECT symbol FROM gold_coin_stats ORDER BY avg_daily_vol_thb_30d DESC NULLS LAST LIMIT 10")]
    symbol = symbol if symbol in symbols else (symbols[0] if symbols else None)
    if not symbol:
        return {"symbols": [], "symbol": None, "actual": [], "forecast": []}
    rows = _query("""
        SELECT hour_utc, rv_pct, vol_thb FROM gold_symbol_hourly WHERE symbol = %s
          AND hour_utc >= (SELECT max(hour_utc) FROM gold_symbol_hourly WHERE symbol = %s) - %s * INTERVAL '1 day'
        ORDER BY hour_utc""", (symbol, symbol, days))
    fc = _query("""SELECT target_hour_utc, predicted, lower, upper FROM forecasts
                   WHERE model_name = 'volatility_forecast' AND symbol = %s ORDER BY target_hour_utc""", (symbol,))
    return {"symbols": symbols, "symbol": symbol, "actual": _clean(rows), "forecast": _clean(fc)}


@router.get("/early_warnings")
def early_warnings():
    return _clean(_query("SELECT symbol, ts, probability, chg_5m_pct, vol_x_5m, outcome, generated_at "
                         "FROM early_warnings ORDER BY ts DESC LIMIT 30"))


@router.get("/models")
def models():
    rows = _query("""
        SELECT DISTINCT ON (model_name) model_name, metric, value, baseline, higher_is_better, deployed,
               train_rows, test_rows, extra, run_at
        FROM model_metrics ORDER BY model_name, run_at DESC""")
    deployed = {r["model_name"]: r for r in _query("""
        SELECT DISTINCT ON (model_name) model_name, value, run_at FROM model_metrics
        WHERE deployed ORDER BY model_name, run_at DESC""")}
    for r in rows:
        d = deployed.get(r["model_name"])
        r["deployed_value"] = d["value"] if d else None
        r["deployed_at"] = _iso(d["run_at"]) if d else None
    return _clean(rows)


@router.get("/ticker")
def ticker():
    rows = _query("""
        SELECT t.symbol, t.last, t.percent_change, t.volume_thb_24h, coalesce(p.selected, false) AS tracked
        FROM market_ticker t LEFT JOIN pairs p ON p.symbol = t.symbol
        WHERE t.symbol LIKE '%%\\_THB' AND t.volume_thb_24h > 0""")
    rows = _clean(rows)
    return {
        "gainers": sorted(rows, key=lambda r: -r["percent_change"])[:6],
        "losers": sorted(rows, key=lambda r: r["percent_change"])[:6],
        "snapshot_at": _iso((_query("SELECT max(snapshot_at) AS t FROM market_ticker", one=True) or {}).get("t")),
    }


@router.get("/pipeline")
def pipeline():
    runs = _query("""
        SELECT run_at, count(*) AS coins, sum(rows) AS rows, sum(api_calls) AS calls,
               sum(duration_ms) AS duration_ms, bool_or(is_backfill) AS backfill
        FROM ingest_log GROUP BY run_at ORDER BY run_at DESC LIMIT 24""")
    dq = _query("""SELECT symbol, candles, duplicates, active_share, first_ts, last_ts
                   FROM dq_symbol ORDER BY active_share""")
    return {"runs": _clean(runs)[::-1], "dq": _clean(dq)}


@router.get("/prices")
def prices():
    """ตารางราคาทุกเหรียญที่ระบบเฝ้าดู + sparkline 7 วัน (ราคาปิดทุก 4 ชม. จาก gold_symbol_hourly)"""
    rows = _query("""
        WITH tracked AS (SELECT symbol FROM dq_symbol),
        spark AS (
            SELECT symbol, array_agg(close ORDER BY hour_utc) AS spark
            FROM gold_symbol_hourly
            WHERE hour_utc >= (SELECT max(hour_utc) FROM gold_symbol_hourly) - INTERVAL '7 days'
              AND extract(hour FROM hour_utc)::int %% 4 = 0
            GROUP BY symbol
        ),
        ev AS (SELECT symbol, count(*) AS events_90d FROM gold_events
               WHERE start_ts >= (now() AT TIME ZONE 'UTC') - INTERVAL '90 days' AND pump_pct >= 10 AND max_vol_x >= 10
               GROUP BY symbol)
        SELECT t.symbol, p.name, p.rank_24h, p.listed_at,
               coalesce(m.last, c.last_close) AS last, m.percent_change, m.volume_thb_24h,
               c.active_share, s.spark, coalesce(ev.events_90d, 0) AS events_90d
        FROM tracked t
        LEFT JOIN pairs p ON p.symbol = t.symbol
        LEFT JOIN market_ticker m ON m.symbol = t.symbol
        LEFT JOIN gold_coin_stats c ON c.symbol = t.symbol
        LEFT JOIN spark s ON s.symbol = t.symbol
        LEFT JOIN ev ON ev.symbol = t.symbol
        ORDER BY m.volume_thb_24h DESC NULLS LAST""", ())
    snap = _query("SELECT max(snapshot_at) AS t FROM market_ticker", one=True) or {}
    return {"snapshot_at": _iso(snap.get("t")), "coins": _clean(rows)}


@router.get("/coin")
def coin(symbol: str, days: int = Query(90, ge=1, le=400)):
    """รายละเอียดเหรียญ: ข้อมูลเหรียญ, กราฟราคา (รายชั่วโมง ≤ 30 วัน / รายวันถ้ายาวกว่า), เหตุการณ์, ค่าพยากรณ์"""
    info = _query("""
        SELECT p.symbol, p.name, p.listed_at, p.rank_24h, m.last, m.percent_change, m.high_24h, m.low_24h, m.volume_thb_24h,
               c.first_ts, c.last_ts, c.candles, c.active_share, c.avg_daily_vol_thb_30d
        FROM pairs p LEFT JOIN market_ticker m ON m.symbol = p.symbol LEFT JOIN gold_coin_stats c ON c.symbol = p.symbol
        WHERE p.symbol = %s""", (symbol,), one=True)
    if not info:
        return {"found": False}
    bucket = "hour" if days <= 30 else "day"
    series = _query(f"""
        SELECT date_trunc('{bucket}', hour_utc) AS t,
               (array_agg(close ORDER BY hour_utc DESC))[1] AS close,
               max(high) AS high, min(low) AS low, sum(vol_thb) AS vol_thb, avg(rv_pct) AS rv_pct
        FROM gold_symbol_hourly
        WHERE symbol = %s AND hour_utc >= (SELECT max(hour_utc) FROM gold_symbol_hourly WHERE symbol = %s) - %s * INTERVAL '1 day'
        GROUP BY 1 ORDER BY 1""", (symbol, symbol, days))
    events = _query("""
        SELECT event_id, start_ts, peak_ts, pump_pct, dump_pct, max_vol_x, is_pump_dump, concurrent_symbols, anomaly_score
        FROM gold_events
        WHERE symbol = %s AND start_ts >= (now() AT TIME ZONE 'UTC') - %s * INTERVAL '1 day' AND pump_pct >= 10 AND max_vol_x >= 10
        ORDER BY start_ts DESC""", (symbol, days))
    vol = _query("""
        SELECT avg(rv_pct) AS avg_rv_30d FROM gold_symbol_hourly
        WHERE symbol = %s AND hour_utc >= (SELECT max(hour_utc) FROM gold_symbol_hourly) - INTERVAL '30 days'""", (symbol,), one=True) or {}
    fc = _query("""SELECT target_hour_utc, predicted FROM forecasts
                   WHERE model_name = 'volatility_forecast' AND symbol = %s ORDER BY target_hour_utc""", (symbol,))
    return {"found": True, "bucket": bucket, "info": {k: _iso(v) for k, v in info.items()}, "avg_rv_30d": vol.get("avg_rv_30d"),
            "series": _clean(series), "events": _clean(events), "forecast": _clean(fc)}


# ================================================================= ความสัมพันธ์ระหว่างเหรียญ
_corr_cache = {}


@router.get("/correlation")
def correlation(days: int = Query(30, ge=3, le=365), top: int = Query(15, ge=3, le=30)):
    """
    ค่าสหสัมพันธ์ (correlation) ของผลตอบแทนรายชั่วโมงระหว่างเหรียญ
    - matrix: เหรียญ volume สูงสุด `top` เหรียญ (ใช้วาด heatmap)
    - btc: ทุกเหรียญเทียบกับ BTC (ขยับตาม BTC แค่ไหน)
    คำนวณจาก gold_symbol_hourly แล้วแคชไว้ 10 นาที (self-join หลักล้านแถว)
    """
    import time as _t
    key = (days, top)
    hit = _corr_cache.get(key)
    if hit and _t.time() - hit[0] < 600:
        return hit[1]
    base = """
        WITH h AS (
            SELECT symbol, hour_utc,
                   ln(close / nullif(lag(close) OVER (PARTITION BY symbol ORDER BY hour_utc), 0)) AS r
            FROM gold_symbol_hourly
            WHERE close > 0 AND hour_utc >= (SELECT max(hour_utc) FROM gold_symbol_hourly) - %s * INTERVAL '1 day'
        )"""
    symbols = [r["symbol"] for r in _query(
        "SELECT symbol FROM gold_coin_stats ORDER BY avg_daily_vol_thb_30d DESC NULLS LAST LIMIT %s", (top,))]
    matrix = _query(base + """
        SELECT a.symbol AS a, b.symbol AS b, corr(a.r, b.r) AS c, count(*) AS n
        FROM h a JOIN h b ON a.hour_utc = b.hour_utc AND a.symbol <= b.symbol
        WHERE a.symbol = ANY(%s) AND b.symbol = ANY(%s) AND a.r IS NOT NULL AND b.r IS NOT NULL
        GROUP BY 1, 2""", (days, symbols, symbols)) if symbols else []
    btc = _query(base + """
        SELECT o.symbol, corr(o.r, b.r) AS c, count(*) AS n
        FROM h o JOIN h b ON b.hour_utc = o.hour_utc AND b.symbol = 'BTC_THB'
        WHERE o.symbol <> 'BTC_THB' AND o.r IS NOT NULL AND b.r IS NOT NULL
        GROUP BY o.symbol HAVING count(*) >= 24 ORDER BY c DESC""", (days,))
    out = {"days": days, "symbols": symbols, "matrix": _clean(matrix), "btc": _clean(btc)}
    _corr_cache[key] = (_t.time(), out)
    return out


# ================================================================= ประวัติความแม่นของโมเดล / backtest
@router.get("/model_history")
def model_history():
    return _clean(_query("""
        SELECT model_name, metric, value, baseline, higher_is_better, deployed, run_at
        FROM model_metrics WHERE model_name <> 'anomaly_scoring' ORDER BY run_at"""))


@router.get("/backtest")
def backtest(model: str = Query("volume_forecast"), symbol: str = Query("MARKET")):
    """ผลทายของโมเดลรอบล่าสุดบนชุดทดสอบ + ค่าพยากรณ์ที่เคยเผยแพร่แล้วเทียบของจริง"""
    options = _query("""SELECT DISTINCT model_name, symbol FROM forecast_backtest ORDER BY model_name, symbol""")
    rows = _query("""SELECT hour_utc, actual, predicted, baseline, run_id FROM forecast_backtest
                     WHERE model_name = %s AND symbol = %s ORDER BY hour_utc""", (model, symbol))
    if model == "volume_forecast":
        live = _query("""
            SELECT DISTINCT ON (f.target_hour_utc) f.target_hour_utc AS hour_utc, f.predicted, m.vol_thb AS actual, f.generated_at
            FROM forecast_history f JOIN gold_market_hourly m ON m.hour_utc = f.target_hour_utc
            WHERE f.model_name = 'volume_forecast'
            ORDER BY f.target_hour_utc, f.generated_at DESC""")
    else:
        live = _query("""
            SELECT DISTINCT ON (f.target_hour_utc) f.target_hour_utc AS hour_utc, f.predicted, s.rv_pct AS actual, f.generated_at
            FROM forecast_history f JOIN gold_symbol_hourly s ON s.hour_utc = f.target_hour_utc AND s.symbol = f.symbol
            WHERE f.model_name = %s AND f.symbol = %s
            ORDER BY f.target_hour_utc, f.generated_at DESC""", (model, symbol))
    return {"options": _clean(options), "model": model, "symbol": symbol, "rows": _clean(rows), "live": _clean(live)}


# ================================================================= ดาวน์โหลด CSV
from fastapi.responses import Response  # noqa: E402


def _csv(rows, columns, filename):
    import csv
    import io
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(columns)
    for r in rows:
        w.writerow(["" if r.get(c) is None else (r[c].isoformat() if hasattr(r[c], "isoformat") else r[c]) for c in columns])
    # BOM ให้ Excel อ่านภาษาไทยถูก
    return Response("﻿" + buf.getvalue(), media_type="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@router.get("/export/events.csv")
def export_events(f: F = Depends()):
    where, params = f.where()
    rows = _query(f"""
        SELECT e.event_id, e.symbol, p.name, e.start_ts, e.peak_ts, e.base_price, e.peak_price, e.pump_pct, e.max_vol_x,
               e.dump_pct, e.minutes_to_peak, e.minutes_to_half_retrace, e.is_pump_dump, e.concurrent_symbols,
               e.active_share_1d, e.vol_thb_event, e.anomaly_score
        FROM gold_events e LEFT JOIN pairs p ON p.symbol = e.symbol
        WHERE {where} ORDER BY e.start_ts DESC""", params)
    cols = ["event_id", "symbol", "name", "start_ts", "peak_ts", "base_price", "peak_price", "pump_pct", "max_vol_x", "dump_pct",
            "minutes_to_peak", "minutes_to_half_retrace", "is_pump_dump", "concurrent_symbols", "active_share_1d", "vol_thb_event", "anomaly_score"]
    return _csv(rows, cols, f"events_{f.days or 'all'}d_pump{f.min_pump:g}_vol{f.min_volx:g}.csv")


@router.get("/export/prices.csv")
def export_prices():
    rows = prices()["coins"]
    cols = ["symbol", "name", "last", "percent_change", "volume_thb_24h", "active_share", "events_90d", "rank_24h", "listed_at"]
    return _csv(rows, cols, "prices.csv")


@router.get("/export/coin.csv")
def export_coin(symbol: str, days: int = Query(90, ge=1, le=400)):
    d = coin(symbol, days)
    if not d.get("found"):
        return _csv([], ["t", "close"], f"{symbol}.csv")
    return _csv(d["series"], ["t", "close", "high", "low", "vol_thb", "rv_pct"], f"{symbol}_{days}d_{d['bucket']}.csv")

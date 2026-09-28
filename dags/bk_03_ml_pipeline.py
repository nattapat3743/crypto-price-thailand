"""
bk_03_ml_pipeline.py
================
Stage 3: ML — พยากรณ์ + ตรวจจับ บนข้อมูลใน lake (รันทุก 6 ชั่วโมง)

5 โมเดล แยกเป็น task ละตัว (ตัวหนึ่งพังไม่กระทบตัวอื่น):

1) volume_forecast      : พยากรณ์ volume ซื้อขายรวมทั้งตลาดรายชั่วโมง 6 ชม. ข้างหน้า
                          baseline = "ชั่วโมงหน้าเท่ากับชั่วโมงเดียวกันของเมื่อวาน"
2) volatility_forecast  : พยากรณ์ความผันผวน (realized volatility) ชั่วโมงหน้าของ 10 เหรียญใหญ่
                          baseline = "ชั่วโมงหน้าผันผวนเท่าชั่วโมงนี้"
3) early_warning        : เห็นสัญญาณแรก (ราคา +3% และ volume 3 เท่าใน 5 นาที) แล้วทายว่า
                          จะพุ่งถึง +10% ภายใน 1 ชม. หรือไม่ | baseline = กฎ "volume >= 10 เท่า"
4) direction_experiment : (ทดลอง) ทายว่าราคาอีก 15 นาทีจะสูงหรือต่ำกว่าตอนนี้
                          baseline = ทายคำตอบที่เจอบ่อยที่สุดเสมอ
                          + จำลองเทรดตามโมเดลแล้วหักค่าธรรมเนียม เพื่อดูว่า "ทายถูกบ่อย" = "ได้กำไร" หรือไม่
5) anomaly_scoring      : Isolation Forest ให้คะแนนความผิดปกติ (0-1) กับทุกเหตุการณ์ใน gold_events

หลักการเดียวกับโปรเจกต์เดิม:
- แบ่ง train/test ตามเวลา (ห้ามสุ่ม ไม่งั้นโมเดลแอบเห็นอนาคต)
- champion-challenger: deploy เฉพาะเมื่อ "ชนะ baseline" และ "ดีกว่าโมเดลที่ deploy อยู่"
- dataset ของทุกรอบเก็บเป็น Parquet ใน lake (ml/<model>/<run_id>/) ไม่ส่งผ่าน XCom
⚠️ เป็นงานวิเคราะห์ข้อมูล ไม่ใช่สัญญาณซื้อขาย
"""

import math
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pyarrow as pa
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from bitkub_lake import BANGKOK_OFFSET_H, ML, SILVER_FEATURES, duckdb_conn, lake_glob_exists

POSTGRES_CONN_ID = "postgres_target"
MODEL_ROOT = os.environ.get("MODEL_ROOT", "/opt/airflow/models")
TEST_FRACTION = 0.2
HORIZON_H = 6
TOP_VOL_SYMBOLS = 10
EARLY_CHG, EARLY_VOLX, TARGET_PUMP = 0.03, 3.0, 0.10

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS model_metrics (
    id SERIAL PRIMARY KEY,
    model_name TEXT NOT NULL,
    metric TEXT NOT NULL,                 -- ชื่อตัววัด เช่น MAPE, RMSE, AUC, accuracy
    value FLOAT, baseline FLOAT,
    higher_is_better BOOLEAN NOT NULL,
    deployed BOOLEAN NOT NULL,
    train_rows INT, test_rows INT,
    extra JSONB,
    run_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'UTC')
);
CREATE TABLE IF NOT EXISTS forecasts (
    model_name TEXT, symbol TEXT, target_hour_utc TIMESTAMP, predicted FLOAT, generated_at TIMESTAMP
);
-- ผลทายของโมเดลรอบล่าสุดบนชุดทดสอบ (ข้อมูลช่วงท้าย 20% ที่โมเดลไม่เคยเห็น) ใช้วาด "ทาย vs จริง"
CREATE TABLE IF NOT EXISTS forecast_backtest (
    model_name TEXT, symbol TEXT, hour_utc TIMESTAMP,
    actual FLOAT, predicted FLOAT, baseline FLOAT, run_id TEXT
);
-- เก็บค่าพยากรณ์ทุกครั้งที่เผยแพร่ (ไม่ลบ) เพื่อย้อนเทียบกับของจริงเมื่อเวลาผ่านไป
CREATE TABLE IF NOT EXISTS forecast_history (
    model_name TEXT, symbol TEXT, target_hour_utc TIMESTAMP, predicted FLOAT, generated_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS early_warnings (
    symbol TEXT, ts TIMESTAMP, probability FLOAT,
    chg_5m_pct FLOAT, vol_x_5m FLOAT, outcome TEXT, generated_at TIMESTAMP
);
"""


# ----------------------------------------------------------------- helpers
def _hook():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def _save_dataset(df, model_name, run_id):
    path = f"{ML}/{model_name}/{run_id.replace(':', '-').replace('+', '-')}/dataset.parquet"
    con = duckdb_conn()
    con.register("ds", pa.Table.from_pandas(df, preserve_index=False))
    con.execute(f"COPY ds TO '{path}' (FORMAT PARQUET);")
    con.close()
    return path


def _time_split(df, time_col):
    df = df.sort_values(time_col).reset_index(drop=True)
    cut = df[time_col].iloc[int(len(df) * (1 - TEST_FRACTION))]
    return df[df[time_col] < cut], df[df[time_col] >= cut]


def _champion(model_name, metric, higher_is_better):
    row = _hook().get_first(
        "SELECT value FROM model_metrics WHERE model_name = %s AND metric = %s AND deployed "
        "ORDER BY run_at DESC LIMIT 1", parameters=(model_name, metric))
    return row[0] if row else None


def _decide_and_log(model_name, metric, value, baseline, higher_is_better, model, train_n, test_n, extra=None):
    """champion-challenger: ต้องชนะ baseline และดีกว่า (หรือเท่า) โมเดลที่ deploy อยู่"""
    import json

    import joblib

    better = (lambda a, b: a > b) if higher_is_better else (lambda a, b: a < b)
    champion = _champion(model_name, metric, higher_is_better)
    beats_baseline = baseline is None or better(value, baseline)
    beats_champion = champion is None or not better(champion, value)
    deploy = model is not None and beats_baseline and beats_champion
    if deploy:
        os.makedirs(os.path.join(MODEL_ROOT, model_name), exist_ok=True)
        joblib.dump(model, os.path.join(MODEL_ROOT, model_name, "current_model.pkl"))
    _hook().run(
        """INSERT INTO model_metrics (model_name, metric, value, baseline, higher_is_better, deployed,
                                      train_rows, test_rows, extra)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        parameters=(model_name, metric, float(value), None if baseline is None else float(baseline),
                    higher_is_better, deploy, int(train_n), int(test_n), json.dumps(extra or {})),
    )
    print(f"[{model_name}] {metric} = {value:.4f} | baseline = {baseline} | champion = {champion} "
          f"-> {'DEPLOY' if deploy else 'ไม่ deploy'}")
    return deploy


def _load_model(model_name):
    import joblib

    path = os.path.join(MODEL_ROOT, model_name, "current_model.pkl")
    return joblib.load(path) if os.path.exists(path) else None


def _hour_feats(ts):
    local = ts + pd.Timedelta(hours=BANGKOK_OFFSET_H)
    h = local.dt.hour if hasattr(local, "dt") else local.hour
    dow = local.dt.dayofweek if hasattr(local, "dt") else local.dayofweek
    return np.sin(2 * np.pi * h / 24), np.cos(2 * np.pi * h / 24), dow


def _complete_hours(df, col="hour_utc"):
    """ตัดชั่วโมงปัจจุบัน (ยังเก็บไม่ครบ) ออก"""
    current = pd.Timestamp.utcnow().tz_localize(None).floor("h")
    return df[df[col] < current]


# ----------------------------------------------------------------- 1) volume forecast
VOL_FEATS = ["lag1", "lag2", "lag3", "lag24", "roll24", "hour_sin", "hour_cos", "dow"]


def _volume_frame(hourly):
    s = hourly.set_index("hour_utc")["y"].asfreq("h")          # ชั่วโมงที่ไม่มีข้อมูล = NaN (ไม่เดาเติม)
    out = pd.DataFrame({"hour_utc": s.index, "y": s.values})
    for k in (1, 2, 3, 24):
        out[f"lag{k}"] = s.shift(k).values
    out["roll24"] = s.shift(1).rolling(24, min_periods=12).mean().values
    out["hour_sin"], out["hour_cos"], out["dow"] = _hour_feats(out["hour_utc"])
    return out


def volume_forecast(**kwargs):
    from sklearn.ensemble import HistGradientBoostingRegressor

    rows = _hook().get_records("SELECT hour_utc, vol_thb FROM gold_market_hourly ORDER BY hour_utc")
    hourly = _complete_hours(pd.DataFrame(rows, columns=["hour_utc", "vol_thb"]).assign(
        hour_utc=lambda d: pd.to_datetime(d["hour_utc"])))
    if len(hourly) < 24 * 14:
        print(f"ข้อมูลรายชั่วโมงมี {len(hourly)} ชม. — ต้องการอย่างน้อย 14 วัน ข้ามรอบนี้")
        return
    hourly["y"] = np.log1p(hourly["vol_thb"].astype(float))     # log เพราะ volume กระจายตัวกว้างมาก
    frame = _volume_frame(hourly).dropna()
    _save_dataset(frame, "volume_forecast", kwargs["run_id"])
    train, test = _time_split(frame, "hour_utc")

    model = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05, random_state=42)
    model.fit(train[VOL_FEATS], train["y"])
    pred = np.expm1(model.predict(test[VOL_FEATS]))
    actual = np.expm1(test["y"].values)
    mape = float(np.mean(np.abs(pred - actual) / actual) * 100)
    base = float(np.mean(np.abs(np.expm1(test["lag24"].values) - actual) / actual) * 100)
    _save_backtest("volume_forecast", kwargs["run_id"], test["hour_utc"], ["MARKET"] * len(test),
                   actual, pred, np.expm1(test["lag24"].values))
    _decide_and_log("volume_forecast", "MAPE", mape, base, False, model, len(train), len(test),
                    {"unit": "% คลาดเคลื่อนเฉลี่ย", "baseline": "ค่าชั่วโมงเดียวกันเมื่อวาน"})

    # ---- publish: พยากรณ์ 6 ชม. ข้างหน้าแบบ recursive ด้วยโมเดลที่ deploy อยู่
    deployed = _load_model("volume_forecast")
    if deployed is None:
        return
    series = hourly.set_index("hour_utc")["y"].asfreq("h")
    generated = datetime.utcnow()
    out = []
    for step in range(1, HORIZON_H + 1):
        target = series.index[-1] + pd.Timedelta(hours=1)
        hist = series
        feat = {f"lag{k}": hist.iloc[-k] for k in (1, 2, 3, 24)}
        feat["roll24"] = hist.iloc[-24:].mean()
        hs, hc, dw = _hour_feats(pd.Timestamp(target))
        feat.update(hour_sin=hs, hour_cos=hc, dow=dw)
        y = float(deployed.predict(pd.DataFrame([feat])[VOL_FEATS])[0])
        series.loc[target] = y
        out.append(("volume_forecast", "MARKET", target.to_pydatetime(), float(np.expm1(y)), generated))
    _replace_forecasts("volume_forecast", out)


def _save_backtest(model_name, run_id, hours, symbols, actual, pred, baseline):
    """เก็บผลทายบนชุดทดสอบของรอบล่าสุด (แทนที่ของเดิม) ให้ Dashboard วาดเทียบกับค่าจริง"""
    rows = [(model_name, sym, pd.Timestamp(h).to_pydatetime(), float(a), float(p), None if pd.isna(b) else float(b), run_id)
            for h, sym, a, p, b in zip(hours, symbols, actual, pred, baseline)]
    conn = _hook().get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM forecast_backtest WHERE model_name = %s", (model_name,))
        cur.executemany("INSERT INTO forecast_backtest (model_name, symbol, hour_utc, actual, predicted, baseline, run_id) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s)", rows)
    conn.commit()
    conn.close()
    print(f"[{model_name}] เก็บผล backtest {len(rows):,} แถว")


def _replace_forecasts(model_name, rows):
    conn = _hook().get_conn()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM forecasts WHERE model_name = %s", (model_name,))
        cur.executemany("INSERT INTO forecasts (model_name, symbol, target_hour_utc, predicted, generated_at) "
                        "VALUES (%s, %s, %s, %s, %s)", rows)
        cur.executemany("INSERT INTO forecast_history (model_name, symbol, target_hour_utc, predicted, generated_at) "
                        "VALUES (%s, %s, %s, %s, %s)", rows)
    conn.commit()
    conn.close()
    print(f"[{model_name}] เผยแพร่ค่าพยากรณ์ {len(rows)} แถว")


# ----------------------------------------------------------------- 2) volatility forecast
RV_FEATS = ["lag1", "lag2", "lag3", "lag24", "roll24", "vol_lag1", "hour_sin", "hour_cos", "dow", "sym_mean"]


def _rv_frame(df):
    parts = []
    for sym, g in df.groupby("symbol"):
        s = g.set_index("hour_utc")["y"].asfreq("h", fill_value=0.0)       # ไม่มีการซื้อขาย = ไม่ผันผวน
        v = g.set_index("hour_utc")["lv"].asfreq("h", fill_value=0.0)
        f = pd.DataFrame({"hour_utc": s.index, "symbol": sym, "y": s.values})
        for k in (1, 2, 3, 24):
            f[f"lag{k}"] = s.shift(k).values
        f["roll24"] = s.shift(1).rolling(24).mean().values
        f["vol_lag1"] = v.shift(1).values
        parts.append(f)
    out = pd.concat(parts, ignore_index=True)
    out["hour_sin"], out["hour_cos"], out["dow"] = _hour_feats(out["hour_utc"])
    return out


def volatility_forecast(**kwargs):
    from sklearn.ensemble import HistGradientBoostingRegressor

    hook = _hook()
    top = [r[0] for r in hook.get_records(
        "SELECT symbol FROM gold_coin_stats ORDER BY avg_daily_vol_thb_30d DESC NULLS LAST LIMIT %s",
        parameters=(TOP_VOL_SYMBOLS,))]
    if not top:
        print("ยังไม่มี gold_coin_stats — ข้าม")
        return
    rows = hook.get_records(
        "SELECT symbol, hour_utc, rv_pct, vol_thb FROM gold_symbol_hourly WHERE symbol = ANY(%s) ORDER BY hour_utc",
        parameters=(top,))
    df = _complete_hours(pd.DataFrame(rows, columns=["symbol", "hour_utc", "rv_pct", "vol_thb"]).assign(
        hour_utc=lambda d: pd.to_datetime(d["hour_utc"])))
    if df["hour_utc"].nunique() < 24 * 14:
        print("ข้อมูลน้อยกว่า 14 วัน — ข้ามรอบนี้")
        return
    df["y"] = np.log1p(df["rv_pct"].astype(float))
    df["lv"] = np.log1p(df["vol_thb"].astype(float))
    frame = _rv_frame(df).dropna()
    train, test = _time_split(frame, "hour_utc")
    sym_mean = train.groupby("symbol")["y"].mean()                 # target encoding จาก train เท่านั้น
    for part in (train, test, frame):
        part["sym_mean"] = part["symbol"].map(sym_mean).fillna(sym_mean.mean())
    _save_dataset(frame, "volatility_forecast", kwargs["run_id"])

    model = HistGradientBoostingRegressor(max_iter=300, learning_rate=0.05, random_state=42)
    model.fit(train[RV_FEATS], train["y"])
    pred = np.expm1(model.predict(test[RV_FEATS]))
    actual = np.expm1(test["y"].values)
    rmse = float(np.sqrt(np.mean((pred - actual) ** 2)))
    base = float(np.sqrt(np.mean((np.expm1(test["lag1"].values) - actual) ** 2)))
    _save_backtest("volatility_forecast", kwargs["run_id"], test["hour_utc"], test["symbol"].tolist(),
                   actual, pred, np.expm1(test["lag1"].values))
    _decide_and_log("volatility_forecast", "RMSE", rmse, base, False, {"model": model, "sym_mean": sym_mean},
                    len(train), len(test), {"unit": "จุด % ของความผันผวนรายชั่วโมง", "baseline": "เท่าชั่วโมงก่อนหน้า",
                                            "symbols": top})

    bundle = _load_model("volatility_forecast")
    if bundle is None:
        return
    generated, out = datetime.utcnow(), []
    for sym in top:
        g = df[df["symbol"] == sym]
        if g.empty:
            continue
        s = g.set_index("hour_utc")["y"].asfreq("h", fill_value=0.0)
        v = g.set_index("hour_utc")["lv"].asfreq("h", fill_value=0.0)
        smean = bundle["sym_mean"].get(sym, float(bundle["sym_mean"].mean()))
        for _ in range(HORIZON_H):
            target = s.index[-1] + pd.Timedelta(hours=1)
            feat = {f"lag{k}": s.iloc[-k] for k in (1, 2, 3, 24)}
            feat.update(roll24=s.iloc[-24:].mean(), vol_lag1=v.iloc[-1], sym_mean=smean)
            hs, hc, dw = _hour_feats(pd.Timestamp(target))
            feat.update(hour_sin=hs, hour_cos=hc, dow=dw)
            y = float(bundle["model"].predict(pd.DataFrame([feat])[RV_FEATS])[0])
            s.loc[target] = y
            v.loc[target] = v.iloc[-24]                            # สมมติ volume เท่าเวลาเดียวกันเมื่อวาน
            out.append(("volatility_forecast", sym, target.to_pydatetime(), float(np.expm1(y)), generated))
    _replace_forecasts("volatility_forecast", out)


# ----------------------------------------------------------------- 3) early warning
EW_FEATS = ["chg_5m", "vol_x_5m", "ret_1m", "range_1m", "active_share_1d", "log_vol5", "gap_min",
            "hour_sin", "hour_cos", "dow"]


def _early_triggers(con, since=None):
    """สัญญาณแรกของแต่ละช่วง 30 นาที: ราคา +3% และ volume 3 เท่าใน 5 นาที"""
    where = f"AND ts >= TIMESTAMP '{since:%Y-%m-%d %H:%M:%S}'" if since else ""
    return con.execute(f"""
        WITH t AS (
            SELECT symbol, ts, chg_5m, vol_x_5m, ret_1m, range_1m, active_share_1d, gap_min,
                   ln(1 + vol5_thb) AS log_vol5, open_5m, max_high_next60,
                   max_high_next60 / nullif(open_5m, 0) - 1 AS future_gain,
                   ts - lag(ts) OVER (PARTITION BY symbol ORDER BY ts) AS since_prev
            FROM read_parquet('{SILVER_FEATURES}/*/data.parquet', hive_partitioning = false)
            WHERE chg_5m >= {EARLY_CHG} AND vol_x_5m >= {EARLY_VOLX} {where}
        )
        SELECT * EXCLUDE (since_prev) FROM t
        WHERE since_prev IS NULL OR since_prev > INTERVAL 30 MINUTE
        ORDER BY ts
    """).df()


def early_warning(**kwargs):
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.metrics import precision_score, recall_score, roc_auc_score

    con = duckdb_conn()
    if not lake_glob_exists(con, f"{SILVER_FEATURES}/*/data.parquet"):
        print("ยังไม่มี silver — ข้าม")
        return
    df = _early_triggers(con)
    con.close()
    df = df.dropna(subset=["future_gain"])
    df["label"] = (df["future_gain"] >= TARGET_PUMP).astype(int)
    df["hour_sin"], df["hour_cos"], df["dow"] = _hour_feats(df["ts"])
    df["gap_min"] = df["gap_min"].fillna(60).clip(upper=1440)
    print(f"สัญญาณแรก {len(df):,} ครั้ง กลายเป็น pump (+10%) {df['label'].sum():,} ครั้ง")
    if len(df) < 200 or df["label"].sum() < 20:
        print("ตัวอย่าง pump น้อยเกินไปสำหรับเทรน classifier — ข้ามรอบนี้")
        return
    _save_dataset(df.drop(columns=["open_5m", "max_high_next60"]), "early_warning", kwargs["run_id"])
    train, test = _time_split(df, "ts")
    if test["label"].nunique() < 2:
        print("ชุดทดสอบมีคำตอบแบบเดียว วัด AUC ไม่ได้ — ข้าม")
        return
    model = HistGradientBoostingClassifier(max_iter=250, learning_rate=0.05, class_weight="balanced", random_state=42)
    model.fit(train[EW_FEATS], train["label"])
    prob = model.predict_proba(test[EW_FEATS])[:, 1]
    auc = float(roc_auc_score(test["label"], prob))
    rule = (test["vol_x_5m"] >= 10).astype(int)
    base_auc = float(roc_auc_score(test["label"], test["vol_x_5m"]))   # ใช้ volume อย่างเดียวจัดอันดับ
    pred = (prob >= 0.5).astype(int)
    extra = {
        "precision": float(precision_score(test["label"], pred, zero_division=0)),
        "recall": float(recall_score(test["label"], pred, zero_division=0)),
        "rule_precision": float(precision_score(test["label"], rule, zero_division=0)),
        "rule_recall": float(recall_score(test["label"], rule, zero_division=0)),
        "positive_rate": float(test["label"].mean()),
        "baseline": "จัดอันดับด้วย volume อย่างเดียว",
    }
    _decide_and_log("early_warning", "AUC", auc, base_auc, True, model, len(train), len(test), extra)

    # ---- ให้คะแนนสัญญาณ 48 ชม. ล่าสุดด้วยโมเดลที่ deploy อยู่
    deployed = _load_model("early_warning")
    if deployed is None:
        return
    con = duckdb_conn()
    last_ts = con.execute(f"SELECT max(ts) FROM read_parquet('{SILVER_FEATURES}/*/data.parquet', hive_partitioning = false)").fetchone()[0]
    recent = _early_triggers(con, since=last_ts - timedelta(hours=48))
    con.close()
    generated = datetime.utcnow()
    rows = []
    if not recent.empty:
        recent["hour_sin"], recent["hour_cos"], recent["dow"] = _hour_feats(recent["ts"])
        recent["gap_min"] = recent["gap_min"].fillna(60).clip(upper=1440)
        recent["prob"] = deployed.predict_proba(recent[EW_FEATS])[:, 1]
        for r in recent.itertuples():
            done = (last_ts - r.ts) >= timedelta(minutes=60)
            outcome = ("pump" if r.future_gain >= TARGET_PUMP else "ไม่พุ่งต่อ") if done else "กำลังติดตาม"
            rows.append((r.symbol, r.ts.to_pydatetime(), float(r.prob), float(r.chg_5m * 100),
                         float(r.vol_x_5m), outcome, generated))
    conn = _hook().get_conn()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE early_warnings;")
        cur.executemany("INSERT INTO early_warnings (symbol, ts, probability, chg_5m_pct, vol_x_5m, outcome, generated_at) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s)", rows)
    conn.commit()
    conn.close()
    print(f"ให้คะแนนสัญญาณล่าสุด {len(rows)} รายการ")


# ----------------------------------------------------------------- 4) direction experiment
DIR_FEATS = ["ret_1m", "chg_5m", "vol_x_5m", "range_1m", "active_share_1d", "hour_sin", "hour_cos", "dow"]


def direction_experiment(**kwargs):
    from sklearn.ensemble import HistGradientBoostingClassifier

    hook = _hook()
    top = [r[0] for r in hook.get_records(
        "SELECT symbol FROM gold_coin_stats ORDER BY avg_daily_vol_thb_30d DESC NULLS LAST LIMIT %s",
        parameters=(TOP_VOL_SYMBOLS,))]
    if not top:
        return
    con = duckdb_conn()
    sym_list = ", ".join(f"'{s}'" for s in top)
    # สุ่ม 300,000 นาทีจาก 10 เหรียญใหญ่ (ข้อมูลทั้งหมดใหญ่เกินจำเป็นสำหรับการทดลองนี้)
    df = con.execute(f"""
        SELECT symbol, ts, ret_1m, chg_5m, vol_x_5m, range_1m, active_share_1d,
               close_next15 / close - 1 AS fwd_ret,
               (close_next15 > close)::INT AS up
        FROM read_parquet('{SILVER_FEATURES}/*/data.parquet', hive_partitioning = false)
        WHERE symbol IN ({sym_list}) AND close_next15 IS NOT NULL AND close_next15 <> close AND vol_x_5m IS NOT NULL
        USING SAMPLE 300000 ROWS (reservoir, 42)
    """).df()
    con.close()
    if len(df) < 5000:
        print("ข้อมูลน้อยเกินไป — ข้าม")
        return
    df["hour_sin"], df["hour_cos"], df["dow"] = _hour_feats(df["ts"])
    train, test = _time_split(df, "ts")
    model = HistGradientBoostingClassifier(max_iter=200, learning_rate=0.05, random_state=42)
    model.fit(train[DIR_FEATS], train["up"])
    pred = model.predict(test[DIR_FEATS])
    acc = float((pred == test["up"]).mean() * 100)
    majority = int(train["up"].mean() >= 0.5)
    base = float((test["up"] == majority).mean() * 100)
    # จำลองเทรด: ซื้อเมื่อโมเดลทายว่าขึ้น ขายอีก 15 นาทีต่อมา (ตลาด spot ขาย short ไม่ได้)
    # ความแม่นที่ดูดีมักมาจาก bid-ask bounce (ราคาเด้งระหว่างราคาเสนอซื้อ/ขาย) ซึ่งเทรดจริงไม่ได้กำไร
    buys = test.loc[pred == 1, "fwd_ret"]
    gross = float(buys.mean() * 100) if len(buys) else 0.0
    fee_pct = 0.25 * 2                                              # ค่าธรรมเนียม Bitkub ~0.25% ต่อขา ซื้อ+ขาย
    # การทดลองนี้ไม่ deploy ใช้ทายจริง — เก็บแค่ผลเพื่อแสดงบน Dashboard
    _decide_and_log("direction_experiment", "accuracy", acc, base, True, None, len(train), len(test),
                    {"unit": "% ทายถูก", "baseline": "ทายคำตอบที่เจอบ่อยสุดเสมอ", "horizon_min": 15,
                     "trades": int(len(buys)), "avg_return_gross_pct": gross,
                     "fee_round_trip_pct": fee_pct, "avg_return_net_pct": gross - fee_pct})


# ----------------------------------------------------------------- 5) anomaly scoring
AN_FEATS = ["pump_pct", "max_vol_x", "max_chg_5m_pct", "signal_minutes", "active_share_1d", "log_vol", "minutes_to_peak"]


def anomaly_scoring(**kwargs):
    from sklearn.ensemble import IsolationForest

    hook = _hook()
    rows = hook.get_records(
        "SELECT event_id, pump_pct, max_vol_x, max_chg_5m_pct, signal_minutes, active_share_1d, "
        "vol_thb_event, minutes_to_peak FROM gold_events")
    df = pd.DataFrame(rows, columns=["event_id", "pump_pct", "max_vol_x", "max_chg_5m_pct", "signal_minutes",
                                     "active_share_1d", "vol_thb_event", "minutes_to_peak"])
    if len(df) < 30:
        print(f"เหตุการณ์มี {len(df)} รายการ — น้อยเกินไป ข้าม")
        return
    df["log_vol"] = np.log1p(df["vol_thb_event"].astype(float))
    df["max_vol_x"] = np.log1p(df["max_vol_x"].astype(float))
    X = df[AN_FEATS].astype(float).fillna(df[AN_FEATS].astype(float).median())
    model = IsolationForest(n_estimators=300, contamination="auto", random_state=42).fit(X)
    raw = -model.score_samples(X)                                    # ยิ่งสูงยิ่งผิดปกติ
    score = (pd.Series(raw).rank(pct=True)).values                   # แปลงเป็น 0-1 ตามอันดับ
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.executemany("UPDATE gold_events SET anomaly_score = %s WHERE event_id = %s",
                        [(float(s), e) for s, e in zip(score, df["event_id"])])
    conn.commit()
    conn.close()
    os.makedirs(os.path.join(MODEL_ROOT, "anomaly_scoring"), exist_ok=True)
    import joblib
    joblib.dump(model, os.path.join(MODEL_ROOT, "anomaly_scoring", "current_model.pkl"))
    hook.run(
        """INSERT INTO model_metrics (model_name, metric, value, baseline, higher_is_better, deployed, train_rows, test_rows, extra)
           VALUES ('anomaly_scoring', 'events_scored', %s, NULL, TRUE, TRUE, %s, 0, '{"unit": "เหตุการณ์"}')""",
        parameters=(len(df), len(df)))
    print(f"ให้คะแนนความผิดปกติ {len(df)} เหตุการณ์")


with DAG(
    dag_id="bitkub_ml_pipeline_dag",
    description="Stage 3: พยากรณ์ volume/ความผันผวน + early warning + anomaly score (champion-challenger)",
    default_args={"owner": "bigdata", "retries": 1, "retry_delay": timedelta(minutes=3)},
    schedule="45 */6 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bitkub", "stage-3", "ml-pipeline", "forecast"],
) as dag:
    create_tables = PostgresOperator(task_id="create_tables", postgres_conn_id=POSTGRES_CONN_ID, sql=CREATE_TABLES_SQL)
    # รันทีละตัว (Docker Desktop มี RAM จำกัด) — trigger_rule all_done: ตัวก่อนพังตัวถัดไปยังรันต่อ
    previous = create_tables
    for name, fn in [
        ("volume_forecast", volume_forecast),
        ("volatility_forecast", volatility_forecast),
        ("early_warning", early_warning),
        ("direction_experiment", direction_experiment),
        ("anomaly_scoring", anomaly_scoring),
    ]:
        task = PythonOperator(task_id=name, python_callable=fn, execution_timeout=timedelta(minutes=30),
                              trigger_rule="all_done")
        previous >> task
        previous = task

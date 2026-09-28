"""
main.py — Bitkub Pump Radar: API + Dashboard
================
FastAPI service แยกจาก Airflow (คนละ container) ทำหน้าที่:
- /api/*      : JSON จากตาราง gold / ML ใน postgres_target (ดู api.py)
- /           : หน้า Dashboard (dashboard.html — อ่านไฟล์ใหม่ทุกครั้ง แก้แล้วกด F5 ได้เลย)
- /health     : เช็คสถานะ service
"""

from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from api import _query, router

app = FastAPI(title="Bitkub Pump Radar API", version="1.0.0")
app.include_router(router)
PAGE = Path(__file__).with_name("dashboard.html")


@app.get("/health")
def health():
    db_ok = _query("SELECT 1 AS ok", one=True) is not None
    return {"status": "ok", "database": db_ok}


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return PAGE.read_text(encoding="utf-8")

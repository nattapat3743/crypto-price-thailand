# Dockerfile — Bitkub Pump Radar API + Dashboard
# แยก image ออกจาก Airflow โดยสิ้นเชิง เบาและเริ่มเร็ว
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py api.py dashboard.html ./

EXPOSE 8000

# --reload: แก้ main.py/api.py บนเครื่อง host (mount เข้ามาที่ /app) แล้ว service restart เอง
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

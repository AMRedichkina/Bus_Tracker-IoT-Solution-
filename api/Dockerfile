FROM python:3.7-slim-buster
COPY api.py /app/api.py
COPY secret.py /app/secret.py
RUN pip install fastapi influxdb_client uvicorn
WORKDIR /app
CMD ["uvicorn", "api:app", "--reload", "--host", "0.0.0.0", "--port", "80"]
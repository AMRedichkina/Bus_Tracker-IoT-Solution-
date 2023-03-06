FROM python:3.7-slim-buster
COPY sink_connector.py /app/sink_connector.py
COPY secret.py /app/secret.py
RUN pip install influxdb-client confluent-kafka
WORKDIR /app
CMD ["python", "-u", "sink_connector.py"]

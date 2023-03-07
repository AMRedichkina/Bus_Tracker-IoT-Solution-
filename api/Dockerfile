FROM python:3.7-slim-buster

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "api:app", "--reload", "--host", "0.0.0.0", "--port", "80"]
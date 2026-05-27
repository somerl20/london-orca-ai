FROM python:3.12-slim

# Java is required for PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/worker/worker.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "worker.py"]

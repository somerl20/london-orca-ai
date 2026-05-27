#!/usr/bin/env python3
"""
Generator — each ship runs as an independent thread, uploads a JSON telemetry
file to SeaweedFS S3, then publishes a metadata message to RabbitMQ.

Config (env vars):
  SOURCES        number of ships (default 10)
  RATE           base files/min per ship (default 2)
  SIZE           small | medium | large (default small)
  RATE_JITTER    rate randomisation fraction per ship (default 0.3)
  SIZE_RANDOM    if true, each ship picks a random size tier (default false)
  S3_ENDPOINT    SeaweedFS S3 URL
  S3_BUCKET      bucket name (created if missing)
  S3_ACCESS_KEY
  S3_SECRET_KEY
  RABBITMQ_URL   amqp://user:pass@host:port/
  RABBITMQ_QUEUE queue name (default files)
"""

import json
import logging
import os
import random
import signal
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from io import BytesIO

import boto3
import pika
from botocore.client import Config
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("generator")

ROWS_BY_SIZE: dict[str, int] = {
    "small": 100,
    "medium": 30_000,
    "large": 250_000,
}


def _gps() -> dict:
    return {
        "lat": round(random.uniform(-90.0, 90.0), 6),
        "lon": round(random.uniform(-180.0, 180.0), 6),
        "speed_knots": round(random.uniform(0.0, 25.0), 2),
    }


MEASUREMENT_SCHEMAS: dict[str, Callable[[], dict]] = {
    "gps": _gps,
}


def _make_s3():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "any"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "any"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        log.info("Created bucket: %s", bucket)


def _wait_for_s3(bucket: str, max_wait: int = 120) -> boto3.client:
    deadline = time.monotonic() + max_wait
    delay = 2
    while True:
        try:
            s3 = _make_s3()
            _ensure_bucket(s3, bucket)
            return s3
        except Exception as exc:
            if time.monotonic() > deadline:
                raise RuntimeError(f"S3 not reachable after {max_wait}s") from exc
            log.warning("Waiting for S3 (%s) — retry in %ds", exc.__class__.__name__, delay)
            time.sleep(delay)
            delay = min(delay * 2, 30)


def _wait_for_rabbitmq(url: str, max_wait: int = 120) -> pika.BlockingConnection:
    deadline = time.monotonic() + max_wait
    delay = 2
    while True:
        try:
            return pika.BlockingConnection(pika.URLParameters(url))
        except Exception as exc:
            if time.monotonic() > deadline:
                raise RuntimeError(f"RabbitMQ not reachable after {max_wait}s") from exc
            log.warning("Waiting for RabbitMQ (%s) — retry in %ds", exc.__class__.__name__, delay)
            time.sleep(delay)
            delay = min(delay * 2, 30)


def _build_file(source_id: str, measurement: str, row_count: int) -> tuple[bytes, str]:
    now = datetime.now(timezone.utc)
    filename = f"{source_id}_{measurement}_{now.strftime('%Y%m%dT%H%M%SZ')}.json"
    schema_fn = MEASUREMENT_SCHEMAS[measurement]

    parts = ["[\n"]
    for i in range(row_count):
        ts = (now + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%SZ")
        record = {
            "source_id": source_id,
            "measurement": measurement,
            "timestamp": ts,
            "values": schema_fn(),
        }
        parts.append(json.dumps(record))
        if i < row_count - 1:
            parts.append(",\n")
    parts.append("\n]\n")
    return "".join(parts).encode(), filename


def _ship_loop(
    source_id: str,
    measurement: str,
    row_count: int,
    base_rate: float,
    jitter: float,
    bucket: str,
    queue: str,
    stop: threading.Event,
    total: list,
    lock: threading.Lock,
) -> None:
    s3 = _wait_for_s3(bucket)
    conn = _wait_for_rabbitmq(os.environ["RABBITMQ_URL"])
    ch = conn.channel()
    ch.queue_declare(queue=queue, durable=True)
    ch.queue_declare(queue=f"{queue}.dlq", durable=True)

    rate = base_rate * (1 + random.uniform(-jitter, jitter))
    interval = 60.0 / max(rate, 0.1)
    log.info("%s starting — %.1f files/min (%.1fs interval)", source_id, rate, interval)

    while not stop.is_set():
        t0 = time.monotonic()
        try:
            data, filename = _build_file(source_id, measurement, row_count)
            file_key = f"{source_id}/{filename}"

            s3.upload_fileobj(BytesIO(data), bucket, file_key)

            ch.basic_publish(
                exchange="",
                routing_key=queue,
                body=json.dumps({
                    "file_key": file_key,
                    "source_id": source_id,
                    "measurement": measurement,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "size": len(data),
                }),
                properties=pika.BasicProperties(delivery_mode=2),
            )

            with lock:
                total[0] += 1
                n = total[0]

            elapsed = time.monotonic() - t0
            log.info("[%s] #%04d  %s  (%d KB, %.2fs)", source_id, n, filename, len(data) // 1024, elapsed)

        except Exception as exc:
            log.error("[%s] error: %s", source_id, exc)

        stop.wait(max(0.0, interval - (time.monotonic() - t0)))

    log.info("%s stopped", source_id)
    try:
        conn.close()
    except Exception:
        pass


def main() -> None:
    sources = int(os.environ.get("SOURCES", "10"))
    rate = float(os.environ.get("RATE", "2"))
    size = os.environ.get("SIZE", "small")
    jitter = float(os.environ.get("RATE_JITTER", "0.3"))
    size_random = os.environ.get("SIZE_RANDOM", "false").lower() == "true"
    bucket = os.environ.get("S3_BUCKET", "telemetry")
    queue = os.environ.get("RABBITMQ_QUEUE", "files")

    stop = threading.Event()
    total = [0]
    lock = threading.Lock()

    def _shutdown(signum, frame):
        log.info("Shutdown — finishing in-flight uploads…")
        stop.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    measurements = list(MEASUREMENT_SCHEMAS)
    threads = []
    for i in range(1, sources + 1):
        source_id = f"ship-{i:02d}"
        row_count = ROWS_BY_SIZE[random.choice(list(ROWS_BY_SIZE)) if size_random else size]
        t = threading.Thread(
            target=_ship_loop,
            name=source_id,
            daemon=True,
            args=(
                source_id,
                random.choice(measurements),
                row_count,
                rate,
                jitter,
                bucket,
                queue,
                stop,
                total,
                lock,
            ),
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    with lock:
        log.info("Stopped. %d file(s) produced.", total[0])


if __name__ == "__main__":
    main()

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
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import TypeVar

import boto3
import pika
from botocore.client import Config
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("generator")

# ── Constants ─────────────────────────────────────────────────────────────────

ROWS_BY_SIZE: dict[str, int] = {
    "small": 100,
    "medium": 30_000,
    "large": 250_000,
}

MAX_WAIT_SECONDS: int = 120
INITIAL_RETRY_DELAY: int = 2
MAX_RETRY_DELAY: int = 30
PUBLISH_MAX_WAIT_SECONDS: int = 60
RECONCILE_INTERVAL_SECONDS: int = 60
MIN_RATE: float = 0.1
BYTES_PER_KB: int = 1024
SECONDS_PER_MINUTE: float = 60.0

# ── Data shapes ───────────────────────────────────────────────────────────────

_T = TypeVar("_T")


@dataclass
class GeneratorConfig:
    """Runtime configuration parsed from environment variables."""

    sources: int
    rate: float
    size: str
    jitter: float
    size_random: bool
    bucket: str
    queue: str


@dataclass
class _SharedCounter:
    """Thread-safe integer counter shared across ship threads."""

    value: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def increment(self) -> int:
        """Increment atomically and return the new value."""
        with self.lock:
            self.value += 1
            return self.value


# ── Measurement generators ────────────────────────────────────────────────────

def _gps() -> dict:
    """Return a single random GPS measurement."""
    return {
        "lat": round(random.uniform(-90.0, 90.0), 6),
        "lon": round(random.uniform(-180.0, 180.0), 6),
        "speed_knots": round(random.uniform(0.0, 25.0), 2),
    }


MEASUREMENT_SCHEMAS: dict[str, Callable[[], dict]] = {
    "gps": _gps,
}

# ── Infrastructure helpers ────────────────────────────────────────────────────


def _retry_until(
    operation: Callable[[], _T],
    service_name: str,
    max_wait: int = MAX_WAIT_SECONDS,
) -> _T:
    """
    Retry *operation* with exponential back-off until *max_wait* seconds elapse.

    Raises RuntimeError if the deadline is exceeded.
    """
    deadline = time.monotonic() + max_wait
    delay = INITIAL_RETRY_DELAY
    while True:
        try:
            return operation()
        except Exception as exc:
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f"{service_name} not reachable after {max_wait}s"
                ) from exc
            log.warning(
                "Waiting for %s (%s) — retry in %ds",
                service_name,
                exc.__class__.__name__,
                delay,
            )
            time.sleep(delay)
            delay = min(delay * 2, MAX_RETRY_DELAY)


def _make_s3() -> boto3.client:
    """Create an S3 client pointed at the configured SeaweedFS endpoint."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "any"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "any"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _ensure_bucket(s3, bucket: str) -> None:
    """Create *bucket* in S3 if it does not already exist, then set a 1-day TTL."""
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)
        log.info("Created bucket: %s", bucket)
    try:
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [{
                    "ID": "expire-orphaned-files",
                    "Status": "Enabled",
                    "Filter": {"Prefix": ""},
                    "Expiration": {"Days": 1},
                }]
            },
        )
    except Exception as exc:
        log.warning("Could not set bucket TTL (storage may not support it): %s", exc)


def _wait_for_s3(bucket: str, max_wait: int = MAX_WAIT_SECONDS) -> boto3.client:
    """Block until S3 is reachable and *bucket* exists, then return the client."""
    def connect() -> boto3.client:
        s3 = _make_s3()
        _ensure_bucket(s3, bucket)
        return s3

    return _retry_until(connect, "S3", max_wait)


def _wait_for_rabbitmq(
    url: str, max_wait: int = MAX_WAIT_SECONDS
) -> pika.BlockingConnection:
    """Block until a RabbitMQ connection can be established."""
    return _retry_until(
        lambda: pika.BlockingConnection(pika.URLParameters(url)),
        "RabbitMQ",
        max_wait,
    )


# ── File builder ──────────────────────────────────────────────────────────────


def _build_file(
    source_id: str, measurement: str, row_count: int
) -> tuple[bytes, str]:
    """Serialise *row_count* telemetry records as a JSON array."""
    now = datetime.now(timezone.utc)
    filename = (
        f"{source_id}_{measurement}_{now.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}.json"
    )
    schema_fn = MEASUREMENT_SCHEMAS[measurement]

    parts = ["[\n"]
    for i in range(row_count):
        ts = (
            now - timedelta(seconds=(row_count - 1 - i))
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
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


# ── Upload / publish helpers ──────────────────────────────────────────────────


def _upload_file(
    s3, bucket: str, source_id: str, data: bytes, filename: str
) -> str:
    """Upload *data* to S3 under *source_id/* and return the object key."""
    file_key = f"{source_id}/{filename}"
    s3.upload_fileobj(BytesIO(data), bucket, file_key)
    return file_key


def _metadata_from_key(file_key: str, size: int) -> tuple[str, str, int]:
    """Recover publish metadata from the S3 object key."""
    source_id, _, filename = file_key.partition("/")
    remainder = filename.removeprefix(f"{source_id}_")
    measurement = remainder.split("_", 1)[0]
    if measurement not in MEASUREMENT_SCHEMAS:
        measurement = next(iter(MEASUREMENT_SCHEMAS))
    return source_id, measurement, size


def _publish_confirmed(ch, queue: str, body: str) -> None:
    """Publish a durable message and require RabbitMQ publisher confirmation."""
    confirmed = ch.basic_publish(
        exchange="",
        routing_key=queue,
        body=body,
        properties=pika.BasicProperties(delivery_mode=2),
        mandatory=True,
    )
    if confirmed is False:
        raise RuntimeError(f"RabbitMQ did not confirm publish to {queue}")


def _publish_metadata(
    ch,
    queue: str,
    file_key: str,
    source_id: str,
    measurement: str,
    data_size: int,
) -> None:
    """Publish a file-ready notification message to RabbitMQ."""
    body = json.dumps({
        "file_key": file_key,
        "source_id": source_id,
        "measurement": measurement,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "size": data_size,
    })
    _retry_until(
        lambda: _publish_confirmed(ch, queue, body),
        "RabbitMQ publish",
        PUBLISH_MAX_WAIT_SECONDS,
    )


def _reconcile_orphaned_files(s3, ch, bucket: str, queue: str, prefix: str) -> None:
    """Republish queue messages for existing S3 objects that may be orphaned."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            file_key = obj["Key"]
            source_id, measurement, size = _metadata_from_key(file_key, obj.get("Size", 0))
            _publish_metadata(ch, queue, file_key, source_id, measurement, size)


def _try_reconcile_orphaned_files(s3, ch, bucket: str, queue: str, prefix: str) -> None:
    """Run reconciliation without stopping the ship loop on transient failures."""
    try:
        _reconcile_orphaned_files(s3, ch, bucket, queue, prefix)
    except Exception as exc:
        log.warning("S3 reconciliation failed for %s: %s", prefix, exc)


def _run_one_cycle(
    s3,
    ch,
    source_id: str,
    measurement: str,
    row_count: int,
    bucket: str,
    queue: str,
    counter: _SharedCounter,
) -> None:
    """Build, upload, and publish one telemetry file; log the result."""
    t0 = time.monotonic()
    try:
        data, filename = _build_file(source_id, measurement, row_count)
        file_key = _upload_file(s3, bucket, source_id, data, filename)
        _publish_metadata(
            ch, queue, file_key, source_id, measurement, len(data)
        )
        n = counter.increment()
        elapsed = time.monotonic() - t0
        log.info(
            "[%s] #%04d  %s  (%d KB, %.2fs)",
            source_id, n, filename, len(data) // BYTES_PER_KB, elapsed,
        )
    except Exception as exc:
        log.error("[%s] error: %s", source_id, exc)


def _close_connection(conn: pika.BlockingConnection) -> None:
    """Close a RabbitMQ connection, ignoring errors during shutdown."""
    try:
        conn.close()
    except Exception:
        pass


# ── Ship loop ─────────────────────────────────────────────────────────────────


def _ship_loop(
    source_id: str,
    measurement: str,
    row_count: int,
    base_rate: float,
    jitter: float,
    bucket: str,
    queue: str,
    stop: threading.Event,
    counter: _SharedCounter,
) -> None:
    """
    Main loop for a single ship thread.

    Connects to S3 and RabbitMQ, then uploads one telemetry file per interval
    until *stop* is set.
    """
    s3 = _wait_for_s3(bucket)
    conn = _wait_for_rabbitmq(os.environ["RABBITMQ_URL"])
    ch = conn.channel()
    ch.queue_declare(queue=queue, durable=True)
    ch.queue_declare(queue=f"{queue}.dlq", durable=True)
    ch.confirm_delivery()
    source_prefix = f"{source_id}/"
    _try_reconcile_orphaned_files(s3, ch, bucket, queue, source_prefix)
    last_reconcile = time.monotonic()

    rate = base_rate * (1 + random.uniform(-jitter, jitter))
    interval = SECONDS_PER_MINUTE / max(rate, MIN_RATE)
    log.info(
        "%s starting — %.1f files/min (%.1fs interval)",
        source_id, rate, interval,
    )

    while not stop.is_set():
        t0 = time.monotonic()
        _run_one_cycle(
            s3, ch, source_id, measurement, row_count, bucket, queue, counter
        )
        if time.monotonic() - last_reconcile >= RECONCILE_INTERVAL_SECONDS:
            _try_reconcile_orphaned_files(s3, ch, bucket, queue, source_prefix)
            last_reconcile = time.monotonic()
        stop.wait(max(0.0, interval - (time.monotonic() - t0)))

    log.info("%s stopped", source_id)
    _close_connection(conn)


# ── Entry point ───────────────────────────────────────────────────────────────


def _load_config() -> GeneratorConfig:
    """Parse generator configuration from environment variables."""
    return GeneratorConfig(
        sources=int(os.environ.get("SOURCES", "10")),
        rate=float(os.environ.get("RATE", "2")),
        size=os.environ.get("SIZE", "small"),
        jitter=float(os.environ.get("RATE_JITTER", "0.3")),
        size_random=os.environ.get("SIZE_RANDOM", "false").lower() == "true",
        bucket=os.environ.get("S3_BUCKET", "telemetry"),
        queue=os.environ.get("RABBITMQ_QUEUE", "files"),
    )


def _choose_row_count(cfg: GeneratorConfig) -> int:
    """Return the row count for one ship based on the size configuration."""
    size = random.choice(list(ROWS_BY_SIZE)) if cfg.size_random else cfg.size
    return ROWS_BY_SIZE[size]


def main() -> None:
    """Start one ship thread per configured source and wait for shutdown."""
    cfg = _load_config()
    stop = threading.Event()
    counter = _SharedCounter()

    def _shutdown(signum, frame) -> None:
        log.info("Shutdown — finishing in-flight uploads…")
        stop.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    measurements = list(MEASUREMENT_SCHEMAS)
    threads = []
    for i in range(1, cfg.sources + 1):
        source_id = f"ship-{i:02d}"
        t = threading.Thread(
            target=_ship_loop,
            name=source_id,
            daemon=True,
            args=(
                source_id,
                random.choice(measurements),
                _choose_row_count(cfg),
                cfg.rate,
                cfg.jitter,
                cfg.bucket,
                cfg.queue,
                stop,
                counter,
            ),
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    log.info("Stopped. %d file(s) produced.", counter.value)


if __name__ == "__main__":
    main()

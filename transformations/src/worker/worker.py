#!/usr/bin/env python3
"""
Transform worker — consumes file references from RabbitMQ, reads raw JSON
from SeaweedFS, transforms to columnar format with PySpark, and writes to
Delta Lake. Acks only after a confirmed warehouse write; sends to DLQ after
RETRY_MAX failures.

Config (env vars):
  RABBITMQ_URL     amqp://user:pass@host:port/
  RABBITMQ_QUEUE   queue to consume (default files)
  RABBITMQ_DLQ     dead-letter queue name (default files.dlq)
  RETRY_MAX        failures before DLQ (default 3)
  S3_ENDPOINT      SeaweedFS S3 URL
  S3_BUCKET        bucket name
  S3_ACCESS_KEY
  S3_SECRET_KEY
  WAREHOUSE_PATH   local path for Delta Lake tables (default /data/warehouse)
"""

import json
import logging
import os
import signal
import time

import boto3
import pika
from botocore.client import Config
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, to_timestamp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("worker")


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("transform-worker")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.minPartitionSize", "32mb")
        .config("spark.driver.memory", "1g")
    )
    # configure_spark_with_delta_pip adds spark.jars.packages so Spark resolves
    # the Delta JARs from Maven (cached after first download)
    return configure_spark_with_delta_pip(builder).getOrCreate()


def make_s3():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "any"),
        aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "any"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def wait_for_rabbitmq(url: str, max_wait: int = 120) -> pika.BlockingConnection:
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


def process(spark: SparkSession, s3, msg: dict, warehouse_path: str) -> None:
    bucket = msg["source_bucket"] if "source_bucket" in msg else os.environ.get("S3_BUCKET", "telemetry")
    file_key = msg["file_key"]

    obj = s3.get_object(Bucket=bucket, Key=file_key)
    raw = obj["Body"].read().decode()

    row_count = msg.get("size", 0) // 150  # rough rows estimate from byte size
    n_partitions = max(1, row_count // 50_000)  # ~50K rows per partition, min 1

    raw_df = (
        spark.read
        .option("multiline", "true")
        .json(spark.sparkContext.parallelize([raw], n_partitions))
    )

    df = raw_df.select(
        col("measurement"),
        col("source_id"),
        to_timestamp(col("timestamp")).alias("event_ts"),
        to_date(col("timestamp")).alias("date"),
        col("values.lat").alias("latitude"),
        col("values.lon").alias("longitude"),
        col("values.speed_knots").alias("speed_knots"),
        current_timestamp().alias("recorded_at"),
    )

    # coalesce small files into one output Parquet file per batch;
    # for large files (>50K rows) keep the natural partition count
    df_out = df.coalesce(1) if n_partitions == 1 else df

    delta_path = f"{warehouse_path}/measurements"
    (
        df_out.write
        .format("delta")
        .mode("append")
        .clusterBy("event_ts", "source_id")
        .save(delta_path)
    )

    s3.delete_object(Bucket=bucket, Key=file_key)
    log.info("Processed: %s  (%d rows, %d partition(s))", file_key, df.count(), n_partitions)



def main() -> None:
    rmq_url = os.environ["RABBITMQ_URL"]
    queue = os.environ.get("RABBITMQ_QUEUE", "files")
    dlq = os.environ.get("RABBITMQ_DLQ", "files.dlq")
    retry_max = int(os.environ.get("RETRY_MAX", "3"))
    warehouse_path = os.environ.get("WAREHOUSE_PATH", "/data/warehouse")

    spark = build_spark()
    s3 = make_s3()
    conn = wait_for_rabbitmq(rmq_url)
    ch = conn.channel()
    ch.queue_declare(queue=queue, durable=True)
    ch.queue_declare(queue=dlq, durable=True)
    ch.basic_qos(prefetch_count=1)

    def on_message(ch, method, properties, body):
        headers = properties.headers or {}
        retry_count = headers.get("x-retry-count", 0)
        try:
            msg = json.loads(body)
            process(spark, s3, msg, warehouse_path)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:
            log.error("Failed (attempt %d/%d): %s", retry_count + 1, retry_max, exc)
            if retry_count >= retry_max:
                log.error("Max retries reached — sending to DLQ: %s", body[:200])
                ch.basic_ack(delivery_tag=method.delivery_tag)
                ch.basic_publish(
                    exchange="",
                    routing_key=dlq,
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        headers={"x-retry-count": retry_count, "x-error": str(exc)},
                    ),
                )
            else:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                ch.basic_publish(
                    exchange="",
                    routing_key=queue,
                    body=body,
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        headers={"x-retry-count": retry_count + 1},
                    ),
                )

    ch.basic_consume(queue=queue, on_message_callback=on_message)

    running = True

    def _shutdown(signum, frame):
        nonlocal running
        log.info("Shutdown signal received — stopping consumer")
        running = False
        ch.stop_consuming()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    log.info("Worker ready — consuming from '%s'", queue)
    ch.start_consuming()
    conn.close()
    spark.stop()
    log.info("Worker stopped")


if __name__ == "__main__":
    main()

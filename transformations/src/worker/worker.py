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
import tempfile
import time

import boto3
import pika
from botocore.client import Config
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, to_date, to_timestamp
from pyspark.sql.types import (
    DoubleType, DateType, StringType, StructField, StructType, TimestampType
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("worker")


def _publish_confirmed(ch, routing_key: str, body: bytes, properties: pika.BasicProperties) -> None:
    """Publish only if RabbitMQ confirms durable acceptance."""
    confirmed = ch.basic_publish(
        exchange="",
        routing_key=routing_key,
        body=body,
        properties=properties,
        mandatory=True,
    )
    if confirmed is False:
        raise RuntimeError(f"RabbitMQ did not confirm publish to {routing_key}")


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


def _already_processed(spark: SparkSession, file_key: str, warehouse_path: str) -> bool:
    registry_path = f"{warehouse_path}/_processed_files"
    try:
        return (
            DeltaTable.forPath(spark, registry_path)
            .toDF()
            .filter(col("file_key") == lit(file_key))
            .count() > 0
        )
    except Exception:
        return False


def _mark_processed(spark: SparkSession, file_key: str, warehouse_path: str) -> None:
    registry_path = f"{warehouse_path}/_processed_files"
    (
        DeltaTable.createIfNotExists(spark)
        .location(registry_path)
        .addColumn("file_key", StringType())
        .execute()
    )
    spark.createDataFrame([(file_key,)], ["file_key"]).write.format("delta").mode("append").save(registry_path)


def _ensure_measurements_table(spark: SparkSession, delta_path: str) -> DeltaTable:
    """Create or migrate the measurements table used for idempotent file writes."""
    (
        DeltaTable.createIfNotExists(spark)
        .location(delta_path)
        .addColumn("file_key",     StringType())
        .addColumn("measurement",  StringType())
        .addColumn("source_id",    StringType())
        .addColumn("event_ts",     TimestampType())
        .addColumn("date",         DateType())
        .addColumn("latitude",     DoubleType())
        .addColumn("longitude",    DoubleType())
        .addColumn("speed_knots",  DoubleType())
        .addColumn("recorded_at",  TimestampType())
        .clusterBy("event_ts", "source_id")
        .execute()
    )

    table = DeltaTable.forPath(spark, delta_path)
    if "file_key" not in table.toDF().columns:
        spark.sql(f"ALTER TABLE delta.`{delta_path}` ADD COLUMNS (file_key STRING)")
        table = DeltaTable.forPath(spark, delta_path)
    return table


def process(spark: SparkSession, s3, msg: dict, warehouse_path: str) -> None:
    bucket = msg["source_bucket"] if "source_bucket" in msg else os.environ.get("S3_BUCKET", "telemetry")
    file_key = msg["file_key"]

    if _already_processed(spark, file_key, warehouse_path):
        log.info("Skipping duplicate: %s", file_key)
        try:
            s3.delete_object(Bucket=bucket, Key=file_key)
        except Exception:
            pass
        return

    row_count = msg.get("size", 0) // 150  # rough rows estimate from byte size
    n_partitions = max(1, row_count // 50_000)  # ~50K rows per partition, min 1

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        s3.download_fileobj(bucket, file_key, tmp)
        tmp_file = tmp.name
    try:
        raw_df = spark.read.option("multiline", "true").json(tmp_file).cache()
        try:
            raw_df.count()  # force materialization before temp file is deleted
        except Exception:
            log.warning("Unreadable file, skipping: %s", file_key)
            s3.delete_object(Bucket=bucket, Key=file_key)
            return
    finally:
        os.unlink(tmp_file)

    if not {"measurement", "source_id", "timestamp", "values"}.issubset(set(raw_df.columns)):
        log.warning("Schema mismatch, skipping: %s", file_key)
        s3.delete_object(Bucket=bucket, Key=file_key)
        return

    valid_df = raw_df.filter(
        col("measurement").isNotNull() &
        col("source_id").isNotNull() &
        col("timestamp").isNotNull() &
        col("values").isNotNull()
    )

    if valid_df.rdd.isEmpty():
        log.warning("No valid records in file, skipping: %s", file_key)
        s3.delete_object(Bucket=bucket, Key=file_key)
        return

    df = valid_df.select(
        lit(file_key).alias("file_key"),
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
    table = _ensure_measurements_table(spark, delta_path)

    (
        table.alias("target")
        .merge(
            df_out.alias("source"),
            """
            target.file_key = source.file_key AND
            target.source_id = source.source_id AND
            target.event_ts = source.event_ts
            """,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    _mark_processed(spark, file_key, warehouse_path)
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
    ch.confirm_delivery()
    ch.basic_qos(prefetch_count=5)

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
                try:
                    _publish_confirmed(
                        ch,
                        dlq,
                        body,
                        pika.BasicProperties(
                            delivery_mode=2,
                            headers={"x-retry-count": retry_count, "x-error": str(exc)},
                        ),
                    )
                except Exception:
                    log.exception("DLQ publish failed — requeuing to avoid message loss")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    return
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                try:
                    _publish_confirmed(
                        ch,
                        queue,
                        body,
                        pika.BasicProperties(
                            delivery_mode=2,
                            headers={"x-retry-count": retry_count + 1},
                        ),
                    )
                except Exception:
                    log.exception("Retry publish failed — requeuing original to avoid message loss")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                    return
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

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

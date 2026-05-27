"""
Resilience tests: parallel ships, variable file sizes, bursty load, restart recovery.

T1 / T2 use moto S3 — no Docker required.
T3 / T8 are marked slow: real RabbitMQ via testcontainers, run in CI only.
"""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO

import boto3
import pika
import pytest
from moto import mock_aws
from testcontainers.rabbitmq import RabbitMqContainer

from generator.generator import _build_file, ROWS_BY_SIZE
from worker.worker import build_spark, process

BUCKET = "telemetry"
QUEUE  = "files"


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def spark():
    _spark = build_spark()
    yield _spark
    _spark.stop()


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client(
            "s3", region_name="us-east-1",
            aws_access_key_id="test", aws_secret_access_key="test",
        )
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture(scope="module")
def rabbitmq():
    with RabbitMqContainer("rabbitmq:3") as rmq:
        yield rmq


# ── T1: Parallel ships ────────────────────────────────────────────────────────

class TestParallelShips:
    """
    In production each worker is an independent JVM process (Docker container)
    consuming one message at a time.  Parallelism happens at the container level,
    not within a single SparkSession.  This test verifies that N ships' files can
    all flow through the same worker sequentially and land in the shared Delta table
    without loss or cross-contamination — the correctness property that matters.
    """
    SHIP_COUNT    = 10
    ROWS_PER_SHIP = ROWS_BY_SIZE["small"]  # 100

    def test_all_ships_rows_land_without_loss(self, spark, s3, tmp_path):
        for i in range(1, self.SHIP_COUNT + 1):
            source_id = f"ship-{i:02d}"
            data, fname = _build_file(source_id, "gps", self.ROWS_PER_SHIP)
            key = f"{source_id}/{fname}"
            s3.upload_fileobj(BytesIO(data), BUCKET, key)
            process(spark, s3,
                    {"file_key": key, "source_id": source_id,
                     "measurement": "gps", "size": len(data)},
                    str(tmp_path))

        from delta.tables import DeltaTable
        df = DeltaTable.forPath(spark, str(tmp_path / "measurements")).toDF()

        assert df.count() == self.SHIP_COUNT * self.ROWS_PER_SHIP
        assert df.select("source_id").distinct().count() == self.SHIP_COUNT


# ── T2: Variable file sizes ───────────────────────────────────────────────────

class TestVariableFileSizes:

    @pytest.mark.parametrize("size,row_count", [
        ("small",  ROWS_BY_SIZE["small"]),
        ("medium", ROWS_BY_SIZE["medium"]),
        ("large",  ROWS_BY_SIZE["large"]),
    ])
    def test_tier_completes_and_all_rows_land(self, spark, s3, tmp_path, size, row_count):
        data, fname = _build_file("ship-01", "gps", row_count)
        key = f"ship-01/{fname}"
        s3.upload_fileobj(BytesIO(data), BUCKET, key)

        process(spark, s3, {"file_key": key, "size": len(data)}, str(tmp_path))

        from delta.tables import DeltaTable
        df = DeltaTable.forPath(spark, str(tmp_path / "measurements")).toDF()
        assert df.count() == row_count

    def test_small_and_large_both_complete_concurrently(self, spark, s3, tmp_path):
        large_data, large_fname = _build_file("ship-large", "gps", ROWS_BY_SIZE["large"])
        small_data, small_fname = _build_file("ship-small", "gps", ROWS_BY_SIZE["small"])

        s3.upload_fileobj(BytesIO(large_data), BUCKET, f"ship-large/{large_fname}")
        s3.upload_fileobj(BytesIO(small_data), BUCKET, f"ship-small/{small_fname}")

        large_msg = {"file_key": f"ship-large/{large_fname}", "size": len(large_data)}
        small_msg = {"file_key": f"ship-small/{small_fname}", "size": len(small_data)}

        small_done_at = [None]

        def run_large():
            process(spark, s3, large_msg, str(tmp_path / "large"))

        def run_small():
            process(spark, s3, small_msg, str(tmp_path / "small"))
            small_done_at[0] = time.monotonic()

        start = time.monotonic()
        with ThreadPoolExecutor(max_workers=2) as pool:
            pool.submit(run_large)
            pool.submit(run_small)

        # Small file (100 rows) must not be blocked indefinitely by the large one.
        assert small_done_at[0] - start < 60, "small file was starved by large file"

        from delta.tables import DeltaTable
        assert DeltaTable.forPath(spark, str(tmp_path / "small" / "measurements")).toDF().count() == ROWS_BY_SIZE["small"]
        assert DeltaTable.forPath(spark, str(tmp_path / "large" / "measurements")).toDF().count() == ROWS_BY_SIZE["large"]


# ── T3: Bursty load ───────────────────────────────────────────────────────────

@pytest.mark.slow
class TestBurstyLoad:
    BURST_SIZE    = 20
    ROWS_PER_FILE = ROWS_BY_SIZE["small"]

    def _purge(self, rmq_url: str) -> None:
        conn = pika.BlockingConnection(pika.URLParameters(rmq_url))
        ch = conn.channel()
        ch.queue_declare(queue=QUEUE, durable=True)
        ch.queue_purge(queue=QUEUE)
        conn.close()

    def test_queue_drains_and_no_files_dropped(self, spark, rabbitmq, s3, tmp_path):
        rmq_url = rabbitmq.get_connection_url()
        self._purge(rmq_url)

        # Upload all files and publish all messages before the worker starts,
        # so the queue is fully loaded (bursty) from the worker's perspective.
        file_keys = []
        for i in range(self.BURST_SIZE):
            source_id = f"ship-{i:02d}"
            data, fname = _build_file(source_id, "gps", self.ROWS_PER_FILE)
            key = f"{source_id}/{fname}"
            s3.upload_fileobj(BytesIO(data), BUCKET, key)
            file_keys.append((key, source_id, len(data)))

        pub_conn = pika.BlockingConnection(pika.URLParameters(rmq_url))
        pub_ch = pub_conn.channel()
        pub_ch.queue_declare(queue=QUEUE, durable=True)
        for key, source_id, size in file_keys:
            pub_ch.basic_publish(
                exchange="", routing_key=QUEUE,
                body=json.dumps({"file_key": key, "source_id": source_id,
                                  "measurement": "gps", "size": size}),
                properties=pika.BasicProperties(delivery_mode=2),
            )
        pub_conn.close()

        processed = [0]
        done = threading.Event()

        def run_worker():
            wconn = pika.BlockingConnection(pika.URLParameters(rmq_url))
            wch = wconn.channel()
            wch.queue_declare(queue=QUEUE, durable=True)
            wch.basic_qos(prefetch_count=1)

            def on_message(ch, method, props, body):
                process(spark, s3, json.loads(body), str(tmp_path))
                ch.basic_ack(delivery_tag=method.delivery_tag)
                processed[0] += 1
                if processed[0] == self.BURST_SIZE:
                    done.set()
                    ch.stop_consuming()

            wch.basic_consume(queue=QUEUE, on_message_callback=on_message)
            wch.start_consuming()
            wconn.close()

        threading.Thread(target=run_worker, daemon=True).start()
        assert done.wait(timeout=300), f"only {processed[0]}/{self.BURST_SIZE} messages processed"

        from delta.tables import DeltaTable
        df = DeltaTable.forPath(spark, str(tmp_path / "measurements")).toDF()
        assert df.count() == self.BURST_SIZE * self.ROWS_PER_FILE

        # Queue must be fully drained
        chk_conn = pika.BlockingConnection(pika.URLParameters(rmq_url))
        chk_ch = chk_conn.channel()
        decl = chk_ch.queue_declare(queue=QUEUE, passive=True)
        chk_conn.close()
        assert decl.method.message_count == 0


# ── T8: Restart recovery ──────────────────────────────────────────────────────

@pytest.mark.slow
class TestRestartRecovery:
    ROWS = ROWS_BY_SIZE["small"]

    def _purge(self, rmq_url: str) -> None:
        conn = pika.BlockingConnection(pika.URLParameters(rmq_url))
        ch = conn.channel()
        ch.queue_declare(queue=QUEUE, durable=True)
        ch.queue_purge(queue=QUEUE)
        conn.close()

    def test_unacked_message_is_redelivered_after_connection_drop(self, spark, rabbitmq, s3, tmp_path):
        rmq_url = rabbitmq.get_connection_url()
        self._purge(rmq_url)

        data, fname = _build_file("ship-01", "gps", self.ROWS)
        key = f"ship-01/{fname}"
        s3.upload_fileobj(BytesIO(data), BUCKET, key)

        pub_conn = pika.BlockingConnection(pika.URLParameters(rmq_url))
        pub_ch = pub_conn.channel()
        pub_ch.queue_declare(queue=QUEUE, durable=True)
        pub_ch.basic_publish(
            exchange="", routing_key=QUEUE,
            body=json.dumps({"file_key": key, "source_id": "ship-01",
                              "measurement": "gps", "size": len(data)}),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        pub_conn.close()

        # Worker 1: receives the message then drops its connection without acking.
        # RabbitMQ will redeliver the message to the next consumer.
        received = threading.Event()

        def worker_1():
            w1_conn = pika.BlockingConnection(pika.URLParameters(rmq_url))
            w1_ch = w1_conn.channel()
            w1_ch.queue_declare(queue=QUEUE, durable=True)
            w1_ch.basic_qos(prefetch_count=1)

            def on_message(ch, method, props, body):
                received.set()
                # Close without acking — simulates a crash mid-processing.
                try:
                    w1_conn.close()
                except Exception:
                    pass

            w1_ch.basic_consume(queue=QUEUE, on_message_callback=on_message)
            try:
                w1_ch.start_consuming()
            except Exception:
                pass

        t1 = threading.Thread(target=worker_1, daemon=True)
        t1.start()
        assert received.wait(timeout=10), "worker 1 never received the message"
        t1.join(timeout=5)

        # Worker 2: picks up the redelivered message and processes it to completion.
        done = threading.Event()

        def worker_2():
            w2_conn = pika.BlockingConnection(pika.URLParameters(rmq_url))
            w2_ch = w2_conn.channel()
            w2_ch.queue_declare(queue=QUEUE, durable=True)

            def on_message(ch, method, props, body):
                process(spark, s3, json.loads(body), str(tmp_path))
                ch.basic_ack(delivery_tag=method.delivery_tag)
                done.set()
                ch.stop_consuming()

            w2_ch.basic_consume(queue=QUEUE, on_message_callback=on_message)
            w2_ch.start_consuming()
            w2_conn.close()

        threading.Thread(target=worker_2, daemon=True).start()
        assert done.wait(timeout=30), "worker 2 did not process the redelivered message"

        from delta.tables import DeltaTable
        df = DeltaTable.forPath(spark, str(tmp_path / "measurements")).toDF()
        assert df.count() == self.ROWS
        assert df.filter(df.source_id == "ship-01").count() == self.ROWS

"""
Ship → S3 → RabbitMQ → worker → Delta Lake

Full pipeline trace using a real RabbitMQ broker (testcontainers) and moto S3.
Marked slow: excluded from the pre-commit hook, run in CI.
"""

import json
import threading
from io import BytesIO

import boto3
import pika
import pytest
from moto import mock_aws
from testcontainers.rabbitmq import RabbitMqContainer

from generator.generator import _build_file
from worker.worker import build_spark, process

BUCKET = "telemetry"
QUEUE = "files"


@pytest.fixture(scope="module")
def rabbitmq():
    with RabbitMqContainer("rabbitmq:3") as rmq:
        yield rmq


@pytest.fixture(scope="module")
def spark():
    _spark = build_spark()
    yield _spark
    _spark.stop()


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.mark.slow
def test_ship_event_reaches_warehouse(spark, rabbitmq, s3, tmp_path):
    rmq_url = rabbitmq.get_connection_url()

    # 1. Ship generates a file and uploads to S3
    data, filename = _build_file("ship-01", "gps", 100)
    file_key = f"ship-01/{filename}"
    s3.upload_fileobj(BytesIO(data), BUCKET, file_key)

    # 2. Ship publishes the notification message to RabbitMQ
    pub_conn = pika.BlockingConnection(pika.URLParameters(rmq_url))
    pub_ch = pub_conn.channel()
    pub_ch.queue_declare(queue=QUEUE, durable=True)
    pub_ch.basic_publish(
        exchange="",
        routing_key=QUEUE,
        body=json.dumps({
            "file_key": file_key,
            "source_id": "ship-01",
            "measurement": "gps",
            "size": len(data),
        }),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    pub_conn.close()

    # 3. Worker consumes the message and transforms to Delta Lake
    done = threading.Event()

    def run_worker():
        wconn = pika.BlockingConnection(pika.URLParameters(rmq_url))
        wch = wconn.channel()
        wch.queue_declare(queue=QUEUE, durable=True)

        def on_message(ch, method, props, body):
            process(spark, s3, json.loads(body), str(tmp_path))
            ch.basic_ack(delivery_tag=method.delivery_tag)
            done.set()
            ch.stop_consuming()

        wch.basic_consume(queue=QUEUE, on_message_callback=on_message)
        wch.start_consuming()
        wconn.close()

    threading.Thread(target=run_worker, daemon=True).start()
    assert done.wait(timeout=30), "message not processed within 30 s"

    # 4. Verify data landed in the warehouse
    from delta.tables import DeltaTable

    df = DeltaTable.forPath(spark, str(tmp_path / "measurements")).toDF()

    assert df.count() == 100
    assert df.filter(df.source_id == "ship-01").count() == 100
    assert df.filter(df.measurement == "gps").count() == 100

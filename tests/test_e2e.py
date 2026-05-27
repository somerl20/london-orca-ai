"""
E2E: one telemetry file travels from generator → S3 → (RabbitMQ message) → worker → Delta Lake.

S3 is mocked with moto. RabbitMQ publish/consume is bypassed by constructing the message
payload directly — the broker is infrastructure; the data path is what we verify here.
"""

import json
from io import BytesIO
from unittest.mock import patch
import os

import boto3
import botocore.exceptions
import pytest
from moto import mock_aws

from generator.generator import _build_file
from worker.worker import build_spark, process

BUCKET = "telemetry"


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
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


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestSingleMessagePipeline:
    SOURCE_ID = "ship-01"
    MEASUREMENT = "gps"
    ROW_COUNT = 100  # ROWS_BY_SIZE["small"]

    def test_data_reaches_delta_lake(self, spark, s3, tmp_path):
        # 1. Generator produces the file
        data, filename = _build_file(self.SOURCE_ID, self.MEASUREMENT, self.ROW_COUNT)
        file_key = f"{self.SOURCE_ID}/{filename}"

        # 2. Generator uploads to S3
        s3.upload_fileobj(BytesIO(data), BUCKET, file_key)

        # 3. Generator publishes this message to RabbitMQ (we construct it directly)
        msg = {
            "file_key": file_key,
            "source_id": self.SOURCE_ID,
            "measurement": self.MEASUREMENT,
            "size": len(data),
        }

        # 4. Worker consumes the message and transforms to Delta Lake
        with patch.dict(os.environ, {"S3_BUCKET": BUCKET}):
            process(spark, s3, msg, str(tmp_path))

        # 5. Verify data landed in the warehouse
        from delta.tables import DeltaTable
        df = DeltaTable.forPath(spark, str(tmp_path / "measurements")).toDF()

        assert df.count() == self.ROW_COUNT
        assert set(df.columns) == {
            "measurement", "source_id", "event_ts", "date",
            "latitude", "longitude", "speed_knots", "recorded_at",
        }
        assert df.filter(df.source_id == self.SOURCE_ID).count() == self.ROW_COUNT
        assert df.filter(df.measurement == self.MEASUREMENT).count() == self.ROW_COUNT

    def test_raw_file_deleted_from_s3_after_processing(self, spark, s3, tmp_path):
        data, filename = _build_file(self.SOURCE_ID, self.MEASUREMENT, self.ROW_COUNT)
        file_key = f"{self.SOURCE_ID}/{filename}"

        s3.upload_fileobj(BytesIO(data), BUCKET, file_key)
        msg = {"file_key": file_key, "size": len(data)}

        with patch.dict(os.environ, {"S3_BUCKET": BUCKET}):
            process(spark, s3, msg, str(tmp_path))

        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            s3.get_object(Bucket=BUCKET, Key=file_key)
        assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"

    def test_gps_values_survive_round_trip(self, spark, s3, tmp_path):
        data, filename = _build_file(self.SOURCE_ID, self.MEASUREMENT, self.ROW_COUNT)
        file_key = f"{self.SOURCE_ID}/{filename}"

        # Verify what we know about the generated data
        source_records = json.loads(data.decode())
        expected_lats = {round(r["values"]["lat"], 6) for r in source_records}

        s3.upload_fileobj(BytesIO(data), BUCKET, file_key)
        with patch.dict(os.environ, {"S3_BUCKET": BUCKET}):
            process(spark, s3, {"file_key": file_key, "size": len(data)}, str(tmp_path))

        from delta.tables import DeltaTable
        actual_lats = {
            round(row.latitude, 6)
            for row in DeltaTable.forPath(spark, str(tmp_path / "measurements")).toDF().collect()
        }
        assert actual_lats == expected_lats

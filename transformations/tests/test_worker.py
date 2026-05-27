import json
import os
from unittest.mock import MagicMock, patch
import pytest

from worker.worker import process


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    from worker.worker import build_spark
    _spark = build_spark()
    yield _spark
    _spark.stop()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _raw_json(rows=5, source_id="ship-01") -> bytes:
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)
    records = [
        {
            "source_id": source_id,
            "measurement": "gps",
            "timestamp": (now - timedelta(seconds=rows - 1 - i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "values": {"lat": 51.5 + i * 0.1, "lon": -0.1, "speed_knots": 10.0 + i},
        }
        for i in range(rows)
    ]
    return json.dumps(records).encode()


def _mock_s3(raw: bytes) -> MagicMock:
    s3 = MagicMock()
    s3.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=raw))}
    return s3


def _method(tag=1):
    m = MagicMock()
    m.delivery_tag = tag
    return m


def _props(headers=None):
    p = MagicMock()
    p.headers = headers or {}
    return p


def _capture_callback(retry_max=3):
    """Run worker.main() with mocked infra and capture the on_message closure."""
    import worker.worker as wmod

    captured = {}
    mock_conn = MagicMock()
    mock_ch = MagicMock()
    mock_conn.channel.return_value = mock_ch
    mock_ch.basic_consume.side_effect = lambda queue, on_message_callback: captured.update(
        {"cb": on_message_callback}
    )
    mock_ch.start_consuming.return_value = None

    env = {
        "RABBITMQ_URL": "amqp://guest:guest@localhost/",
        "S3_ENDPOINT": "http://localhost:8333",
        "RABBITMQ_QUEUE": "files",
        "RABBITMQ_DLQ": "files.dlq",
        "RETRY_MAX": str(retry_max),
        "WAREHOUSE_PATH": "/tmp/test_wh",
    }
    with (
        patch("worker.worker.build_spark", return_value=MagicMock()),
        patch("worker.worker.make_s3", return_value=MagicMock()),
        patch("worker.worker.wait_for_rabbitmq", return_value=mock_conn),
        patch.dict(os.environ, env),
    ):
        wmod.main()

    return captured["cb"], mock_ch


# ── process() ─────────────────────────────────────────────────────────────────

class TestProcess:
    def test_output_schema(self, spark, tmp_path):
        raw = _raw_json(5)
        msg = {"file_key": "ship-01/f.json", "size": len(raw)}

        process(spark, _mock_s3(raw), msg, str(tmp_path))

        from delta.tables import DeltaTable
        cols = set(DeltaTable.forPath(spark, str(tmp_path / "measurements")).toDF().columns)
        assert cols == {
            "measurement", "source_id", "event_ts", "date",
            "latitude", "longitude", "speed_knots", "recorded_at",
        }

    def test_row_count(self, spark, tmp_path):
        raw = _raw_json(10)
        process(spark, _mock_s3(raw), {"file_key": "ship-01/f.json", "size": len(raw)}, str(tmp_path))

        from delta.tables import DeltaTable
        assert DeltaTable.forPath(spark, str(tmp_path / "measurements")).toDF().count() == 10

    def test_values_correctly_mapped(self, spark, tmp_path):
        raw = _raw_json(1)
        process(spark, _mock_s3(raw), {"file_key": "ship-01/f.json", "size": len(raw)}, str(tmp_path))

        from delta.tables import DeltaTable
        row = DeltaTable.forPath(spark, str(tmp_path / "measurements")).toDF().collect()[0]
        assert row.source_id == "ship-01"
        assert row.measurement == "gps"
        assert pytest.approx(row.latitude, abs=1e-6) == 51.5
        assert pytest.approx(row.longitude, abs=1e-6) == -0.1
        assert pytest.approx(row.speed_knots, abs=1e-6) == 10.0

    def test_s3_file_deleted_after_write(self, spark, tmp_path):
        raw = _raw_json(5)
        s3 = _mock_s3(raw)
        process(spark, s3, {"file_key": "ship-01/f.json", "size": len(raw)}, str(tmp_path))

        s3.delete_object.assert_called_once_with(Bucket="telemetry", Key="ship-01/f.json")

    def test_source_bucket_from_message_overrides_env(self, spark, tmp_path):
        raw = _raw_json(3)
        s3 = _mock_s3(raw)
        msg = {"file_key": "ship-01/f.json", "source_bucket": "other-bucket", "size": len(raw)}
        process(spark, s3, msg, str(tmp_path))

        s3.get_object.assert_called_once_with(Bucket="other-bucket", Key="ship-01/f.json")

    def test_appends_across_multiple_batches(self, spark, tmp_path):
        for i in range(3):
            raw = _raw_json(5, source_id=f"ship-{i:02d}")
            s3 = _mock_s3(raw)
            process(spark, s3, {"file_key": f"ship-{i:02d}/f.json", "size": len(raw)}, str(tmp_path))

        from delta.tables import DeltaTable
        assert DeltaTable.forPath(spark, str(tmp_path / "measurements")).toDF().count() == 15


# ── on_message callback ───────────────────────────────────────────────────────

class TestOnMessage:
    def test_success_acks(self):
        body = json.dumps({"file_key": "ship-01/f.json", "size": 100}).encode()
        cb, ch = _capture_callback()

        with patch("worker.worker.process"):
            cb(ch, _method(1), _props(), body)

        ch.basic_ack.assert_called_once_with(delivery_tag=1)
        ch.basic_nack.assert_not_called()

    def test_first_failure_nacks_and_requeues(self):
        body = json.dumps({"file_key": "ship-01/f.json", "size": 100}).encode()
        cb, ch = _capture_callback(retry_max=3)

        with patch("worker.worker.process", side_effect=RuntimeError("boom")):
            cb(ch, _method(1), _props({"x-retry-count": 0}), body)

        ch.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)
        ch.basic_ack.assert_not_called()

        publish_kwargs = ch.basic_publish.call_args.kwargs
        assert publish_kwargs["routing_key"] == "files"
        assert publish_kwargs["properties"].headers["x-retry-count"] == 1

    def test_max_retries_routes_to_dlq(self):
        body = json.dumps({"file_key": "ship-01/f.json", "size": 100}).encode()
        cb, ch = _capture_callback(retry_max=3)

        with patch("worker.worker.process", side_effect=RuntimeError("permanent")):
            cb(ch, _method(1), _props({"x-retry-count": 3}), body)

        ch.basic_ack.assert_called_once_with(delivery_tag=1)
        ch.basic_nack.assert_not_called()

        publish_kwargs = ch.basic_publish.call_args.kwargs
        assert publish_kwargs["routing_key"] == "files.dlq"

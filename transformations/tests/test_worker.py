import json
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import ANY, MagicMock, patch
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
    def _download(bucket, key, fileobj):
        fileobj.write(raw)
    s3.download_fileobj.side_effect = _download
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
            "file_key", "measurement", "source_id", "event_ts", "date",
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

        s3.download_fileobj.assert_called_once_with("other-bucket", "ship-01/f.json", ANY)

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
        assert publish_kwargs["mandatory"] is True
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
        assert publish_kwargs["mandatory"] is True


# ── T4: Worker crash / retry ──────────────────────────────────────────────────

class TestCrashRecovery:
    def test_transient_failure_eventually_succeeds(self):
        body = json.dumps({"file_key": "ship-01/f.json", "size": 100}).encode()
        cb, ch = _capture_callback(retry_max=3)

        call_count = [0]

        def flaky(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("transient")

        # First delivery (retry_count=0) → fails → nack + republish with count=1
        with patch("worker.worker.process", side_effect=flaky):
            cb(ch, _method(1), _props({"x-retry-count": 0}), body)

        ch.basic_nack.assert_called_once_with(delivery_tag=1, requeue=False)
        assert ch.basic_publish.call_args.kwargs["properties"].headers["x-retry-count"] == 1
        ch.basic_ack.assert_not_called()
        ch.reset_mock()

        # Second delivery (retry_count=1) → succeeds → ack, nothing republished
        with patch("worker.worker.process", side_effect=flaky):
            cb(ch, _method(2), _props({"x-retry-count": 1}), body)

        ch.basic_ack.assert_called_once_with(delivery_tag=2)
        ch.basic_nack.assert_not_called()
        ch.basic_publish.assert_not_called()


# ── T5: Duplicate delivery ────────────────────────────────────────────────────

class TestIdempotency:
    def test_duplicate_delivery_no_duplicate_rows(self, spark, tmp_path):
        raw = _raw_json(10)
        s3 = _mock_s3(raw)
        # Suppress the S3 delete so the file is still accessible on second delivery,
        # simulating a crash after the Delta write but before the ack.
        s3.delete_object = MagicMock()
        msg = {"file_key": "ship-01/f.json", "size": len(raw)}

        process(spark, s3, msg, str(tmp_path))  # first delivery
        process(spark, s3, msg, str(tmp_path))  # duplicate

        from delta.tables import DeltaTable
        df = DeltaTable.forPath(spark, str(tmp_path / "measurements")).toDF()
        assert df.count() == 10

    def test_distinct_file_keys_are_not_skipped(self, spark, tmp_path):
        for i in range(3):
            raw = _raw_json(5, source_id=f"ship-{i:02d}")
            process(spark, _mock_s3(raw), {"file_key": f"ship-{i:02d}/f.json", "size": len(raw)}, str(tmp_path))

        from delta.tables import DeltaTable
        assert DeltaTable.forPath(spark, str(tmp_path / "measurements")).toDF().count() == 15


# ── T6: Invalid / malformed file ─────────────────────────────────────────────

class TestInvalidFile:
    def test_malformed_json_skips_silently(self, spark, tmp_path):
        s3 = _mock_s3(b"not valid json {{{")
        process(spark, s3, {"file_key": "ship-01/bad.json", "size": 100}, str(tmp_path))
        s3.delete_object.assert_called_once()

    def test_wrong_schema_drops_silently(self, spark, tmp_path):
        raw = json.dumps([{"unrelated_field": 1, "other": 2}]).encode()
        s3 = _mock_s3(raw)
        process(spark, s3, {"file_key": "ship-01/bad.json", "size": len(raw)}, str(tmp_path))
        s3.delete_object.assert_called_once()

    def test_empty_file_is_silent_noop(self, spark, tmp_path):
        s3 = _mock_s3(b"[]")
        # Must not raise — empty file is skipped, not retried or sent to DLQ
        process(spark, s3, {"file_key": "ship-01/empty.json", "size": 0}, str(tmp_path))
        s3.delete_object.assert_called_once()

    def test_pipeline_continues_after_invalid_file(self, spark, tmp_path):
        # Bad file is skipped silently; the next valid file must still land correctly
        process(spark, _mock_s3(b"not valid json"), {"file_key": "ship-01/bad.json", "size": 10}, str(tmp_path))

        # Valid file after the bad one must land correctly
        raw = _raw_json(5)
        process(spark, _mock_s3(raw), {"file_key": "ship-01/good.json", "size": len(raw)}, str(tmp_path))

        from delta.tables import DeltaTable
        df = DeltaTable.forPath(spark, str(tmp_path / "measurements")).toDF()
        assert df.count() == 5

    def test_invalid_file_routes_to_dlq_after_max_retries(self):
        body = json.dumps({"file_key": "ship-01/bad.json", "size": 100}).encode()
        cb, ch = _capture_callback(retry_max=3)

        with patch("worker.worker.process", side_effect=Exception("parse error")):
            for retry in range(4):  # retry_count 0 → 3
                headers = {"x-retry-count": retry} if retry else {}
                cb(ch, _method(retry), _props(headers), body)

        dlq_calls = [
            c for c in ch.basic_publish.call_args_list
            if c.kwargs.get("routing_key") == "files.dlq"
        ]
        assert len(dlq_calls) == 1
        assert "x-error" in dlq_calls[0].kwargs["properties"].headers


# ── T7: Late arrival ──────────────────────────────────────────────────────────

def _raw_json_at_time(rows: int, source_id: str, base_ts: datetime) -> bytes:
    records = [
        {
            "source_id": source_id,
            "measurement": "gps",
            "timestamp": (base_ts + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "values": {"lat": 51.5 + i * 0.01, "lon": -0.1, "speed_knots": 10.0},
        }
        for i in range(rows)
    ]
    return json.dumps(records).encode()


class TestMessageSafety:
    def test_consumer_enables_publisher_confirms(self):
        _, ch = _capture_callback()
        ch.confirm_delivery.assert_called_once()

    def test_retry_publish_failure_requeues_original(self):
        """Publish failure during retry must not lose the original message."""
        body = json.dumps({"file_key": "ship-01/f.json", "size": 100}).encode()
        cb, ch = _capture_callback(retry_max=3)
        ch.basic_publish.side_effect = Exception("broker down")

        with patch("worker.worker.process", side_effect=RuntimeError("processing error")):
            cb(ch, _method(1), _props({"x-retry-count": 0}), body)

        ch.basic_ack.assert_not_called()
        ch.basic_nack.assert_called_once_with(delivery_tag=1, requeue=True)

    def test_dlq_publish_failure_requeues_original(self):
        """Publish failure during DLQ routing must not lose the original message."""
        body = json.dumps({"file_key": "ship-01/f.json", "size": 100}).encode()
        cb, ch = _capture_callback(retry_max=3)
        ch.basic_publish.side_effect = Exception("broker down")

        with patch("worker.worker.process", side_effect=RuntimeError("permanent error")):
            cb(ch, _method(1), _props({"x-retry-count": 3}), body)

        ch.basic_ack.assert_not_called()
        ch.basic_nack.assert_called_once_with(delivery_tag=1, requeue=True)


class TestLateArrival:
    def test_late_file_lands_with_original_event_ts(self, spark, tmp_path):
        now = datetime.now(timezone.utc)
        week_ago = now - timedelta(days=7)

        # Process a current file first
        raw_now = _raw_json_at_time(5, "ship-current", now)
        process(spark, _mock_s3(raw_now), {"file_key": "ship-current/f.json", "size": len(raw_now)}, str(tmp_path))

        # Then a late file with timestamps from last week
        raw_old = _raw_json_at_time(5, "ship-late", week_ago)
        process(spark, _mock_s3(raw_old), {"file_key": "ship-late/f.json", "size": len(raw_old)}, str(tmp_path))

        from delta.tables import DeltaTable
        df = DeltaTable.forPath(spark, str(tmp_path / "measurements")).toDF()

        assert df.count() == 10

        late_rows = df.filter(df.source_id == "ship-late").collect()
        assert len(late_rows) == 5
        for row in late_rows:
            # event_ts must carry the original timestamp, not today's date
            assert row.event_ts.date() == week_ago.date()
            # recorded_at is ingestion time and must be recent
            ingested_at = row.recorded_at.replace(tzinfo=timezone.utc)
            assert (now - ingested_at).total_seconds() < 60

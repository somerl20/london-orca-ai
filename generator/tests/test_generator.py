import json
from unittest.mock import MagicMock, patch
import pytest

from generator.generator import (
    _gps,
    _build_file,
    _metadata_from_key,
    _publish_confirmed,
    _publish_metadata,
    _reconcile_orphaned_files,
    _wait_for_s3,
    _wait_for_rabbitmq,
    ROWS_BY_SIZE,
)


# ── _gps() ────────────────────────────────────────────────────────────────────

class TestGps:
    def test_lat_in_range(self):
        for _ in range(200):
            assert -90.0 <= _gps()["lat"] <= 90.0

    def test_lon_in_range(self):
        for _ in range(200):
            assert -180.0 <= _gps()["lon"] <= 180.0

    def test_speed_in_range(self):
        for _ in range(200):
            assert 0.0 <= _gps()["speed_knots"] <= 25.0

    def test_all_keys_present(self):
        assert set(_gps()) == {"lat", "lon", "speed_knots"}


# ── _build_file() ─────────────────────────────────────────────────────────────

class TestBuildFile:
    def test_returns_bytes_and_string(self):
        data, name = _build_file("ship-01", "gps", 5)
        assert isinstance(data, bytes)
        assert isinstance(name, str)

    def test_filename_convention(self):
        _, name = _build_file("ship-01", "gps", 5)
        assert name.startswith("ship-01_gps_")
        assert name.endswith(".json")

    def test_valid_json_array(self):
        data, _ = _build_file("ship-01", "gps", 5)
        assert isinstance(json.loads(data), list)

    @pytest.mark.parametrize("count", [1, 100, 500])
    def test_exact_row_count(self, count):
        data, _ = _build_file("ship-01", "gps", count)
        assert len(json.loads(data)) == count

    def test_record_schema(self):
        data, _ = _build_file("ship-42", "gps", 1)
        record = json.loads(data)[0]
        assert record["source_id"] == "ship-42"
        assert record["measurement"] == "gps"
        assert "timestamp" in record
        assert {"lat", "lon", "speed_knots"} <= set(record["values"])

    def test_timestamps_strictly_increasing(self):
        data, _ = _build_file("ship-01", "gps", 10)
        timestamps = [r["timestamp"] for r in json.loads(data)]
        assert timestamps == sorted(timestamps)
        assert len(set(timestamps)) == len(timestamps)

    def test_rows_by_size_constants(self):
        assert ROWS_BY_SIZE["small"] == 100
        assert ROWS_BY_SIZE["medium"] == 30_000
        assert ROWS_BY_SIZE["large"] == 250_000


# ── _wait_for_s3() ────────────────────────────────────────────────────────────

class TestWaitForS3:
    def test_raises_after_deadline(self):
        with patch("generator.generator._make_s3", side_effect=OSError("no s3")):
            with pytest.raises(RuntimeError, match="S3 not reachable"):
                _wait_for_s3("telemetry", max_wait=0)

    def test_returns_client_immediately_on_success(self):
        mock_client = MagicMock()
        with patch("generator.generator._make_s3", return_value=mock_client):
            with patch("generator.generator._ensure_bucket"):
                result = _wait_for_s3("telemetry", max_wait=10)
        assert result is mock_client

    def test_retries_before_success(self):
        mock_client = MagicMock()
        attempts = [0]

        def flaky_s3():
            attempts[0] += 1
            if attempts[0] < 3:
                raise OSError("transient")
            return mock_client

        with patch("generator.generator._make_s3", side_effect=flaky_s3):
            with patch("generator.generator._ensure_bucket"):
                with patch("time.sleep"):
                    result = _wait_for_s3("telemetry", max_wait=60)

        assert result is mock_client
        assert attempts[0] == 3


# ── _wait_for_rabbitmq() ──────────────────────────────────────────────────────

class TestWaitForRabbitMQ:
    def test_raises_after_deadline(self):
        with patch("pika.BlockingConnection", side_effect=OSError("no rmq")):
            with pytest.raises(RuntimeError, match="RabbitMQ not reachable"):
                _wait_for_rabbitmq("amqp://localhost/", max_wait=0)

    def test_returns_connection_on_success(self):
        mock_conn = MagicMock()
        with patch("pika.BlockingConnection", return_value=mock_conn):
            result = _wait_for_rabbitmq("amqp://localhost/", max_wait=10)
        assert result is mock_conn


# ── Publish / reconcile ───────────────────────────────────────────────────────

class TestPublishMetadata:
    def test_publish_uses_publisher_confirms(self):
        ch = MagicMock()

        _publish_confirmed(ch, "files", "{}")

        ch.basic_publish.assert_called_once()
        kwargs = ch.basic_publish.call_args.kwargs
        assert kwargs["routing_key"] == "files"
        assert kwargs["mandatory"] is True
        assert kwargs["properties"].delivery_mode == 2

    def test_publish_raises_when_broker_nacks(self):
        ch = MagicMock()
        ch.basic_publish.return_value = False

        with pytest.raises(RuntimeError, match="did not confirm"):
            _publish_confirmed(ch, "files", "{}")

    def test_publish_metadata_retries_before_success(self):
        ch = MagicMock()
        attempts = [0]

        def flaky_publish(*args, **kwargs):
            attempts[0] += 1
            if attempts[0] < 2:
                raise OSError("transient")

        with (
            patch("generator.generator._publish_confirmed", side_effect=flaky_publish),
            patch("time.sleep"),
        ):
            _publish_metadata(ch, "files", "ship-01/f.json", "ship-01", "gps", 123)

        assert attempts[0] == 2

    def test_metadata_recovered_from_s3_key(self):
        assert _metadata_from_key("ship-01/ship-01_gps_20260101T000000Z_abcd.json", 123) == (
            "ship-01",
            "gps",
            123,
        )

    def test_reconciler_republishes_existing_objects(self):
        s3 = MagicMock()
        paginator = MagicMock()
        paginator.paginate.return_value = [{
            "Contents": [{
                "Key": "ship-01/ship-01_gps_20260101T000000Z_abcd.json",
                "Size": 123,
            }]
        }]
        s3.get_paginator.return_value = paginator
        ch = MagicMock()

        with patch("generator.generator._publish_metadata") as publish:
            _reconcile_orphaned_files(s3, ch, "telemetry", "files", "ship-01/")

        paginator.paginate.assert_called_once_with(Bucket="telemetry", Prefix="ship-01/")
        publish.assert_called_once_with(
            ch,
            "files",
            "ship-01/ship-01_gps_20260101T000000Z_abcd.json",
            "ship-01",
            "gps",
            123,
        )

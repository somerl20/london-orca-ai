# Testing Strategy

Three layers cover the pipeline from individual functions through to full broker-to-warehouse flows.

---

## Overview

| Layer | File(s) | Scope | Pre-commit | CI |
|---|---|---|---|---|
| Unit | `generator/tests/test_generator.py` | File building, data shapes | ✅ | ✅ |
| Unit | `transformations/tests/test_worker.py` | `process()`, `on_message` callback | ✅ | ✅ |
| E2E (mocked broker) | `tests/test_e2e.py` | Generator → S3 → worker → Delta | ✅ | ✅ |
| E2E (real broker) | `tests/test_ship_to_warehouse.py` | Full stack with real RabbitMQ | ❌ | ✅ |
| Resilience | `transformations/tests/test_worker.py` | Crash, duplicate, invalid, late arrival | ✅ | ✅ |
| Resilience | `tests/test_resilience.py` | Parallel ships, size tiers, burst, restart | ✅ / ❌ | ✅ |

Pre-commit runs `pytest -m "not slow"`.  Tests marked `slow` require Docker (real RabbitMQ via testcontainers) and run in CI only.

---

## Unit Tests

### Generator — `generator/tests/test_generator.py`

Verifies the file-building logic in isolation, with no infrastructure dependencies.

| Test | What it checks |
|---|---|
| `test_filename_format` | Filename includes `source_id`, measurement, and UTC timestamp |
| `test_row_count` | Output contains exactly the requested number of records |
| `test_record_shape` | Each record has `source_id`, `measurement`, `timestamp`, `values` |
| `test_gps_values_in_range` | `lat` ∈ [-90, 90], `lon` ∈ [-180, 180], `speed_knots` ≥ 0 |
| `test_output_is_valid_json` | Bytes decode to a valid JSON array |

### Worker — `transformations/tests/test_worker.py`

#### `TestProcess` — the `process()` function with real Spark, mocked S3

| Test | What it checks |
|---|---|
| `test_output_schema` | Delta table has the eight expected columns |
| `test_row_count` | All rows from the source file land in Delta |
| `test_values_correctly_mapped` | GPS coordinates and speed survive the transformation |
| `test_s3_file_deleted_after_write` | Raw file is deleted from S3 after a confirmed write |
| `test_source_bucket_from_message_overrides_env` | `source_bucket` in the message takes precedence over `S3_BUCKET` env var |
| `test_appends_across_multiple_batches` | Three sequential files accumulate correctly (no overwrites) |

#### `TestOnMessage` — the RabbitMQ callback routing logic

| Test | What it checks |
|---|---|
| `test_success_acks` | Successful `process()` → `basic_ack` |
| `test_first_failure_nacks_and_requeues` | First failure → `basic_nack`, message republished with `x-retry-count: 1` |
| `test_max_retries_routes_to_dlq` | `retry_count >= RETRY_MAX` → `basic_ack` + publish to DLQ |

---

## E2E Tests

### Mocked broker — `tests/test_e2e.py`

S3 is mocked with moto. The RabbitMQ publish/consume step is bypassed by constructing the message payload directly — the broker is infrastructure; the data path is what is verified.

| Test | What it checks |
|---|---|
| `test_data_reaches_delta_lake` | File built by generator lands in Delta with correct row count and schema |
| `test_raw_file_deleted_from_s3_after_processing` | S3 object is gone after the worker completes |
| `test_gps_values_survive_round_trip` | Latitude values in the source JSON match what is in Delta |

### Real broker — `tests/test_ship_to_warehouse.py` `slow`

Spins up a real RabbitMQ container via testcontainers.  Exercises the full path: generator uploads to S3, publishes to RabbitMQ, worker consumes and writes to Delta.

| Test | What it checks |
|---|---|
| `test_ship_event_reaches_warehouse` | Message published to a real broker is consumed and all rows land in Delta |

---

## Resilience Tests

Designed for the scenarios where the system must survive unexpected conditions at scale.  All assertions target data correctness and pipeline continuity, not just the happy path.

### Worker resilience — `transformations/tests/test_worker.py`

#### `TestCrashRecovery` (T4)

| Test | Input / action | Pass condition |
|---|---|---|
| `test_transient_failure_eventually_succeeds` | `process()` raises on attempt 1, succeeds on attempt 2 | First delivery nacked with `x-retry-count: 1`; second delivery acked, no DLQ publish |

#### `TestIdempotency` (T5)

S3 delete is suppressed so the file remains accessible on the second delivery, simulating a crash after the Delta write but before the ack.

| Test | Input / action | Pass condition |
|---|---|---|
| `test_duplicate_delivery_no_duplicate_rows` | Same message delivered twice | Delta contains 10 rows, not 20 |
| `test_distinct_file_keys_are_not_skipped` | Three files with different keys processed sequentially | All 15 rows present — idempotency guard does not skip legitimate new files |

**Production code change required:** `process()` now maintains a `_processed_files` Delta table. Before writing, it checks whether the `file_key` was already processed. After a successful write it records the key. Duplicate deliveries are silently skipped and the S3 file is cleaned up.

#### `TestInvalidFile` (T6)

| Test | Input / action | Pass condition |
|---|---|---|
| `test_malformed_json_raises` | `b"not valid json {{{"` | `process()` raises — message will be retried |
| `test_wrong_schema_raises` | Valid JSON but no expected columns | `process()` raises — message will be retried |
| `test_empty_file_is_silent_noop` | `b"[]"` | No exception raised; S3 file deleted; pipeline continues |
| `test_pipeline_continues_after_invalid_file` | Bad file then valid file | Valid file lands correctly after the bad one fails |
| `test_invalid_file_routes_to_dlq_after_max_retries` | `process()` raises on all 4 attempts | One publish to `files.dlq` with `x-error` header populated |

#### `TestLateArrival` (T7)

| Test | Input / action | Pass condition |
|---|---|---|
| `test_late_file_lands_with_original_event_ts` | Current file processed, then a file with timestamps 7 days old | Late rows appear with `event_ts.date() == week_ago.date()`; `recorded_at` reflects ingestion time |

### Integration resilience — `tests/test_resilience.py`

#### `TestParallelShips` (T1)

| Test | Input / action | Pass condition |
|---|---|---|
| `test_all_ships_rows_land_without_loss` | 10 ships, 100 rows each, processed sequentially through one worker | `count == 1 000`; 10 distinct `source_id` values in Delta |

> In production each worker is an independent JVM container consuming one message at a time.  Parallelism is at the container level.  This test verifies the correctness property: all ships' data accumulates in the shared Delta table without loss or cross-contamination.

#### `TestVariableFileSizes` (T2)

| Test | Input / action | Pass condition |
|---|---|---|
| `test_tier_completes_and_all_rows_land[small]` | 100-row file | All rows land |
| `test_tier_completes_and_all_rows_land[medium]` | 30 000-row file | All rows land |
| `test_tier_completes_and_all_rows_land[large]` | 250 000-row file | All rows land |
| `test_small_and_large_both_complete_concurrently` | Large and small file submitted together | Small file completes within 60 s regardless of large file progress |

#### `TestBurstyLoad` (T3) `slow`

| Test | Input / action | Pass condition |
|---|---|---|
| `test_queue_drains_and_no_files_dropped` | 20 messages published before worker starts | All 2 000 rows in Delta; RabbitMQ queue depth returns to 0; nothing in DLQ |

#### `TestRestartRecovery` (T8) `slow`

| Test | Input / action | Pass condition |
|---|---|---|
| `test_unacked_message_is_redelivered_after_connection_drop` | Worker 1 receives message then closes connection without acking | Worker 2 receives the redelivered message; all rows land in Delta |

---

## Running the Tests

```bash
# Fast suite — runs on every git commit
pytest -m "not slow"

# Full suite including Docker-based integration tests
pytest

# A single layer
pytest transformations/tests/test_worker.py
pytest tests/test_e2e.py
pytest tests/test_resilience.py
```

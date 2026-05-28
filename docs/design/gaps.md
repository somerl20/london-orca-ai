# Known Gaps

Gaps deliberately deferred given the time constraint. Each entry states what is missing, what the impact is, and what the fix looks like.

---

## 1. All exceptions treated as retryable

**What's missing:** The worker catches all exceptions in a single `except Exception` block and retries uniformly. Permanent failures — malformed JSON, missing `source_id`, wrong `values` shape — will burn through `RETRY_MAX` retries before reaching `files.dlq`, wasting time and queue capacity.

**Impact:** A structurally invalid file takes `RETRY_MAX × backoff` time to reach the DLQ instead of failing immediately. No data is lost — it ends up in `files.dlq` either way.

**Fix:** Classify exceptions into transient (network errors, SeaweedFS timeouts, Spark OOM) vs permanent (schema validation errors, JSON parse errors). Nack and route to DLQ immediately on permanent failures without retrying.

---

## 2. Non-atomic idempotency

**What's missing:** The idempotency check (`_already_processed`) and the Delta write are two separate operations. A worker crash between the Delta write completing and `_mark_processed` recording the file key leaves the row written but the file unmarked — the next redelivery writes a duplicate row.

**Impact:** Rare in practice (requires a crash in a narrow window after a successful write). Results in a duplicate row, not data loss. Detectable via `COUNT(*)` per `file_key`.

**Fix:** Wrap both operations in a single Delta transaction — record the data rows and the processed file marker atomically. Delta supports multi-table transactions; this eliminates the crash window entirely.

---

## 3. No autoscaling

**What's missing:** Workers must be scaled manually (`docker-compose up --scale worker=N`). During bursty load the queue depth grows but nothing spins up additional workers automatically.

**Impact:** Bursty loads increase end-to-end latency. The pipeline does not drop data — RabbitMQ buffers indefinitely — but processing lag grows until a human intervenes.

**Fix:** Kubernetes HPA keyed on `rabbitmq_queue_messages` depth. When the queue grows above a threshold, Kubernetes spins up more worker pods; when it drains, it scales back down. See [design.md — Production at 500+ Sources](design.md#6-production-at-500-sources).

---

## 4. No custom worker metrics

**What's missing:** Prometheus is wired and scrapes RabbitMQ and SeaweedFS metrics. The worker emits no custom metrics — no per-ship throughput, no processing latency histogram, no DLQ rate, no S3 delete failures.

**Impact:** At 10 ships the RabbitMQ queue depth is a sufficient proxy for pipeline health. At 500+ ships a silent failure in one ship's stream is invisible without per-source metrics.

**Fix:** Instrument the worker with a Prometheus client — emit `rows_processed_total{source_id}`, `processing_latency_seconds`, `dlq_routed_total`, `s3_delete_failures_total`. Wire Grafana dashboards and alerts on queue depth growth and DLQ accumulation rate.

---

## 5. Schema evolution — no bronze/silver layer

**What's missing:** The worker projects only GPS fields (`lat`, `lon`, `speed_knots`). New measurement types (e.g. `engine`, `weather`) or field renames require a code change and a table migration.

**Impact:** The pipeline is brittle to upstream schema changes. An unknown `measurement` type is silently ignored rather than stored for later processing.

**Fix:** Introduce a bronze/silver architecture. Bronze accepts all raw data as-is (store the full JSON payload). Silver applies the typed schema with explicit projection and migration steps. New measurement types land in bronze without breaking the silver pipeline.

---

## 6. Large-file chunking

**What's missing:** Each file is loaded into PySpark in full. Very large files (hundreds of MB) are bounded only by the worker container's memory (`local[2]`, JVM heap).

**Impact:** Files beyond ~200 MB risk OOM on the default Docker memory allocation (6–8 GB shared across all containers). Current `large` size (~45 MB) is well within limits.

**Fix:** Stream-parse the JSON file in chunks before handing off to Spark, or increase Docker memory limits. In production, enforcing a maximum file size at the generator and splitting oversized payloads is the cleaner boundary.

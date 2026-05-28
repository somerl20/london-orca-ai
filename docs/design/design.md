# Design Document — Ship Telemetry Ingestion Pipeline

## 1. Pipeline Architecture

Three stages with clear boundaries:

**Stage 1 — Landing** — Ships upload JSON files to SeaweedFS. On success the generator publishes the S3 key to RabbitMQ. The file is the unit of work; the message is the trigger.

**Stage 2 — Transform** — Workers consume messages from RabbitMQ, download the file from SeaweedFS, validate the schema, transform with PySpark, and append to a partitioned Delta Lake table. The raw file is deleted and the message is acked only after a confirmed warehouse write. Failed messages are retried up to `RETRY_MAX` times then routed to `files.dlq`.

**Stage 3 — Serve** — The Delta table is exposed over JDBC via Spark Thrift Server. Prometheus scrapes RabbitMQ and SeaweedFS metrics.

---

## 2. Tool Choices

| Component | Tool | Rationale |
|---|---|---|
| **Queue** | RabbitMQ | Native DLQ, durable queues, at-least-once delivery — see [broker.md](broker.md) |
| **Object store** | SeaweedFS | S3-compatible, large file support, TTL lifecycle — see [storage.md](storage.md) |
| **Transform** | PySpark | Batch-natural, scales local → cluster with no code changes — see [transform.md](transform.md) |
| **Data store** | Delta Lake | ACID, schema evolution, Spark-native, no extra server — see [warehouse.md](warehouse.md), [clustering.md](clustering.md) |
| **Analytics** | Spark Thrift Server | JDBC access to Delta Lake for SQL queries |
| **Monitoring** | Prometheus | Scrapes RabbitMQ and SeaweedFS metrics out of the box |

---

## 3. Failure Handling

| Failure | Behaviour |
|---|---|
| Worker (PySpark) crashes mid-processing before sending ack | RabbitMQ detects the dropped connection and redelivers the message to the next available worker |
| Worker fails to download the file from SeaweedFS | Retries with backoff; `x-retry-count` header increments on each attempt; routed to `files.dlq` after `RETRY_MAX` failures |
| Downloaded file is malformed — e.g. invalid JSON, missing `source_id`, wrong `values` shape | Nacked and retried; after `RETRY_MAX` failures routed to `files.dlq` with `x-error` header describing the cause |
| RabbitMQ redelivers a message the worker already processed (e.g. crash after Delta write, before ack) | Worker checks `_processed_files` Delta table before writing; duplicate `file_key` is skipped and the raw S3 file deleted |
| Generator uploads file to SeaweedFS but crashes before publishing the key to RabbitMQ | File sits orphaned in SeaweedFS with no queue message to trigger processing; bucket TTL deletes it automatically |
| RabbitMQ container restarts | Queues are durable — messages survive the restart; workers reconnect automatically via pika retry logic |

---



## 4. Focus Areas

**Reliable message delivery over raw throughput** — RabbitMQ with durable queues, explicit acks, retry counting, and DLQ. A message is never silently lost — it either lands in the warehouse or lands in `files.dlq` with a reason.

**Idempotent writes** — The `_processed_files` guard means the pipeline survives crashes, redeliveries, and restarts without producing duplicate rows. This is the hardest correctness property to get right in an at-least-once system.

**Error isolation at every stage** — Each failure mode has its own explicit path rather than a generic catch-all. The distinction between transient failures (retried) and worker process crashes (RabbitMQ handles redelivery) was consciously designed.

**Test coverage as a safety net** — Unit, E2E, and resilience tests covering crash recovery, duplicates, schema errors, and bursty load — so the correctness claims are verified, not just asserted (see [testing.md](../testing.md)).

What was deliberately left lighter: observability (Prometheus wired but no custom worker metrics), schema evolution (only GPS fields projected), and large-file chunking (see [gaps.md](gaps.md)).

---

## 5. Trade-offs

**Delta Lake over Iceberg** — Delta needed zero extra infrastructure (no catalog server). The cost is tight Spark coupling. For this scope that's acceptable; for a multi-engine production system Iceberg would be the right call.

**Spark `local[2]` per container instead of cluster mode** — Correct for the file sizes given. Cluster mode would require external infrastructure that adds no value here.

**All exceptions treated as retryable** — The worker retries everything including permanent failures like malformed JSON. The right fix is to distinguish transient vs permanent errors and skip retries for the latter. Deferred because it requires a more careful exception taxonomy in the code.

**Check-then-write idempotency instead of an atomic transaction** — The `_processed_files` check and the Delta write are two separate operations. A crash between them leaves an inconsistency. The correct fix is a single Delta transaction recording both. Deferred — meaningful complexity for an edge case that is rare in practice.

**No Grafana dashboards, no custom worker metrics** — Infrastructure is fully wired (Prometheus, RabbitMQ exporter). Emitting custom worker metrics and building dashboards would have consumed the remaining time budget without improving pipeline correctness.

---

## 6. Production at 500+ Sources

**Spark → Databricks** — Replace `local[2]` with Databricks (managed Spark). You get auto-scaling, cluster management, and native Delta Lake support with no ops overhead. For teams that don't want to manage Spark infrastructure, Databricks is the pragmatic call. Alternative: self-hosted Spark cluster on Kubernetes/YARN — no code changes required, only the `master("local[2]")` config switches to the cluster URL.

**Delta Lake → stay on Delta (Databricks) or migrate to Iceberg** — If moving to Databricks, Delta Lake remains the natural choice. If the org needs multiple compute engines (Trino, Flink, Spark), migrate to Apache Iceberg with a REST catalog — decouples the table format from the engine so any engine can read and write without going through Spark. Snowflake is a valid alternative if the org already uses it, but comes with per-query cost and storage lock-in.

**SeaweedFS → AWS S3** (or GCS / Azure Blob) — SeaweedFS is the self-hosted substitute used here due to the no-paid-services constraint. In production, replace it with a managed object store. Managed durability, no ops, native TTL lifecycle policies, and scales transparently with 500+ concurrent ship uploads.

**RabbitMQ** — Move from a single broker to a cluster with quorum queues. Quorum queues replicate messages across nodes so a single broker failure doesn't lose in-flight messages or block producers.

**Worker scaling** — Kubernetes HPA keyed on `rabbitmq_queue_messages` depth. When the queue grows, Kubernetes spins up more worker pods automatically; when it drains, it scales back down.

**Idempotency** — Replace the current check-then-write pattern with a single Delta/Iceberg transaction that records both the data rows and the processed file marker atomically. Eliminates the crash window between the two writes.

**Schema evolution** — Introduce a bronze/silver layer. Bronze accepts all raw data as-is. Silver applies the typed schema with explicit migration steps. New measurement types or field changes land in bronze first without breaking the silver pipeline.

**Observability** — Emit structured metrics from the worker (rows/s, processing latency, DLQ rate, S3 delete failures). Set alerts on queue depth growth, DLQ accumulation, and worker crash loops. At 500+ sources a silent failure in one ship's stream is invisible without per-source metrics.

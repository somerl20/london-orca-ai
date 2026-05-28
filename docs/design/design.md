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
| **Data store** | Delta Lake | ACID, schema evolution, Spark-native, no extra server — see [warehouse.md](warehouse.md) |
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



## 6. Production at 500+ Sources

| Area | Change |
|---|---|
| **Spark** | Cluster mode on Kubernetes/YARN — separate driver + executor nodes, no code changes needed |
| **Delta Lake → Iceberg** | Engine portability + proper catalog (Hive Metastore or REST) for clean write/read separation |
| **RabbitMQ** | Clustered with quorum queues for HA |
| **SeaweedFS** | Distributed mode with multiple volume servers for parallel write throughput |
| **Worker scaling** | Kubernetes HPA keyed on `rabbitmq_queue_messages` depth |
| **Idempotency** | Single Delta transaction recording data and processed-file marker atomically |
| **Schema evolution** | Bronze/silver layer — bronze accepts all raw data, silver enforces the typed schema |
| **Observability** | Structured worker metrics (rows/s, lag, DLQ rate), alerts on queue depth and DLQ growth |

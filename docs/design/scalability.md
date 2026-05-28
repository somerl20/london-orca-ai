# Scalability & Reliability Analysis

> "The most critical aspect is designing for parallel processing and large scale, the pipeline must handle many concurrent ships, variable file sizes, and bursty loads while guaranteeing that no data is lost."

---

## Summary

| Dimension | Status | Gap |
|---|---|---|
| Parallel processing | ✅ Solid | — |
| Variable file sizes | ✅ Solid | `local[2]` ceiling per worker |
| Bursty loads | ⚠️ Buffered, manually scaled | No autoscaling |
| No data loss | ✅ Solid | Non-atomic idempotency edge case |

---

## Parallel Processing

Workers consume from RabbitMQ independently — no shared state except the Delta Lake table. Each worker runs its own PySpark session and writes concurrently. Delta's optimistic concurrency control handles simultaneous appends safely.

Scale horizontally at any time:

```bash
docker-compose up --scale worker=5 --no-recreate
```

In production: Kubernetes HPA keyed on `rabbitmq_queue_messages` depth spins workers up and down automatically — see [design.md](design.md#6-production-at-500-sources).

See also: [broker.md](broker.md) — queue configuration, worker prefetch.

---

## Variable File Sizes

The worker adapts Spark parallelism dynamically based on the row count of each file:

```python
n_partitions = max(1, row_count // 50_000)  # ~50K rows per Spark partition, min 1
```

Small files (~15 KB) coalesce to a single partition. Large files (~45 MB) fan out across multiple partitions. The same code path handles all sizes without configuration changes.

**Ceiling:** Each worker runs `local[2]` — two Spark executor cores per container. Large files are processed correctly but more slowly than in cluster mode. For production at scale, replace `local[2]` with a Databricks or Spark cluster — see [transform.md](transform.md) and [design.md](design.md#6-production-at-500-sources).

---

## Bursty Loads

RabbitMQ acts as the buffer. When ships dump files simultaneously, messages queue up and workers drain them at their own pace — the producer is never blocked. Queue depth is visible in the RabbitMQ management UI at `http://localhost:15672`.

**Gap:** Workers must be scaled manually. During a burst the queue grows but nothing automatically spins up new workers. In production this would be Kubernetes HPA on `rabbitmq_queue_messages` — buffering is handled, autoscaling is not yet wired.

---

## No Data Loss

Four layers guarantee every file either reaches the warehouse or is explicitly accounted for:

| Layer | Mechanism |
|---|---|
| At-least-once delivery | Workers send an explicit ack only after a confirmed Delta write; RabbitMQ redelivers on worker crash |
| Retry with DLQ | Failed messages are retried up to `RETRY_MAX` times; exhausted messages go to `files.dlq` with an `x-error` header — never silently dropped |
| Idempotency guard | `_processed_files` Delta table prevents duplicate rows on redelivery |
| Durable queues | Messages survive RabbitMQ restarts; workers reconnect automatically |

**Known edge case:** The idempotency check (`_already_processed`) and the Delta write are two separate operations. A crash between them leaves an inconsistency — data is written but not marked processed, so the next redelivery writes a duplicate row. The correct fix is a single Delta transaction recording both atomically. Deferred — rare in practice, documented in [design.md](design.md#5-trade-offs).

See also: [broker.md](broker.md) — DLQ routing, retry headers.

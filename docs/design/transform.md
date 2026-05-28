# Transform Layer — Requirements & Decision

Consumes file references from the queue, reads raw JSON from the object store, transforms to columnar format, and writes to the data warehouse.

---

## Requirements

| # | Requirement |
|---|-------------|
| 1 | Consume messages from RabbitMQ |
| 2 | Read files from object store (S3-compatible) |
| 3 | Schema validation — detect and handle schema changes |
| 4 | Chunk large files — handle up to 45MB / 250K rows without full memory load |
| 5 | Transform JSON → columnar format |
| 6 | Write to data warehouse efficiently |
| 7 | Ack queue message only after confirmed warehouse write |
| 8 | Delete file from object store after confirmed write |
| 9 | Horizontally scalable — multiple parallel workers |
| 10 | Error handling — retry logic, forward to DLQ on failure |
| 11 | Resource efficient — runs on a Mac |
| 12 | Open source |

---

## Candidates

| Solution | Fits? | Notes |
|----------|-------|-------|
| **Apache Spark** | ✅ Chosen | Industry standard for distributed batch. PySpark API, local mode for dev, scales to cluster in production. |
| **Apache Flink** | ⚠️ | Best for true low-latency streaming — overkill for periodic file drops. PyFlink less mature than PySpark. |
| **Dask** | ⚠️ | Python-native parallel computing. Not as battle-tested as Spark at extreme scale. |
| **Python + Polars** | ⚠️ | Rust-backed, fast, lazy evaluation. Scales horizontally via containers only — no distributed execution within a job. |
| **Python + pandas** | ❌ | Single-threaded, eager loading, 5–10x memory overhead. Not production-grade. |
| **Apache Beam** | ❌ | Portable abstraction with no benefit at this scale. Adds complexity. |

---

## Spark vs Flink

| | **Apache Spark** | **Apache Flink** |
|---|---|---|
| **Processing model** | Batch-first, micro-batch for streaming | Stream-first, true real-time streaming |
| **Use case fit here** | ✅ Natural fit — periodic file drops are batch jobs | ⚠️ Overkill — designed for continuous low-latency streams |
| **Latency** | Seconds (micro-batch) | Milliseconds (true streaming) |
| **Python API** | ✅ PySpark — mature, widely used | ⚠️ PyFlink — exists but less mature |
| **Community** | Very large, huge ecosystem | Smaller but active |
| **Learning curve** | Steep but well-resourced | Steeper, less learning material |
| **Local dev mode** | ✅ `local[*]` — easy single-machine setup | ✅ Supported but more complex config |
| **Resource usage** | ~1–2GB RAM minimum | ~1–2GB RAM minimum |
| **Fault tolerance** | Strong (RDD lineage) | Strong (distributed snapshots) |
| **Exactly-once semantics** | ⚠️ Complex to guarantee | ✅ Built-in |
| **Production adoption** | Industry standard for batch | Industry standard for streaming |
| **JSON → Parquet** | ✅ Native | ✅ Supported |

---

## Decision: Apache Spark

Chosen for its natural fit with batch file processing, mature PySpark API, and strong production adoption at scale. Runs in `local[*]` mode for the assignment; switches to cluster mode for production with no code changes.

Flink ruled out — strengths (true streaming, millisecond latency, exactly-once) are not required for periodic file drops and come at the cost of added complexity. Pandas ruled out — not production-grade.

---

## Known Limitation: Transformation and Warehouse Write Share the Same Container

In the current setup, the transform worker runs Spark in `local[2]` mode and writes directly to Delta Lake within the same container. This means transformation (CPU-bound) and warehouse writes (I/O-bound) compete for the same resources.

**Why this is unavoidable here:** Delta Lake has no standalone write API — Spark is the writer. There is no way to split "transform" and "write to Delta" into separate containers without introducing an intermediate storage layer.

**Current mitigation:** The worker container is capped at 2 CPUs / 2GB RAM via Docker resource limits, leaving headroom for warehouse read consumers (Spark Thrift Server).

**Production recommendation:** Run Spark in cluster mode (separate driver + executor nodes). Each executor gets isolated CPU and memory. Writers and readers run on separate nodes, and the cluster scales independently of the warehouse query layer. The code requires no changes — only the `master("local[2]")` config switches to a cluster URL.

# Data Warehouse — Requirements & Decision

Target store for transformed columnar data — queryable, ACID-compliant, partitioned by source/date/measurement.

---

## Requirements

| # | Requirement |
|---|-------------|
| 1 | Accept columnar writes from Spark workers |
| 2 | Concurrent writes from multiple workers without corruption — ACID guarantees |
| 3 | Full SQL query support |
| 4 | Partitioning — by source, date, measurement |
| 5 | Schema evolution — handle new/changed fields gracefully |
| 6 | High read throughput for analytics queries |
| 7 | Resource efficient — runs on a Mac |
| 8 | Open source |
| 9 | Monitoring / query statistics |

---

## Candidates

| Solution | Fits? | Notes |
|----------|-------|-------|
| **Delta Lake** | ✅ Chosen | Open table format, native Spark integration, ACID, schema evolution, runs on local filesystem — no extra server needed |
| **Apache Iceberg** | ✅ | More portable open standard, ACID, schema evolution, but requires a catalog service — adds infra complexity |
| **ClickHouse** | ✅ | Dedicated columnar server, full SQL, built-in monitoring. Heavier (~500MB+ RAM), Spark connector less native than Delta |
| **DuckDB** | ⚠️ | Excellent for reads, very lightweight. Not designed for concurrent writes from multiple processes — race conditions with parallel workers |
| **Parquet + DuckDB** | ⚠️ | Simple but no ACID — concurrent writes from parallel workers risk corruption without a table format on top |
| **Apache Druid** | ❌ | Very heavy and complex to configure |
| **Apache Hive + HDFS** | ❌ | Too heavy, not suitable for Mac |

---

## Pros / Cons — Top 3

| | **Delta Lake** | **Apache Iceberg** | **ClickHouse** |
|---|---|---|---|
| **Pros** | Native Spark integration (just add a package) | True open standard — works with Spark, Flink, Trino, Dask | Dedicated columnar server — fastest analytical queries |
| | ACID transactions out of the box | ACID transactions | Built-in monitoring & web UI |
| | Schema evolution built-in | Schema evolution built-in | Full SQL, very expressive |
| | No extra server — runs on local filesystem | Highly portable across engines | High concurrent write throughput |
| | Time travel / data versioning | Time travel / data versioning | Battle-tested at production scale |
| | Lightweight — no infra overhead | Strong community & adoption growing fast | Strong ecosystem & connectors |
| **Cons** | Primarily a Databricks ecosystem tool | Requires catalog service (Hive metastore or REST) — extra infra | Heaviest option (~500MB+ RAM) |
| | **Tightly coupled to Spark — hard to replace either without the other** | More complex setup than Delta Lake | Separate container, more config |
| | Less portable to other engines | Steeper learning curve | Spark connector exists but less native than Delta |
| | | Smaller community than Delta Lake | Not a table format — harder to migrate storage layer later |

---

## Decision: Delta Lake

Chosen for its seamless Spark integration — ACID transactions, schema evolution, and partitioning with no additional infrastructure or server required.

**Acknowledged trade-off:** Delta Lake and Spark are tightly coupled. Replacing one effectively requires replacing the other. This is an accepted limitation for the current scope. For a production system requiring engine portability, Apache Iceberg would be the better long-term choice.

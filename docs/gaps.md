# Gaps To Complete

Prioritized against the home assignment requirements and likely production edge cases.

## P0 - Critical

### 1. README is not sufficient to run the pipeline

The assignment explicitly asks for a repository where someone unfamiliar with the choices can run the pipeline by following `README.md`. The current root README only contains the project name.

Complete:
- Add setup prerequisites: Docker, Docker Compose, Python version, available machine memory.
- Add `docker-compose up` commands for core, monitoring, analytics, and scaling workers.
- Add how to validate data reached the warehouse.
- Add how to run fast and full tests.
- Add service URLs and credentials for RabbitMQ, SeaweedFS, Prometheus, Grafana, Spark Thrift, and Metabase.

### 2. Invalid input files are silently deleted instead of retried or quarantined

The requirement says failures at every stage should be handled without data loss. In `worker.process()`, malformed JSON, wrong schema, and empty/invalid rows are logged, deleted from S3, and treated as success. The RabbitMQ message is then acked by the caller, so the source data is gone and there is no DLQ evidence.

Complete:
- Decide which cases are recoverable and which are permanent.
- For malformed JSON and schema mismatch, preserve the raw object or move it to a quarantine bucket/prefix.
- Publish a DLQ/error record with `file_key`, reason, and timestamp.
- Align tests and `docs/testing.md`; the docs currently describe DLQ behavior that the implementation does not fully enforce.

### 3. Large-file chunking is not really implemented

The assignment calls out chunking, batching, and variable file sizes. The generator builds the full JSON file in memory as a list of strings, and the worker downloads the whole object to a temp file, reads it as one multiline JSON array, caches it, and forces `count()`. This is acceptable for the sample 45 MB files, but it is not a true large-file strategy.

Complete:
- Make generator writes streaming in the Dockerized generator, like the provided helper file does.
- Avoid `multiline=true` array ingestion for large files, or split incoming arrays into newline-delimited JSON before Spark reads them.
- Remove unnecessary `.cache()` or explicitly unpersist.
- Add memory-pressure tests around large and bursty files.

### 4. Concurrent write and idempotency guarantees are incomplete

The assignment emphasizes concurrent ships and no data loss. Delta Lake helps, but the current idempotency registry uses a check-then-write pattern in `_already_processed()` / `_mark_processed()`, which is not atomic across multiple workers. A crash after the measurement merge but before `_mark_processed()` can also leave inconsistent processing state.

Complete:
- Make the processed-file marker transactional with the data write, or use a single Delta transaction pattern that records both data and file status.
- Add retries for Delta concurrent modification conflicts.
- Add tests with multiple independent worker processes writing to the same warehouse path.
- Add duplicate-delivery tests that simulate crashes after data write and before marker write.

## P1 - High

### 5. Schema evolution is documented, but only fixed GPS fields are supported

The worker only projects `values.lat`, `values.lon`, and `values.speed_knots`. New measurements or additional GPS fields are dropped, and schema changes are handled as skip/delete paths rather than structured evolution.

Complete:
- Define a schema evolution policy: permissive bronze table, strict silver table, or versioned schemas.
- Preserve raw records or raw JSON for replay.
- Add tests for extra fields, missing optional fields, renamed fields, and new measurement types.

### 6. Monitoring stack is mostly infrastructure, not pipeline observability

Prometheus scrapes RabbitMQ and SeaweedFS, but there are no worker/generator metrics, no Spark metrics target, and Grafana provisioning is empty. The assignment asks for production readiness and failure visibility.

Complete:
- Emit worker metrics: files processed, rows processed, failures, DLQ count, processing duration, delete failures.
- Emit generator metrics: files uploaded, publish failures, orphan reconciliation count.
- Add Grafana dashboards or provisioning docs.
- Add alerts for queue depth, no successful ingestion, DLQ growth, and worker crash loops.

### 7. Analytics path is not fully turnkey

Metabase and Spark Thrift are declared, but dashboards are not provisioned and the README does not explain connection setup. Spark Thrift table registration may also fail or be empty before the Delta table exists.

Complete:
- Document Metabase connection steps or provision them.
- Add a startup path that tolerates the measurements table not existing yet.
- Add a simple validation query in the README and/or script.

### 8. Reconciliation can flood duplicate messages

The generator periodically republishes metadata for every existing object under a ship prefix. If workers are slow, this can repeatedly enqueue duplicate messages for in-flight files.

Complete:
- Track reconciliation state, object age, or an in-flight marker.
- Reconcile only objects older than a safe threshold.
- Add metrics and tests for slow-worker reconciliation behavior.

## P2 - Medium

### 9. Assignment generator interface and Docker generator interface diverge

The assignment describes `OUTPUT-DIR`, `RATE`, `SIZE`, and `SOURCES` for local file drops. The repo's production generator writes directly to S3 and RabbitMQ. That is a reasonable design choice, but it needs explicit explanation.

Complete:
- Document why the generator was adapted from local files to S3 + RabbitMQ.
- Add local-mode instructions if preserving the assignment helper workflow matters.

### 10. Design documentation is spread across many files

The assignment asks for a 1-2 page design document. The repo has useful component docs, but no single concise design document that covers architecture, failure handling, trade-offs, and production changes in one place.

Complete:
- Add `docs/design.md` as the canonical 1-2 page summary.
- Link the deeper component docs from it.

### 11. Test suite overstates some production guarantees

Some tests use one Spark session, mocked S3, or separate warehouse paths, so they do not prove the same behavior as scaled Docker workers sharing one Delta warehouse.

Complete:
- Add a real Docker Compose integration test for scaled workers.
- Add tests for shared-warehouse concurrent writes.
- Add tests around broker restart and SeaweedFS restart, not only RabbitMQ redelivery.

### 12. Operational cleanup is only partially covered

The raw object is deleted after successful processing and bucket TTL is attempted, but processed-file metadata, Delta small files, old table versions, and quarantine retention are not covered.

Complete:
- Add Delta maintenance notes or jobs: `OPTIMIZE`, compaction, and `VACUUM`.
- Add retention policy for `_processed_files`.
- Add quarantine retention and replay procedure.

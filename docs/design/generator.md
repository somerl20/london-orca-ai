# Generator — Requirements

Simulate a fleet of ships independently uploading JSON sensor telemetry to an object store.

---

## Changes from Original

| # | Change | Detail |
|---|--------|--------|
| 1 | **Upload target** | Write file to object store, then publish a metadata message to the queue. |
| 2 | **Queue message** | Published only after a confirmed upload. Contains: `file_key`, `source_id`, `measurement`, `timestamp`, `size`. |
| 3 | **True parallelism** | Each ship runs as an independent concurrent worker — ships must not block each other. |
| 4 | **Bursty load** | Randomize rate and size tier per ship to simulate uneven real-world load. |
| 5 | **Startup readiness** | Retry/backoff until both object store and queue are reachable before starting. |
| 6 | **Graceful shutdown** | Handle `SIGTERM` — finish current upload + message, log total files produced, then exit. |
| 7 | **Dockerized** | Run as a container. All config via environment variables. |

---

## Known Edge Case

**File written, message not published** — if the generator crashes after a successful upload but before publishing the queue message, the file is orphaned in the object store and never processed.

This is an accepted trade-off. At a rate of one record per second, losing a small number of files on a crash is acceptable. The orphaned file will be cleaned up by the object store TTL.

---

## Configuration

| Variable      | Default  | Description                                                     |
|---------------|----------|-----------------------------------------------------------------|
| `SOURCES`     | `10`     | Number of ships                                                 |
| `RATE`        | `2`      | Base files per minute per ship                                  |
| `SIZE`        | `small`  | `small` (~15 KB) · `medium` (~5 MB) · `large` (~45 MB)         |
| `RATE_JITTER` | `0.3`    | Rate randomization fraction per ship                            |
| `SIZE_RANDOM` | `false`  | If `true`, each ship picks a random size tier                   |

---

## Out of Scope

Schema validation, deduplication, retry on processing failure — all belong to the worker.

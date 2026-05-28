# Delta Lake Clustering Keys

The `measurements` table uses Liquid Clustering on `(event_ts, source_id)`:

```python
.clusterBy("event_ts", "source_id")
```

Delta writes data into files whose min/max statistics are co-located so the engine can skip files that don't overlap a predicate. The leading key is `event_ts`, the secondary key is `source_id`.

---

## Liquid Clustering vs `partitionBy`

| | `partitionBy("source_id")` | `partitionBy("date")` | Liquid Clustering `(event_ts, source_id)` ✓ |
|---|---|---|---|
| File skipping | Directory-level, always guaranteed | Directory-level, always guaranteed | Stats-based, fully effective after `OPTIMIZE` |
| Small-files risk | High — 10 ships × frequent appends → many tiny files per partition | Low per partition, but hundreds of date directories over time | Low — no directory split, files consolidated by `OPTIMIZE` |
| Schema change cost | Full table rewrite to change partition key | Full table rewrite | Change cluster keys, apply incrementally via `OPTIMIZE` |
| Best for | High-cardinality, stable key, infrequent appends | Date-bucketed batch loads | Mixed access patterns, low-cardinality keys, streaming appends |
| **Why chosen** | — | — | Avoids small-files problem at 10 ships; supports both time-range and per-ship queries; cluster keys can evolve |

---

## Optimal Queries

| Query pattern | Why efficient |
|---|---|
| `WHERE event_ts BETWEEN t1 AND t2` | Leading cluster key — most files skipped |
| `WHERE event_ts BETWEEN t1 AND t2 AND source_id = 'ship-42'` | Both keys used — minimal files read |
| Merge / upsert on `source_id AND event_ts` | Matches the cluster keys exactly — Delta narrows the file search before conflict check |

---

## Inefficient Queries and How to Fix Them

| Query pattern | Why inefficient | Suggestion |
|---|---|---|
| `WHERE source_id = 'ship-42'` (no time range) | Secondary key only — file skipping less precise | Add a time range predicate; or run `OPTIMIZE` regularly to tighten co-location |
| `WHERE measurement = 'gps'` | Not a cluster key — only min/max stats skipping, ranges likely wide | Add `measurement` as a third cluster key if per-type queries become common |
| `WHERE latitude BETWEEN 30 AND 35` | Not a cluster key — min/max skipping exists but weak; lat/lon values scatter across all files so ranges overlap heavily | Cluster on `latitude` if geospatial queries are frequent; or maintain a geo-indexed table separately |
| `WHERE date = '2026-05-23'` | `date` is an explicit derived column, not a partition or cluster key — only min/max skipping | Rewrite as `WHERE event_ts BETWEEN '2026-05-23' AND '2026-05-24'` to use the leading cluster key. Note: Iceberg hidden partitioning handles this automatically via `PARTITIONED BY (day(event_ts))` — Delta Lake has no equivalent |

> **Note:** File skipping reaches full effectiveness only after `OPTIMIZE` consolidates files. Fresh appends carry accurate min/max stats per file but are not physically co-located until `OPTIMIZE` runs. A nightly `OPTIMIZE` job is the right fix in production.

# Requirements Proof

Verified by running [`scripts/prove_requirements.py`](../scripts/prove_requirements.py) against the live Docker pipeline.

## How to run

```bash
docker exec -i london-orca-ai-worker-1 python - < scripts/prove_requirements.py
```

## Results

| Requirement | Check | Result | Detail |
|---|---|---|---|
| **Latency ≤15 min** | p99 of `recorded_at − event_ts` for events generated in the last 20 min | **PASS** | p99=857.9s  avg=564.0s  recent_rows=106  threshold=900s |
| **Parallel processing** | Max distinct ships processed within the same clock-minute | **PASS** | max_ships_in_one_minute=10 |
| **Volume** | Total rows, distinct ships, distinct files in Delta | **PASS** | total_rows=8,200  distinct_ships=10  distinct_files=82 |
| **Data completeness** | `COUNT DISTINCT file_key` in measurements == registry | **PASS** | distinct_file_keys_in_measurements=82  distinct_file_keys_in_registry=82 |
| **Resource efficiency** | Average Parquet file size via `DESCRIBE DETAIL` | **PASS** | num_files=82  total_size=0.5MB  avg_file_size=5.8KB |
| **Schema validation** | Delta schema has exactly the expected columns, no extras | **PASS** | expected columns matched, extra=[]  missing=[] |

6/6 requirements verified.

## Notes

- **R6 (Error handling / DLQ)** is not observable from Delta Lake — failed messages route to RabbitMQ `files.dlq`, leaving no trace in Delta by design. This requirement is covered by `tests/test_resilience.py::TestRestartRecovery`.
- Latency is measured as **p99** over events generated in the last 20 minutes — this reflects steady-state performance and excludes cold-start outliers.
- Tests run with: 3 worker replicas, `RATE=0.2` files/min per ship, 10 ships, `SIZE=small`.

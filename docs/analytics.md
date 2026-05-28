# Analytics Dashboards

The pipeline ships with pre-built dashboards powered by [Rill](https://www.rilldata.com/) — a lightweight, open-source BI tool built on DuckDB. No login required.

## Accessing the Dashboard

Once the stack is running, open **http://localhost:3030** in your browser.

The **Fleet Operations Dashboard** loads automatically. It shows:

| Panel | What it shows |
|---|---|
| Total Events | How many telemetry records have been processed |
| Active Ships | Number of distinct ships seen in the selected period |
| Avg Fleet Speed | Mean speed across all ships (knots) |
| Max Speed | Peak speed recorded in the selected period |
| Time series | Event volume over time — reflects pipeline throughput |

## Filtering

Use the filter bar at the top to slice by:
- **Ship ID** (`source_id`) — isolate a single vessel
- **Measurement type** (`measurement`) — filter by sensor type
- **Date** — narrow the time window

## Notes

- The dashboard reads directly from the Delta Lake warehouse via DuckDB — no intermediate database needed.
- Data appears within ~2 minutes of the pipeline starting, once the first batch of files has been processed by the workers.
- The source is refreshed automatically whenever new data arrives.

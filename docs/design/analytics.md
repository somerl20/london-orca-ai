# Analytics & Dashboards — Requirements & Decision

Optional component. Business intelligence layer for querying and visualizing warehouse data.

---

## Requirements

| # | Requirement |
|---|-------------|
| 1 | Open source |
| 2 | Easy to implement |
| 3 | High refresh rate |
| 4 | Rich UI — statistics and charts from the warehouse |
| 5 | Editable filters, ranges, drill-downs |
| 6 | Native connection to Delta Lake / Spark SQL |
| 7 | SQL support — custom queries on top of the data |
| 8 | Shareable dashboards |
| 9 | Low resource footprint — runs on a Mac |
| 10 | Docker-compose friendly |

---

## Candidates

| Solution | Fits? | Notes |
|----------|-------|-------|
| **Metabase** | ✅ Chosen | Enterprise-grade UI, easy setup, great filtering, Docker-ready. Connects to Spark SQL via JDBC (Spark Thrift Server). Community Edition is free and open source. |
| **Apache Superset** | ❌ | Feature-complete but UI is poor. Heavier (~500MB+). |
| **Redash** | ⚠️ | SQL-focused, simpler. Acceptable UI but less polished than Metabase. |
| **Grafana** | ⚠️ | Already in stack for monitoring. Not designed for BI-style dashboards. |

---

## Decision: Metabase (Community Edition)

Chosen for its polished UI, ease of setup, and enterprise-grade dashboard capabilities — all free in the Community Edition.

**Connection to Delta Lake:** Metabase connects via Spark Thrift Server (JDBC), which exposes Delta Lake tables as standard SQL. No custom connectors needed.

**Note:** Delta Lake connection requires Spark Thrift Server to be running as a query endpoint. This is an additional lightweight service but requires no code changes to the existing Spark setup.

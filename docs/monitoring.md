# Monitoring — Requirements & Decision

Optional component. Observability layer for pipeline health, throughput, and alerting.

---

## Requirements

| # | Requirement |
|---|-------------|
| 1 | Open source |
| 2 | High refresh rate — near real-time metrics |
| 3 | Built-in UI |
| 4 | Alerting on no data / bottlenecks — email or other channels |
| 5 | Pipeline-specific metrics — queue depth, consumer lag, files processed/failed, processing time, worker throughput |
| 6 | Low resource footprint — runs alongside all other containers on a Mac |
| 7 | Easy integration — existing exporters for RabbitMQ, SeaweedFS, Spark |
| 8 | Log aggregation — centralized logs from all containers |
| 9 | Historical retention — configurable metrics window |
| 10 | Docker-compose friendly — minimal config |

---

## Decision: Prometheus + Grafana

Two containers. Covers all requirements with minimal setup.

| Component | Role |
|-----------|------|
| **Prometheus** | Scrapes metrics from all pipeline components. RabbitMQ, SeaweedFS, and Spark all expose Prometheus endpoints natively — zero custom code. |
| **Grafana** | UI + alerting in one. Community dashboards for RabbitMQ and Spark available out of the box. Alerting (email/Slack) built-in — no separate Alertmanager needed. |

**Ruled out:**
- **Netdata** — simpler config but cannot surface pipeline-specific metrics (queue depth, files processed)
- **Grafana Loki** — useful for log aggregation but adds a third container; out of scope for optional component
- **Alertmanager** — redundant, Grafana handles alerting natively at this scale

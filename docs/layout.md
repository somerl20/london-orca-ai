# Repository Layout

```
london-orca-ai/
├── generator/              # Ship simulator (Python + Dockerfile)
│   └── src/generator/
├── transformations/        # PySpark transform worker (Python + Dockerfile)
│   └── src/worker/
├── warehouse/
│   └── spark-thrift/       # Spark Thrift Server Dockerfile
├── analytics/
│   └── queries/            # Sample SQL queries for Delta Lake
├── monitoring/
│   ├── prometheus/         # prometheus.yml scrape config
│   └── grafana/            # Provisioning + dashboards
├── datalake/
│   └── seaweedfs/          # SeaweedFS S3 bucket config
├── infra/
│   └── compose/            # Modular Compose files (core / monitoring / analytics)
├── scripts/
│   └── query_measurements.py  # Ad-hoc Delta Lake query script
├── tests/                  # Test suite (unit + resilience + e2e)
├── docs/                   # Component design documents
└── docker-compose.yml      # Root convenience Compose file (profiles)
```

# Ship Telemetry Ingestion Pipeline

A production-grade data pipeline that ingests GPS sensor telemetry from a fleet of ships, transforms JSON payloads into columnar Delta Lake format, and exposes the data for analytics.

## Architecture

[![E2E pipeline diagram](docs/diagram/e2e.svg)](docs/diagram/pipeline.html)

**Pipeline stages:**
1. **Generator** — simulates ships by writing JSON files to SeaweedFS and publishing the S3 key to RabbitMQ.
2. **Worker** — consumes messages from RabbitMQ, downloads the file from SeaweedFS, validates the schema, transforms with PySpark, appends to a partitioned Delta Lake table, then deletes the raw file. Failed messages are retried up to `RETRY_MAX` times before being routed to a dead-letter queue (`files.dlq`).
3. **Delta Lake** — columnar, ACID-compliant storage. Partitioned by `source_id` for efficient per-ship queries.
4. **Analytics** — Spark Thrift Server exposes the Delta table over JDBC; Metabase connects for visual exploration.
5. **Monitoring** — Prometheus scrapes RabbitMQ and worker metrics; Grafana visualises pipeline health.

---

## Prerequisites

### macOS

| Dependency | Minimum version | Install |
|---|---|---|
| Docker Desktop | 4.x | [docs.docker.com/desktop/mac](https://docs.docker.com/desktop/mac/install/) |
| Docker Compose | v2 (bundled) | Included with Docker Desktop |
| Python | 3.10+ | `brew install python` or [python.org](https://python.org) |
| Git | any | `brew install git` |

**Docker Desktop resource settings** (required for the worker's Spark JVM):

1. Open Docker Desktop → Settings → Resources
2. Set **Memory** to at least **6 GB** (8 GB recommended)
3. Set **CPUs** to at least **4**
4. Click **Apply & Restart**

### Windows

| Dependency | Minimum version | Install |
|---|---|---|
| Docker Desktop | 4.x (WSL2 backend) | [docs.docker.com/desktop/windows](https://docs.docker.com/desktop/windows/install/) |
| WSL2 | - | `wsl --install` in PowerShell (Admin) |
| Docker Compose | v2 (bundled) | Included with Docker Desktop |
| Python | 3.10+ | [python.org/downloads](https://www.python.org/downloads/) or `winget install Python.Python.3.12` |
| Git | any | [git-scm.com](https://git-scm.com/download/win) or `winget install Git.Git` |

**WSL2 memory limit** — by default WSL2 can consume all available RAM. To cap it, create or edit `%USERPROFILE%\.wslconfig`:

```ini
[wsl2]
memory=8GB
processors=4
```

Then restart WSL: `wsl --shutdown` in PowerShell.

**Docker Desktop resource settings** — same as macOS: Settings → Resources → set Memory ≥ 6 GB.

**All commands below work in PowerShell, CMD, or WSL2 bash.** If you use CMD or PowerShell, replace the `\` line-continuation character with `` ` `` (PowerShell) or `^` (CMD).

---

## Quick Start

```bash
# 1. Clone the repository
git clone <repo-url>
cd london-orca-ai

# 2. Start the core pipeline (generator + broker + worker + object store)
docker compose up
```

That's it. The generator starts producing ship files immediately. The worker consumes and transforms them into Delta Lake.

To run with **monitoring** (Prometheus + Grafana):

```bash
docker compose --profile monitoring up
```

To run with **analytics** (Spark Thrift + Metabase):

```bash
docker compose --profile analytics up
```

To run **everything**:

```bash
docker compose --profile monitoring --profile analytics up
```

---

## Service URLs & Credentials

| Service | URL | Credentials |
|---|---|---|
| RabbitMQ Management UI | http://localhost:15672 | `guest` / `guest` |
| SeaweedFS S3 API | http://localhost:8333 | `any` / `any` |
| SeaweedFS Filer | http://localhost:8888 | — |
| Prometheus | http://localhost:9090 | — |
| Grafana | http://localhost:3000 | `admin` / `admin` |
| Spark Thrift Server (JDBC) | `localhost:10000` | — |
| Metabase | http://localhost:3010 | Set on first launch |

---

## Generator Configuration

The generator simulates a fleet of ships. It is configured via environment variables in `docker-compose.yml`. To change the defaults without editing the file, pass them on the command line:

```bash
# 20 ships, medium-sized files, 1 file/min
docker compose up -e SOURCES=20 -e SIZE=medium -e RATE=1

# Large files to stress-test chunking
docker compose up -e SIZE=large -e RATE=0.5
```

| Variable | Default | Description |
|---|---|---|
| `SOURCES` | `10` | Number of distinct ship IDs |
| `RATE` | `2` | Files published per minute |
| `SIZE` | `small` | `small` (~15 KB) · `medium` (~5 MB) · `large` (~45 MB) |
| `RATE_JITTER` | `0.3` | Random ± fraction applied to the interval between files |
| `SIZE_RANDOM` | `false` | If `true`, each file picks a random size tier |
| `S3_BUCKET` | `telemetry` | Target S3 bucket in SeaweedFS |
| `RABBITMQ_QUEUE` | `files` | Queue where file keys are published |

**File naming convention:**

```
{source_id}_{measurement}_{timestamp_utc}.json
# e.g. ship-42_gps_20260523T101400Z.json
```

### Running the generator locally (without Docker)

Requires Python 3.10+, no extra packages:

```bash
# macOS / Linux / WSL2
python generator/src/generator/generator.py \
  --output-dir ./input \
  --rate 2 \
  --size small \
  --sources 10

# Windows PowerShell
python generator\src\generator\generator.py `
  --output-dir .\input `
  --rate 2 `
  --size small `
  --sources 10
```

---

## Scaling Workers

Workers consume from RabbitMQ in parallel. Scale horizontally to increase throughput:

```bash
# Start 3 workers
docker compose up --scale worker=3

# Scale up while the pipeline is already running
docker compose up --scale worker=5 --no-recreate
```

Each worker runs its own PySpark session and writes to Delta Lake concurrently. Delta's optimistic concurrency control handles simultaneous appends safely.

---

## Validating Data in the Warehouse

### Option 1 — query script inside a running worker container

```bash
# Find the worker container name
docker compose ps

# Run the validation script
docker exec -it london-orca-ai-worker-1 \
  python3 /app/scripts/query_measurements.py
```

The script prints:
- Schema of the `measurements` table
- Total row count
- 10 most recent rows
- Row count per ship
- Average and max pipeline lag (event timestamp → recorded timestamp)

### Option 2 — Metabase (analytics profile)

1. Start with `--profile analytics`
2. Open http://localhost:3010
3. Complete the Metabase setup wizard
4. Add a **Spark SQL** data source:
   - Host: `spark-thrift`
   - Port: `10000`
   - Database: `default`
5. Query the `measurements` table

### Option 3 — raw SQL queries

Sample queries are in [`analytics/queries/`](analytics/queries/):

- [`ships.sql`](analytics/queries/ships.sql) — per-ship row counts and latest event
- [`pipeline_health.sql`](analytics/queries/pipeline_health.sql) — lag metrics

---

## Monitoring

Start with `--profile monitoring`, then open Grafana at http://localhost:3000 (`admin` / `admin`).

**Prometheus targets:**
- RabbitMQ metrics: http://localhost:15692/metrics
- Pipeline worker metrics (when emitted): http://localhost:9090

**Key metrics to watch:**

| Metric | What it signals |
|---|---|
| `rabbitmq_queue_messages` (`files`) | Backlog — if growing, workers are falling behind |
| `rabbitmq_queue_messages` (`files.dlq`) | Poisoned/unprocessable messages |
| Worker restart count (`docker compose ps`) | Repeated crashes indicate persistent failures |

---

## Running Tests

Install test dependencies first:

```bash
# macOS / Linux / WSL2
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-test.txt

# Windows PowerShell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-test.txt
```

> **Windows note:** PySpark requires Java. Install the JDK:
> `winget install EclipseAdoptium.Temurin.17.JDK`
> Then set `JAVA_HOME` to the JDK path and add `%JAVA_HOME%\bin` to `PATH`.

Run the full test suite:

```bash
pytest
```

Run only fast unit tests (no containers required):

```bash
pytest tests/test_ship_to_warehouse.py -v
```

Run resilience tests (requires Docker — starts a real RabbitMQ container):

```bash
pytest tests/test_resilience.py -v
```

Run end-to-end tests:

```bash
pytest tests/test_e2e.py -v
```

Run with coverage:

```bash
pytest --cov=. --cov-report=term-missing
```

---

## Repository Layout

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

---

## Failure Handling

| Failure scenario | Behaviour |
|---|---|
| Worker crashes mid-processing | RabbitMQ redelivers the message (no ack sent). Worker retries up to `RETRY_MAX` times on restart. |
| Malformed JSON or schema mismatch | Message is nacked and retried; after `RETRY_MAX` retries it is routed to `files.dlq` for inspection. |
| SeaweedFS temporarily unavailable | Worker retries with backoff; RabbitMQ message stays unacked until the download succeeds or the retry limit is reached. |
| RabbitMQ restart | Queues are durable; messages survive broker restarts. Workers reconnect automatically via `pika` retry logic. |
| Duplicate file delivery | The worker checks a `_processed_files` Delta table before writing; already-processed keys are skipped. |
| Delta concurrent write conflict | PySpark/Delta optimistic concurrency detects conflicts and retries the affected transaction. |

---

## Troubleshooting

**Worker exits immediately on startup**

```bash
docker compose logs worker
```

Common causes: RabbitMQ not yet ready (the healthcheck retries for up to 100 s), or SeaweedFS bucket not created yet. The `restart: on-failure` policy will retry automatically.

**`java.lang.OutOfMemoryError` in worker logs**

Increase Docker Desktop memory to at least 6 GB (see Prerequisites).

**SeaweedFS S3 returns 403**

The bucket must exist before the generator uploads. The generator creates it on startup; if it races, restarting the generator container resolves it:

```bash
docker compose restart generator
```

**Windows: `python` command not found**

Use `py` instead of `python`, or add the Python installation directory to your `PATH` in System Settings → Environment Variables.

**Windows: `JAVA_HOME` not set error when running tests locally**

```powershell
# PowerShell — set for the current session
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.x-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
```

Add these to your PowerShell profile (`$PROFILE`) to persist across sessions.

**Port already in use**

Stop any local services on the conflicting port, or override the host port in `docker-compose.yml`. Common conflicts: port `5672` (local RabbitMQ), `8080` (other HTTP servers).

---

## Stopping and Cleaning Up

```bash
# Stop all containers
docker compose down

# Stop and remove all volumes (deletes all ingested data)
docker compose down -v
```

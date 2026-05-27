# Storage (Landing Zone) — Requirements & Decision

Durable landing zone for raw ship telemetry files before processing.

---

## Requirements

| # | Requirement |
|---|-------------|
| 1 | High concurrent write throughput — many ships uploading large files simultaneously |
| 2 | High read throughput — workers pulling files for processing |
| 3 | Delete API — worker deletes file after confirmed warehouse write |
| 4 | TTL — auto-expire orphaned files (configurable) |
| 5 | Atomic visibility — no partial files visible to consumers |
| 6 | Persistence — survives container restart |
| 7 | Monitoring — metrics, throughput visibility, detect stalls |
| 8 | Resource efficient — runs on a Mac |
| 9 | Open source |

---

## Candidates

| Solution | Verdict | Reason |
|----------|---------|--------|
| **SeaweedFS** | ✅ Chosen | S3-compatible, handles large files well, built-in monitoring UI, active community, good documentation. Sufficient for production-scale simulation. |
| **Garage** | ❌ | Too minimal — weak monitoring, smaller community, less proven at high file throughput. |
| **Zenko CloudServer** | ❌ | Heavier, less commonly used, adds unnecessary complexity. |
| **Local volume** | ❌ | No TTL, no metrics, concurrent write bottleneck at scale. |
| **MinIO** | ❌ | Dropped — no longer maintained. |

---

## Known Edge Case

If the generator crashes after uploading a file but before publishing the queue message, the file is orphaned. TTL ensures it is cleaned up automatically within the configured window.

# Message Broker — Requirements & Decision

Coordinates file processing jobs between the generator and the processing workers.

---

## Requirements

| # | Requirement |
|---|-------------|
| 1 | **At-least-once delivery** — no message loss, even on broker restart |
| 2 | **Persistence** — messages survive container restart |
| 3 | **Parallel consumption** — multiple workers consume concurrently |
| 4 | **Acknowledgment** — worker acks only after confirmed warehouse write |
| 5 | **Dead Letter Queue** — failed messages after N retries go to DLQ |
| 6 | **Retry mechanism** — configurable retry count before DLQ |
| 7 | **Monitoring** — queue depth, throughput, consumer lag |
| 8 | **Resource efficient** — runs on a Mac |
| 9 | **Open source** |

---

## Candidates

| Solution | Fits? | Throughput (single node) | Source |
|----------|-------|--------------------------|--------|
| **RabbitMQ** | ✅ Chosen | ~50K msg/s (persistent + acks); up to 1M+ cluster | [RabbitMQ Blog](https://www.rabbitmq.com/blog/2022/05/16/rabbitmq-3.10-performance-improvements) · [VMware](https://blogs.vmware.com/tanzu/rabbitmq-hits-one-million-messages-per-second-on-google-compute-engine/) |
| **ActiveMQ Artemis** | ✅ | ~27K msg/s (tuned, persistent); millions in ideal conditions | [Apache Artemis Docs](https://artemis.apache.org/components/artemis/documentation/latest/perf-tools.html) · [Apache Blog](https://blogsarchive.apache.org/activemq/entry/fast_messaging_with_artemis) |
| **Redis Streams** | ⚠️ | ~136K ops/sec (XADD); up to 1.2M with multiple consumers | [Redis Docs](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/benchmarks/) |
| **NATS JetStream** | ⚠️ | ~160K msg/s peak (persistent mode) | [NATS Bench Docs](https://docs.nats.io/using-nats/nats-tools/nats_cli/natsbench) |
| **Kafka** | ❌ | ~2M writes/sec | [LinkedIn Engineering](https://engineering.linkedin.com/kafka/benchmarking-apache-kafka-2-million-writes-second-three-cheap-machines) · [kafka.apache.org](https://kafka.apache.org/performance.html) |

---

## Pros / Cons

| | **RabbitMQ** | **ActiveMQ Artemis** | **Redis Streams** | **NATS JetStream** |
|---|---|---|---|---|
| **Pros** | Built-in DLQ | Built-in DLQ | Extremely lightweight | Extremely lightweight |
| | Rich management UI | Web console | Already common in stacks | Very simple config |
| | AMQP standard | Multi-protocol (AMQP, MQTT, STOMP) | High throughput | Low latency |
| | Large community | Apache foundation backed | Fast consumer groups | At-least-once delivery |
| | Easy Docker setup | Flexible routing | ~50MB RAM | ~20MB RAM |
| | Well documented | Mature, battle-tested | | Active development |
| **Cons** | Erlang runtime (unfamiliar to debug) | JVM overhead — heaviest option | No native DLQ (manual impl) | Smaller community |
| | ~150MB RAM | ~200MB+ RAM | Not a dedicated broker | Less tooling & ecosystem |
| | | Steeper config learning curve | Monitoring less rich | Monitoring less rich |
| | | Smaller community than RabbitMQ | Redis is primarily a cache | JetStream adds complexity over core NATS |

---

## Decision: RabbitMQ

Chosen for its combination of high throughput (~50K msg/s, well above requirements), native DLQ, rich monitoring UI, and the largest community of the candidates. Runs comfortably on a Mac at ~150MB RAM.

Kafka ruled out — overkill in complexity and resource usage. Redis and NATS ruled out — no native DLQ, weaker monitoring. Artemis ruled out — heavier JVM footprint and smaller community than RabbitMQ.

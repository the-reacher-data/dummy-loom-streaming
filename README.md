# dummy-loom-streaming

Dummy streaming repo built on **loom-kernel** to explore Kafka/Bytewax streaming with a two-pod smoke.

## Stack

- **Redpanda** — Kafka-compatible broker (input broker)
- **Kafka** — Apache Kafka broker (response broker)
- **LGTM (Grafana + Tempo + Loki + Mimir)** — Observability stack with OTLP
- **loom-kernel** `0.5.0.dev62` — Streaming DSL, Bytewax adapter, Kafka clients
- **httpx** — HTTP client for scraping tasks

## Smoke topology

- `async-pod`: consumes `scrape.requests` from Redpanda and publishes responses to Kafka
- `sync-pod`: consumes `scrape.responses` from Kafka and fans out new `scrape.requests` back to Redpanda

The sync branch also uppercases the request id and prints the response before fan-out.

## Quick Start

The smoke pods emit OpenTelemetry spans to the local LGTM service at `http://localhost:4318`.

```bash
# Start infra + pods
make smoke-up

# Seed the initial requests into Redpanda
make smoke-seed

# Tail the pod logs
make smoke-logs

# Stop and clean up
make smoke-down
```

## URLs

- Redpanda Kafka: `localhost:19092`
- Redpanda Console: `localhost:19644`
- Kafka: `localhost:9092`
- Grafana: `http://localhost:3000`
- OTLP HTTP: `http://localhost:4318`

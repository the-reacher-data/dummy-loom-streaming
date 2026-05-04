# dummy-loom-streaming

Dummy streaming repo built on **loom-kernel** to explore Kafka/Bytewax streaming with a multi-pod smoke.

## Stack

- **Redpanda** — Kafka-compatible broker (input broker)
- **Kafka** — Apache Kafka broker (response broker)
- **LGTM (Grafana + Tempo + Loki + Mimir)** — Observability stack with OTLP
- **loom-kernel** `0.5.0.dev110` — Streaming DSL, Bytewax adapter, Kafka clients
- **httpx** — HTTP client for scraping tasks

## Smoke topology

- `async-pod`: consumes `scrape.requests` from Redpanda and publishes responses to Kafka
- `sync-pod`: consumes `scrape.responses` from Kafka and fans out new `scrape.requests` back to Redpanda
- `error-pod`: consumes `scrape.errors` and prints the full `ErrorEnvelope`

The sync branch also prints the response before fan-out and routes a demo business branch for product `13`.

## Smoke cases

The default seed sends three requests configured in `config/seed_streaming.yaml`:

- `seed-products-catalog`: happy path that starts the fan-out
- `seed-products-malformed`: malformed URL path that exercises task failure
- `seed-products-unreachable`: unreachable URL that exercises transport failure

The smoke keeps the seed declarative so the cases can be changed without touching code.

## Quick Start

The smoke pods emit OpenTelemetry spans to the local LGTM service at `http://localhost:4318`.
Run the steps in order:

```bash
# Start infra + pods
make smoke-up

# Seed the three configured requests into Redpanda
make smoke-seed-all

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

## Config

The seed payloads live in `config/seed_streaming.yaml`. The async and sync pods
also read their task-specific HTTP and fan-out settings from YAML, so the smoke
can be tuned without changing the task code.

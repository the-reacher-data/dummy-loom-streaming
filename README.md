# dummy-loom-streaming

Dummy streaming repo built on **loom-kernel** to validate Kafka/Bytewax streaming flows, error routing, and observability with a multi-pod smoke test.

## Stack

- **Redpanda** — Kafka-compatible broker (input broker)
- **Kafka** — Apache Kafka broker (response broker)
- **LGTM (Grafana + Tempo + Loki + Mimir)** — Observability stack with OTLP
- **loom-kernel** `0.5.0` — Streaming DSL, Bytewax adapter, Kafka clients
- **httpx** — HTTP client for scraping tasks

## Smoke topology

- `async-pod`: consumes `scrape.requests` from Redpanda and publishes responses to Kafka
- `sync-pod`: consumes `scrape.responses` from Kafka and fans out new `scrape.requests` back to Redpanda
- `error-pod`: consumes `scrape.errors` and prints the full `ErrorEnvelope`

The sync branch also prints the response before fan-out, routes a demo business branch for product `13`, and mirrors managed errors through the shared error topic.

## Smoke cases

The seed entrypoint publishes a selectable request set and a raw corrupt record for decode-error validation:

- `good`: only the happy-path catalog request
- `errors`: malformed URL path plus unreachable URL
- `all`: the happy path and both error cases

The smoke keeps the request shapes fixed and selects the published subset with a single mode flag. It also includes a corrupt record seed so the wire decode path can be validated end to end.

## Quick Start

The smoke pods emit OpenTelemetry spans to the local LGTM service at `http://localhost:4318`.
Run the steps in order:

```bash
# Start infra + pods
make start

# Seed requests into Redpanda. Defaults to the full set.
make seed

# Or choose a smaller subset
make seed SEED_MODE=good
make seed SEED_MODE=errors

# Tail the pod logs
make logs

# Stop and clean up
make down
```

## URLs

- Redpanda Kafka: `localhost:19092`
- Redpanda Console: `localhost:19644`
- Kafka: `localhost:9092`
- Grafana: `http://localhost:3000`
- OTLP HTTP: `http://localhost:4318`

## Config

 The seed payloads are generated directly by the seed entrypoint. The async and
 sync pods still read their task-specific HTTP and fan-out settings from YAML,
 so the smoke can be tuned without changing the task code.

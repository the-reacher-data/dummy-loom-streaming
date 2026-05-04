.PHONY: start stop down logs info sync lock test run-sync run-async smoke-up smoke-seed smoke-seed-all smoke-seed-product smoke-down smoke-logs smoke-logs-errors

UV ?= uv

start:
	docker compose up -d redpanda kafka otel-lgtm kafka-init
	@echo ""
	@echo "Services"
	@docker compose ps
	@echo ""
	@echo "URLs"
	@echo "- Redpanda Kafka:   localhost:19092"
	@echo "- Redpanda Console: localhost:19644"
	@echo "- Kafka:            localhost:9092"
	@echo "- Grafana (LGTM):   http://localhost:3000"

smoke-up:
	docker compose up -d --build redpanda kafka otel-lgtm kafka-init async-pod sync-pod error-pod
	@echo ""
	@docker compose ps

smoke-seed:
	docker compose run --rm seed

smoke-seed-all: smoke-seed

smoke-seed-product:
	docker compose run --rm --no-deps seed-product

smoke-down:
	docker compose down -v --remove-orphans

smoke-logs:
	docker compose logs --tail=200 -f async-pod sync-pod error-pod

smoke-logs-errors:
	docker compose logs --tail=200 -f error-pod

stop:
	docker compose down

down:
	docker compose down -v --remove-orphans

logs:
	docker compose logs --tail=200 -f

sync:
	$(UV) sync --extra dev

lock:
	$(UV) lock

test:
	$(UV) run pytest -q

run-sync:
	$(UV) run python -m dummy_streaming.entrypoints.run_sync

run-async:
	$(UV) run python -m dummy_streaming.entrypoints.run_async

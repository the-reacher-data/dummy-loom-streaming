SEED_MODE ?= all

.PHONY: start up seed down logs

start:
	docker compose up -d --build redpanda kafka otel-lgtm kafka-init async-pod sync-pod error-pod
	@echo ""
	@echo "========================================"
	@echo "  Stack UP"
	@echo "========================================"
	@echo ""
	@echo "Public URLs"
	@echo "- Redpanda Console: http://localhost:19644"
	@echo "- Grafana (LGTM):   http://localhost:3000"
	@echo "- Redpanda Kafka:   localhost:19092"
	@echo "- Kafka broker:     localhost:9092"
	@echo ""
	@docker compose ps

up: start

seed:
	docker compose run --rm --build seed uv run python -m smoke.entrypoints.seed_requests --mode $(SEED_MODE)

down:
	docker compose down -v --remove-orphans

logs:
	docker compose logs --tail=200 -f

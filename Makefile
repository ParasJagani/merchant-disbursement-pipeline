.PHONY: lint test branding check up down golden live
lint:
	ruff check .
	ruff format --check .
test:
	python -m pytest
branding:
	./scripts/check_branding.sh
check: lint test branding
up:
	docker compose up -d
down:
	docker compose down
golden:
	python -m generator backfill --out data/golden
live:
	python -m generator live --rate 50

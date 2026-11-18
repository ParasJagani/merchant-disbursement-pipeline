.PHONY: lint test branding check up down
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

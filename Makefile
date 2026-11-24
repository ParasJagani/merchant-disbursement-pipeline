.PHONY: lint test branding check up down golden live stage deploy verify demo-up demo-down silver-render silver-verify silver-reset
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
stage:
	python -m file_writer backfill --stager snowflake
deploy:
	python -m ops.deploy
verify:
	python -m ops.verify_bronze
demo-up:
	python -m ops.task_control up
demo-down:
	python -m ops.task_control down
silver-render:
	python -m silver.render
silver-verify:
	python -m ops.verify_silver
silver-reset:
	python -m ops.reset_silver

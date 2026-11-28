.PHONY: lint test branding check up down golden live stage deploy verify demo-up demo-down silver-render silver-verify silver-reset dbt-oracle dbt-local gold gold-verify silver-refresh quality-verify chaos-plan chaos-drill alerts-setup alerts-verify alerts-plan alerts-drill statements statements-verify statements-verify-snowflake dashboard dashboard-snowflake
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
dbt-oracle:
	cp data/golden/oracle/daily_disbursement.csv dbt/seeds/oracle_daily_disbursement.csv
dbt-local:
	python -m silver.local
	DBT_DUCKDB_PATH=dbt/target/local.duckdb dbt build --project-dir dbt --profiles-dir dbt --target duckdb
gold:
	dbt build --project-dir dbt --profiles-dir dbt --target snowflake
gold-verify:
	python -m ops.verify_gold
silver-refresh:
	python -m ops.refresh_silver
quality-verify:
	python -m ops.verify_quality
chaos-plan:
	python -m ops.chaos_drill
chaos-drill:
	python -m ops.chaos_drill --run
alerts-setup:
	python -m ops.setup_alerts --email "$(EMAIL)"
alerts-verify:
	python -m ops.verify_monitoring
alerts-plan:
	python -m ops.alert_drill
alerts-drill:
	python -m ops.alert_drill --run
statements:
	python -m statements.build --all --out data/statements
statements-verify:
	python -m statements.verify
statements-verify-snowflake:
	python -m statements.verify --source snowflake
dashboard:
	streamlit run dashboard/app.py
dashboard-snowflake:
	DASHBOARD_SOURCE=snowflake streamlit run dashboard/app.py

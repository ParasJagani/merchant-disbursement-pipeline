#!/usr/bin/env bash
# The whole pipeline on a real Snowflake account, start to finish, with the meter stopped at the end
# whatever happens. Used by the manual "integration" workflow, and runnable by hand:
#
#     make integration
#
# Needs the environment from docs/SETUP_SNOWFLAKE.md. Takes a few minutes and uses credits.
set -euo pipefail
cd "$(dirname "$0")/.."

stop_meter() {
  echo "== stopping tasks, alert and dynamic tables"
  python -m ops.task_control down
}
trap stop_meter EXIT

step() { echo; echo "== $*"; }

step "generate the golden dataset"
python -m generator backfill --out data/golden
make dbt-oracle
step "deploy the SQL objects and grants"
python -m ops.deploy
step "stage and load bronze"
python -m file_writer backfill --stager snowflake
python -m ops.load_bronze
python -m ops.verify_bronze
step "bring silver up"
python -m ops.task_control up
python -m ops.refresh_silver
python -m ops.verify_silver
step "quality"
python -m ops.verify_quality
step "gold, as the TRANSFORMER role"
dbt build --project-dir dbt --profiles-dir dbt --target snowflake
python -m ops.verify_gold
step "roles"
python -m ops.verify_roles
step "statements, as the REPORTER role"
python -m statements.verify --source snowflake
echo; echo "== integration passed"

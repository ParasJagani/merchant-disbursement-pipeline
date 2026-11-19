# Decisions

Each decision has the default from the design plan. Change a line here, and note why, if you choose differently.

| # | Decision | Choice | Status |
| --- | --- | --- | --- |
| 1 | Bronze load path | Internal stage plus a scheduled `COPY INTO` task; move to S3 and Snowpipe auto-ingest later | Default |
| 2 | Orchestration | GitHub Actions for CI only; Airflow is an optional Phase 6 stretch | Default |
| 3 | Repository | New public repository with fresh history after the scrub | Done in Phase 0 |
| 4 | Snowflake account | Trial (30 days, credit amount per the signup page); sign up right before the first run | Decided; clock starts at sign-up |
| 5 | DuckDB mode | Kept, for free local runs and CI | Default |
| 6 | Names | Fictional merchants (for example a ride-and-delivery brand with a food-delivery sub-brand) | Default |
| 7 | Dashboard | Streamlit | Default |

## Standing design rules

- Money is stored as integer minor units, never floats.
- Silver has two tracks: streams, tasks and `MERGE` for reference data; dynamic tables for events.
- dbt owns the dynamic tables, gold and the tests. Terraform owns infrastructure, bronze and the stream track.
- Gold is built on posting date from a ledger, and checked against the generator's oracle file.
- Bad data is quarantined with a reason; nothing is dropped silently.

## Phase 2 notes

- Terraform owns account-level objects (database, schemas, warehouses, monitors, roles). Data objects (stage, bronze tables, load procedure and task) are versioned SQL in `sql/`, applied by `python -m ops.deploy`. Reason: the Snowflake provider has been renaming resources, and plain SQL is easier to test and to read in an interview.
- Trial simplification: the Terraform service user holds ACCOUNTADMIN. Narrowing it is a Phase 8 task.
- Bronze rows are everything that passed validation, including duplicate deliveries. Duplicates are removed in silver by `event_id`.
- Invalid messages never reach bronze. They go to a dead-letter topic (Kafka) or a dead-letter file (backfill) with a reason.

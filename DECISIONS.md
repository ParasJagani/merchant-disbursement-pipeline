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
- dbt owns the dynamic tables, gold and the tests. Terraform owns account-level objects only. Bronze, the load task and the stream track are versioned SQL in `sql/`.
- Gold is built on posting date from a ledger, and checked against the generator's oracle file.
- Bad data is quarantined with a reason; nothing is dropped silently.

## Phase 2 notes

- Terraform owns account-level objects (database, schemas, warehouses, monitors, roles). Data objects (stage, bronze tables, load procedure and task) are versioned SQL in `sql/`, applied by `python -m ops.deploy`. Reason: the Snowflake provider has been renaming resources, and plain SQL is easier to test and to read in an interview.
- Trial simplification: the Terraform service user holds ACCOUNTADMIN. Narrowing it is a Phase 8 task.
- Bronze rows are everything that passed validation, including duplicate deliveries. Duplicates are removed in silver by `event_id`.
- Invalid messages never reach bronze. They go to a dead-letter topic (Kafka) or a dead-letter file (backfill) with a reason.

## Phase 2 first run (trial account, AWS, Enterprise edition)

Gate passed: all seven topics loaded with counts equal to the generator's manifest (187,684 valid
messages in 148 files), re-staging reported 0 new files, and a second `LOAD_BRONZE()` call changed
no counts. Six defects only a real account could show, all fixed:

1. Terraform provider 2.x takes single values `suspend_trigger` and `suspend_immediate_trigger`, not lists.
2. A resource monitor with a `frequency` also needs `start_timestamp` (`IMMEDIATELY`).
3. The example key path in `terraform.tfvars.example` did not match the file `make_keys.sh` writes.
4. A `PUT` needs a current database: the loader connection now sets database and schema, and the stage name is fully qualified.
5. The stage's file format was stored unqualified, so a worksheet or task with no current schema could not find it. It is now `{db}.BRONZE.NDJSON_GZ`, and `ALTER STAGE` repairs older stages.
6. `verify_bronze` signed in as the loader user with the admin key. It now uses the loader's own key.

Lesson: the tests used fakes for the connection, so they could not see these. Phase 8 adds one
integration run against a real account (or an emulator) so this class of error shows up in CI.

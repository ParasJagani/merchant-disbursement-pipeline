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
- Terraform owns account-level objects only. Bronze, the load task, silver and its tasks are versioned SQL in `sql/`. dbt, from Phase 4, owns gold and its tests.
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

## Phase 3 notes (silver)

- **Generated SQL.** Seven tables share the same parsing and validation rules, so `silver/spec.py` describes each table once and `python -m silver.render` writes `sql/30_silver_reference.sql` and `sql/40_silver_events.sql`. A test fails if the files on disk are not what the generator would write. Read the SQL; do not edit it.
- **Silver in versioned SQL, not dbt.** This replaces the earlier plan that dbt would own the dynamic tables. Reason: the same SQL can be run on DuckDB in tests and on Snowflake in the deploy, which dbt models with Snowflake-only dynamic-table config cannot. dbt returns in Phase 4 for gold and its tests.
- **Reference track** (merchants, accounts): an append-only stream on each bronze table, a task that runs only when the stream has data, and one procedure with one transaction: insert rejects, insert new versions, merge the current row. The stream offset moves only on commit, so a failed run is retried with the same rows. History is a view that derives `valid_to` from the next version, so a late event cannot leave a stale end date.
- **Event track** (five topics): three dynamic tables per topic. `typed_` parses and flags every bronze row; `silver_` keeps the newest valid event per entity (highest `sequence`, not latest arrival); `rejected_` keeps what failed with a reason and the original message. Late and out-of-order events fix themselves because ordering is by sequence.
- **Duplicates** are removed by `event_id` among valid rows only. A malformed copy of a real `event_id` (the generator makes these) must not push the genuine event out as its "duplicate".
- **Integers are strict.** A JSON string `"12.50"` is rejected as `bad_type_payload_amount`. `TRY_TO_NUMBER` on the text would have accepted it and rounded it to 13.
- **Fees can be zero.** A fee that rounds to 0 is valid; a negative fee is rejected. Sales, refunds and disputes must be positive; adjustments are signed and non-zero.
- **Dynamic tables do not wait for tests.** Unlike a dbt run, nothing blocks a refresh when data is bad, so the gate is by construction: bad rows are rejected into quarantine and never reach `silver_*`. Phase 5 adds alerts on the quarantine counts.
- **A new stream starts empty**, so the streams are created with `SHOW_INITIAL_ROWS = TRUE`; otherwise rows already in bronze would never be merged.
- **Cost.** Leaf dynamic tables use a 5 minute target lag, so they refresh (and use credits) while resumed. `make demo-down` suspends them. A fresh deploy leaves tasks suspended; dynamic tables start refreshing on creation, so run `make demo-down` after deploying if you will not be using them.
- **How it is tested.** `silver/local.py` runs the generated SQL on DuckDB (dynamic tables as views, streams as a refilled table). A ledger built only from the silver tables equals the oracle to the cent on the full golden dataset (2,199 account-days, 0 differing rows), and on a small run with malformed messages injected straight into bronze. DuckDB cannot show that Snowflake accepts each statement or that refreshes are incremental; `make silver-verify` checks that on the real account.

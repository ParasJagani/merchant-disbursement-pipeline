# Phase 4: gold, run and check

Gold is built by **dbt** from the silver tables. Three models in the `GOLD` schema:

| Model | What it is |
| --- | --- |
| `gold_ledger` | One signed money movement per row (sale, refund, dispute debit and reversal, platform fee, pass-through fee, adjustment). Every gold number is a sum of these. |
| `gold_daily_disbursement` | What each account is owed per posting day. Same columns as the oracle. |
| `gold_monthly_disbursement` | The daily report rolled up by calendar month. |

The posting date is the UTC date of the event that moved the money, not the day it arrived, so a late event lands on the right day.

The gate is the dbt test `gold_matches_oracle`: gold and the generator's answer key (loaded as the seed `oracle_daily_disbursement`) must have no differing rows, in either direction.

## Without Snowflake

```bash
make check        # includes the gold tests: real dbt runs on a small world, plus an off-by-one-cent run that must fail
make dbt-local    # builds silver in a DuckDB file from data/golden, then runs dbt on it (21 models/tests)
```

## On Snowflake

Needs the Phase 3 tables to be populated, and the same environment variables as before.

```bash
pip install -r requirements-dev.txt     # adds dbt-core and dbt-snowflake
make dbt-oracle                         # refresh the answer-key seed from data/golden (only needed if you regenerate)
make gold                               # dbt build: seed, three models, all tests
make gold-verify                        # independent comparison with the oracle CSV
make dbt-docs                           # lineage graph in the browser (local file, no Snowflake cost)
```

Expected: `dbt build` ends with `Completed successfully` and no ERROR; `gold-verify` prints three PASS lines.

dbt runs as the admin user (a trial simplification, as in the earlier phases). It uses `TRANSFORM_WH`, which suspends itself when idle. Nothing here keeps running: gold is rebuilt only when you run `make gold`. You do not need `make demo-down` for gold, but the Phase 3 dynamic tables and tasks still cost credits if they are resumed.

## If something fails

| Symptom | Likely cause |
| --- | --- |
| `Env var required but not provided: 'SNOWFLAKE_ADMIN_PRIVATE_KEY_PATH'` | The variables from `.env` are not loaded in this terminal |
| `Could not find profile` | Run from the repository root; the Makefile passes `--profiles-dir dbt` |
| `Object ... SILVER.silver_transactions does not exist` | Phase 3 has not been deployed, or the dynamic tables have not refreshed yet |
| `gold_matches_oracle` fails | Open `dbt/target/run/...` or run the test query by hand; the `problem` column says whether a row is missing from gold or unexpected |
| Counts in silver are 0 | The silver tasks or refreshes have not run since bronze loaded (see `docs/PHASE3_SILVER.md`) |

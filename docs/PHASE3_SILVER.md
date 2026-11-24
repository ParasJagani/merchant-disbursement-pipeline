# Phase 3: silver, run and check

Silver turns raw bronze messages into typed, validated, current-state tables. There are two tracks:

| Track | Tables | How it updates |
| --- | --- | --- |
| Reference | merchants, accounts (with history) | An append-only **stream** on bronze, a **task** that runs only when the stream has data, one `MERGE` procedure |
| Events | transactions, refunds, disputes, fees, adjustments | **Dynamic tables** that Snowflake keeps up to date within a target lag |

Everything lives in `sql/30_silver_reference.sql` and `sql/40_silver_events.sql`, generated from `silver/spec.py`.

## What you can check without Snowflake

```bash
make check            # includes the silver tests on DuckDB
python -m silver.render --check
```

The tests prove the logic against the generator's answer key: a ledger built from silver alone equals the oracle to the cent, with duplicates, out-of-order events and malformed messages in the input.

## Run it on Snowflake

You need the Phase 2 setup (trial account, keys, bronze loaded). From the repository folder, with the virtual environment active and your environment variables set as in `docs/SETUP_SNOWFLAKE.md`:

```bash
git pull                       # or: git pull "<path to the bundle>" main
pip install -r requirements-dev.txt     # adds duckdb, used to compute the expected counts
make deploy                    # creates streams, procedures, tasks (suspended) and dynamic tables
```

The dynamic tables are created and refresh once straight away. The tasks are created suspended. Resume the reference tasks and run them once by hand so you do not have to wait five minutes:

```sql
ALTER TASK MERCHANT_DISBURSEMENT.OPS.MERGE_MERCHANTS_TASK RESUME;
ALTER TASK MERCHANT_DISBURSEMENT.OPS.MERGE_ACCOUNTS_TASK RESUME;
CALL MERCHANT_DISBURSEMENT.OPS.MERGE_MERCHANTS();
CALL MERCHANT_DISBURSEMENT.OPS.MERGE_ACCOUNTS();
```

If the dynamic tables have not caught up yet, force them:

```sql
ALTER DYNAMIC TABLE MERCHANT_DISBURSEMENT.SILVER.typed_transactions REFRESH;
-- repeat for the other typed_ tables, then the silver_ and rejected_ tables
```

Then the gate:

```bash
make silver-verify
```

Expected: every line `PASS`. It computes the expected row counts by running the same SQL on DuckDB over `data/golden`, compares them with Snowflake, checks that the ledger built from the silver tables equals `data/golden/oracle/daily_disbursement.csv`, and that every dynamic table reports `INCREMENTAL` refresh mode.

**Stop the meter when you are done**, whatever the result:

```bash
make demo-down
```

This suspends the tasks and all dynamic tables.

## If something fails

| Symptom | Likely cause |
| --- | --- |
| `make deploy` fails at a `CREATE DYNAMIC TABLE` with a message about incremental refresh | Snowflake cannot refresh that query incrementally. Paste the message; the fix is a small rewrite of one query in `silver/render.py` |
| `make deploy` fails inside `CREATE PROCEDURE` | A scripting syntax difference. Paste the message; the same applies |
| Row counts are 0 in the reference tables | The stream was created before bronze had data, or the task is still suspended. `CALL` the procedure by hand |
| `ledger from silver equals the oracle` fails but counts pass | A value is parsed differently on Snowflake. Run `SELECT * FROM SILVER.silver_transactions LIMIT 5` and look at the timestamps |
| You changed a silver rule and nothing changed | Dynamic tables are never rebuilt by a deploy. Run `make silver-reset` (dry run), then `python -m ops.reset_silver --yes`, then `make deploy` |

## The comparison lab

The same merchants data built both ways, to see the trade-off:

| | Streams + tasks + MERGE | Dynamic table |
| --- | --- | --- |
| You write | Three statements, a transaction, a task | One query |
| History (SCD2) | Yes, kept in a versions table | Only latest state unless you keep the history yourself |
| Late events | Handled by the `sequence` rule in the MERGE | Handled by `ROW_NUMBER` in the query |
| Failure behaviour | A failed run keeps the stream offset and retries | Snowflake retries; you cannot add your own logic |
| Cost control | Runs only when the stream has data | Refreshes on the lag schedule if any base data changed |
| Best for | Slowly changing reference data you must keep history for | Facts where the latest state is enough |

That is why reference data uses the first and events the second.

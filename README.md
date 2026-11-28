# Merchant Disbursement Pipeline

A Kafka to Snowflake pipeline that turns payment events into a daily disbursement report per merchant, account and currency.

Status: Phases 0 to 5 built and verified on Snowflake (Phase 5 guide: `docs/PHASE5_QUALITY.md`); Phase 6 monitoring is built, Snowflake run pending (`docs/PHASE6_MONITORING.md`) (scaffold, generator and oracle, file writer and bronze load, silver). Phase 2 has run against a real Snowflake account and passed its gate (`docs/SETUP_SNOWFLAKE.md`); Phase 3 has also run on Snowflake and passed its gate (`docs/PHASE3_SILVER.md`). See `docs/roadmap.md` for the plan and `DECISIONS.md` for the choices made.

## Layout

| Folder | Purpose |
| --- | --- |
| `generator/` | Seeded event generator for seven topics, plus the oracle file |
| `file_writer/` | Consumes Kafka and writes staged files |
| `terraform/` | Account-level Snowflake objects: database, schemas, warehouses, monitors, roles |
| `sql/` | Versioned SQL for data objects: stage, bronze tables, load task, silver (applied by `ops/deploy.py`) |
| `silver/` | Silver table specs, the SQL generator (`python -m silver.render`) and the DuckDB test harness |
| `ops/` | Deploy, verify and demo-mode helpers |
| `dbt/` | Gold models (ledger, daily and monthly disbursement) and the tests that compare them with the oracle |
| `dashboard/` | Streamlit app |
| `statements/` | PDF statement builder |
| `tests/` | Python tests |

## Quick start

```bash
make up        # start Redpanda locally
make golden    # write the 30-day golden dataset to data/golden
make live      # publish the same events to Kafka at 50 events per second
make check     # lint, tests, branding check
```

## The generator

`python -m generator backfill` writes one NDJSON file per topic per day, an oracle file, and a manifest with hashes. `python -m generator live` publishes the same stream to Kafka. The same seed always gives the same bytes.

| Topic | Key | Events |
| --- | --- | --- |
| `ref.merchants` | `merchant_id` | upsert, delete |
| `ref.accounts` | `account_id` | upsert, delete |
| `payments.transactions` | `transaction_id` | authorized, captured, declined, voided |
| `payments.refunds` | `refund_id` | refund_issued |
| `payments.disputes` | `dispute_id` | dispute_opened, dispute_won, dispute_lost |
| `billing.fees` | `fee_id` | fee_assessed (platform or pass-through) |
| `ledger.adjustments` | `adjustment_id` | adjustment_posted |

Every message has the same envelope: `event_id`, `event_type`, `entity_id`, `event_time`, `emitted_at`, `schema_version`, `sequence`, and `op` on the reference topics. Money is integer minor units.

Delivery effects are switchable and never change the business truth: duplicate deliveries (about 1.5%), out-of-order arrival (about 2%), malformed poison messages (about 0.1%, six kinds), and one optional field added to transactions from day 15 (schema version 2).

`data/golden/oracle/daily_disbursement.csv` is the expected disbursement per merchant, account, currency and posting date, computed from the ledger entries and not from the delivered stream. `generator/reference.py` rebuilds it from the delivered messages alone (dedupe, reject malformed, post on event date), and a test proves the two agree. The warehouse gold table must match this file exactly.

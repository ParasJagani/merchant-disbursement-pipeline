# Merchant Disbursement Pipeline

A Kafka to Snowflake pipeline that turns payment events into a daily disbursement report per merchant, account and currency.

Status: Phase 0 complete (repository scaffold). See `docs/roadmap.md` for the plan and `DECISIONS.md` for the choices made.

## Layout

| Folder | Purpose |
| --- | --- |
| `generator/` | Seeded event generator for seven topics, plus the oracle file |
| `file_writer/` | Consumes Kafka and writes staged files |
| `terraform/` | Snowflake infrastructure, bronze and the stream track |
| `dbt/` | Dynamic tables, gold models and tests |
| `dashboard/` | Streamlit app |
| `statements/` | PDF statement builder |
| `tests/` | Python tests |

## Quick start

```bash
make up      # start Redpanda locally
make check   # lint, tests, branding check
```

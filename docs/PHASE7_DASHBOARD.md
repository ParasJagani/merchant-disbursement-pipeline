# Phase 7: dashboard and statements

Gate: **the totals in the PDF statements equal the gold totals.**

Two things were built on top of gold: a PDF statement per merchant and month, and a Streamlit
dashboard with two pages. Both read the same data through one small layer, from either a local
DuckDB file (free, works offline) or the real Snowflake gold tables.

## The statement

One PDF per merchant per calendar month (UTC), A4 landscape:

- **Page 1, the merchant summary:** a table per currency, then a table per account, each with sales,
  refunds, disputes, platform fees, pass-through fees, adjustments and total disbursed.
- **One page per account:** every day of the month and a total row.

Rules the layout follows: amounts are integer minor units formatted with integer arithmetic (never a
float); currencies are never added together; the printed totals are the sums of the printed daily
rows, so a reader can add the page up; and a statement is **refused** (not printed) if a gold row's
parts do not add up to its total. Closed merchants get statements too, because they were still paid.

```bash
python -m statements.build --merchant mch_001 --month 2026-01
make statements                    # every merchant and month into data/statements/
```

## The gate: `make statements-verify`

It builds every statement, reads each PDF back **as text, the way a person would**, and compares:

| Check | Compares |
| --- | --- |
| account totals equal gold monthly | printed account totals with `gold_monthly_disbursement` |
| daily rows equal gold daily | every printed day with `gold_daily_disbursement` |
| daily rows add up to the total | the printed days summed, with the printed total |
| currency summary equals its accounts | the printed currency table with the printed account table |
| account totals equal the answer key | printed account totals with the generator's oracle, which gold never touched |
| grand total per currency equals gold | all statements added up, with gold's grand total |

```bash
make statements-verify             # the local DuckDB file
make statements-verify-snowflake   # the real gold tables on Snowflake (the gate)
```

On the golden dataset: 30 statements, every check passes, and the grand totals per currency add up to
339,370,111 minor units, the same figure gold and the oracle give. The check earned its keep during
the build: the first version left out two closed merchants and the grand total came out short.

## First run on Snowflake (trial account)

Gate passed: `make gold` built 21 of 21 (3 models, 17 tests), then `make statements-verify-snowflake` built 30 statements and every check passed. Printed grand totals equal gold per currency: AUD 4,824,266; CAD 18,081,644; EUR 84,282,138; GBP 125,460,687; USD 106,721,376 (339,370,111 minor units in all). The dashboard started against Snowflake. The only fix after the build was in the checker, not the statements: pypdf 7 cuts text into different lines than 6.x, so the reader now works on the stream of words and takes merchant and month from the PDF title.

## The dashboard

```bash
make golden && make dbt-local      # once: builds dbt/target/local.duckdb
make dashboard                     # local file, free
make dashboard-snowflake           # the real gold tables (uses warehouse credits)
```

The sidebar switches the data source and the page.

**Merchant disbursements.** Pick a merchant, a month and an account (or all). You get the amount
disbursed per currency, a by-account table, a daily chart stacked by account, a chart of what was taken
off the payout (refunds, disputes, fees, adjustments), the daily rows, and a button that downloads
the statement PDF. Each account keeps its colour whatever is filtered, and there is one chart per
currency.

**Pipeline health.** The Phase 5 and 6 signals in one place: open incidents, signals firing,
quarantined messages and unaccounted rows, then the signals table, the incident log, the quarantine
reasons and the reconciliation table. On the local file the failed-task and failed-refresh signals show
`n/a`, because only Snowflake has that history.

Queries are cached for five minutes (the Refresh button clears the cache), so a Snowflake session
costs little. Gold and the dashboard do not need the tasks or dynamic tables running: `make demo-down`
state is fine, but the warehouse wakes for each query.

## What is tested where

| Question | Where it is proved |
| --- | --- |
| Money formatting, statement refuses bad rows, PDF totals equal gold and the answer key, the gate fails on a one-cent change, a missing merchant or a wrong answer key | `tests/test_statements.py`, every `make check` |
| The dashboard pages render, filters change the numbers, headline numbers equal gold, closed merchants are listed, a missing file is explained | `tests/test_dashboard.py`, using Streamlit's headless test harness |
| The Snowflake SQL is qualified and parameters are bound | `tests/test_statements.py`, with a scripted connection |
| The real gold tables on Snowflake produce the same statements | `make statements-verify-snowflake` |

## Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| Dashboard says the local file does not exist | `make golden && make dbt-local` |
| `statements.verify` says the answer key was not found | It looks for `data/golden/oracle/daily_disbursement.csv` (`make golden`); the other checks still run |
| Snowflake source cannot open | The usual key-pair environment variables must be set, and the warehouse must exist (see `docs/SETUP_SNOWFLAKE.md`) |
| A statement is refused: "the parts add up to ... but gold says ..." | A gold row is internally inconsistent. This must never be printed; check the gold model |

# Decisions

Each decision has the default from the design plan. Change a line here, and note why, if you choose differently.

| # | Decision | Choice | Status |
| --- | --- | --- | --- |
| 1 | Bronze load path | Internal stage plus a scheduled `COPY INTO` task; move to S3 and Snowpipe auto-ingest later | Default |
| 2 | Orchestration | GitHub Actions for CI only; Airflow is an optional Phase 6 stretch | Default |
| 3 | Repository | New public repository with fresh history after the scrub | Done in Phase 0 |
| 4 | Snowflake account | Trial or paid, to be confirmed | **Open: confirm before Phase 2** |
| 5 | DuckDB mode | Kept, for free local runs and CI | Default |
| 6 | Names | Fictional merchants (for example a ride-and-delivery brand with a food-delivery sub-brand) | Default |
| 7 | Dashboard | Streamlit | Default |

## Standing design rules

- Money is stored as integer minor units, never floats.
- Silver has two tracks: streams, tasks and `MERGE` for reference data; dynamic tables for events.
- dbt owns the dynamic tables, gold and the tests. Terraform owns infrastructure, bronze and the stream track.
- Gold is built on posting date from a ledger, and checked against the generator's oracle file.
- Bad data is quarantined with a reason; nothing is dropped silently.

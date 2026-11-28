# Roadmap

Nine phases, about 90 hours (an estimate). Each phase ends at a gate that is a demo or an automated check.

| Phase | Hours | Scope | Gate |
| --- | --- | --- | --- |
| P0 Scrub and decisions | 4 | Fresh repo, branding check, decisions file, layout | Repo clean, decisions recorded |
| P1 Generator and topics (done) | 12 | Seven topics, event envelope, oracle file, golden dataset, Redpanda | Same seed gives same data; oracle written |
| P2 Load into bronze (done) | 10 | File writer, Terraform base, stage, bronze tables, `COPY INTO` task | A replayed file loads exactly once |
| P3 Silver, both tracks (done) | 14 | Streams, tasks and `MERGE` for reference data; dynamic tables for events; comparison lab | Both tracks live; dynamic tables incremental |
| P4 Gold (done) | 10 | Ledger, daily disbursement, monthly view | Gold matches the oracle to the cent |
| P5 Quality and errors (done; gate passed on Snowflake) | 10 | Quarantine, test matrix, replay, chaos drill | Zero rows lost silently |
| P6 Monitoring (done; gate passed on Snowflake) | 10 | Alerts, health signals, demo mode | An alert fires on an injected failure |
| P7 Dashboard and PDF (done; gate passed on Snowflake) | 12 | Streamlit pages, one statement layout | PDF totals equal gold totals |
| P8 CI, docs, packaging (done; roles gate passed on Snowflake) | 8 | Integration run, README, diagram, demo | CI green, README complete |

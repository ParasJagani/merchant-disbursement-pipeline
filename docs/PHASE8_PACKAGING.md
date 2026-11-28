# Phase 8: CI, roles and packaging

Gate: **CI is green and the README is complete.** The role change also needs one run on Snowflake.

## Roles: one service user, three roles

Before this phase dbt and the dashboard ran as the account admin. Now:

| Role | Used by | Can | Cannot |
| --- | --- | --- | --- |
| LOADER | file writer, bronze load | stage files, insert into bronze | read silver or gold |
| TRANSFORMER | dbt (`make gold`) | read silver, own and replace gold tables | change silver, read the alert settings |
| REPORTER | dashboard, statements | read gold and a few named views | write anything, read bronze or the alert settings |

The grants are in `sql/70_grants.sql`, applied by `make deploy` (it runs last, so views replaced by
an earlier file get their grants back). TRANSFORMER and REPORTER roll up to the admin role, so the
admin service user can switch to either; dbt and the reporting code name their role explicitly
(`SNOWFLAKE_TRANSFORMER_ROLE`, `SNOWFLAKE_REPORTER_ROLE`). Deploy, the drills and the other `ops`
tools still run as the admin on purpose: they create and drop objects.

`make roles-verify` is the gate. It connects as each role and runs probes that must work and probes
that must be refused. A refusal counts only if Snowflake reports a privilege problem, so a typo
cannot pass as "denied". Probes that write use `WHERE 1=0` or a scratch table that is dropped.

### First run

```bash
git pull ...
make deploy          # applies 70_grants.sql; moves ownership of existing gold tables to TRANSFORMER
make roles-verify    # every line PASS
make gold            # dbt now runs as TRANSFORMER
make statements-verify-snowflake   # the reporter role reads gold
make demo-down
```

### First run on Snowflake (trial account)

Gate passed. `make roles-verify` printed 18 of 18 PASS; `make gold` built 21 of 21 as TRANSFORMER
(it replaced the tables the admin had made); `make statements-verify-snowflake` passed as REPORTER
with grand totals equal to gold. The first run of the gate failed on five refusal probes: sessions
held the admin's privileges through secondary roles. Fixed by `USE SECONDARY ROLES NONE` per
connection and `DEFAULT_SECONDARY_ROLES = ()` for the service user (see `DECISIONS.md`).

### Full integration run

`make integration` has also run end to end on the trial account and finished with "integration
passed": deploy (90 statements), bronze counts equal the golden dataset (re-staging added no new
files, so the replay check held), silver 33 of 33 with every dynamic table incremental, quality with
the one known warning (extra copies in the fees dead-letter file, no message missing), gold 21 of 21
as TRANSFORMER, roles 18 of 18, and statements 30 of 30 with totals equal to gold. The script then
suspended the tasks, the alert and the dynamic tables.

If `make gold` fails with "insufficient privileges", paste the error: the likely fix is one more
grant in `sql/70_grants.sql`, and `make roles-verify` will show which probe is missing it.

## CI

| Workflow | When | What |
| --- | --- | --- |
| `ci` | every push and pull request | lint, tests, branding, then the whole pipeline offline: golden data, silver on DuckDB, dbt build (21 tests), statements verified against gold, dbt docs built and uploaded as an artifact |
| `integration` | manual only, type `run` to confirm | `make integration` on a real Snowflake account; the meter is stopped at the end even if a step failed |

Secrets for `integration` (repository settings, Secrets): `SNOWFLAKE_ORGANIZATION`,
`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_ADMIN_PRIVATE_KEY`, `SNOWFLAKE_LOADER_PRIVATE_KEY` (the contents of the
two private key files). The keys are written to a temporary folder, removed at the end, and never
printed. The optional `BRANDING_DENYLIST` secret feeds the branding check in `ci`.

`make integration` runs the same steps by hand with your own environment.

## Packaging

README with the diagram and the proof table; `docs/DEMO.md` (ten minutes, cost-safe);
`docs/INTERVIEW_NOTES.md`; a sample statement and screenshots in `docs/samples` and `docs/images`;
`make dbt-docs` for the lineage graph.

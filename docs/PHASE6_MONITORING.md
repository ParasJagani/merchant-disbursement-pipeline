# Phase 6: monitoring and alerts

Gate: **an alert fires on an injected failure**, and clears when the failure is gone.

Phase 5 made sure nothing is lost silently. Phase 6 makes sure someone finds out: bad messages in
quarantine, rows that stop adding up, failed tasks and failed refreshes open an incident, send an
email, and close themselves when the cause is fixed.

## What was built

| Object | What it is |
| --- | --- |
| `OPS.alert_rules` | One row per signal: severity, threshold, window, on/off. Edit these rows, not SQL. A redeploy never overwrites a rule you changed |
| `OPS.health_signals` | A view that measures every signal right now and says which are firing |
| `OPS.alert_log` | One row per incident: `OPEN` while the signal fires, `RESOLVED` (with a time) when it clears |
| `OPS.RAISE_ALERTS()` | Opens new incidents (once each), refreshes open ones, resolves cleared ones, then sends emails |
| `OPS.SEND_ALERT_EMAILS()` | One email per new incident. A failed email is written to the log, never raised |
| `OPS.HEALTH_ALERT` | The scheduled Snowflake `ALERT` (every 10 minutes, serverless). It calls `RAISE_ALERTS()` when anything is firing, still open, or recently failed |

The SQL is `sql/60_monitoring.sql`; `make deploy` applies it. The alert is created **suspended**,
like the tasks, and `make demo-up` / `make demo-down` resume and suspend it.

## The signals

| Signal | Severity | Fires when | Default |
| --- | --- | --- | --- |
| `quarantine_recent` | warning | any message was quarantined in the last 60 minutes | on |
| `reconciliation_gap` | critical | `OPS.silver_reconciliation` shows unaccounted rows or missing entities | on |
| `task_failures_recent` | critical | a task run failed in the last 60 minutes | on |
| `dynamic_table_failures_recent` | critical | a dynamic table refresh failed in the last 60 minutes | on |
| `bronze_stale_minutes` | warning | the newest bronze row is older than 180 minutes | **off**: the demo data is loaded once, so it would always be stale. Turn it on when the live feed runs |

Change a rule with plain SQL, for example:

```sql
UPDATE MERCHANT_DISBURSEMENT.OPS.alert_rules SET threshold = 10 WHERE signal = 'quarantine_recent';
UPDATE MERCHANT_DISBURSEMENT.OPS.alert_rules SET enabled = TRUE WHERE signal = 'bronze_stale_minutes';
```

## Incident life cycle

A signal has at most one open incident. While it keeps firing, the incident stays one row (its
value and `last_seen_at` are refreshed, no second email). When the signal stops firing the next
run marks it `RESOLVED`. If it fires again later, that is a new incident and a new email.

## Email set-up (once)

Snowflake only sends email to a **verified address of a user in the same account**. Use the address
you signed up with:

```bash
make alerts-setup EMAIL=you@example.com
```

This creates the email integration, stores the address in `OPS.alert_settings` (in Snowflake, never
in git) and sends a test message. If it fails, verify the address in Snowsight (your name, then
Profile) and run it again. Without it, alerts still open and resolve in `OPS.alert_log`; the email
status reads `not configured`.

## The gate: `make alerts-drill`

```bash
make alerts-plan       # shows what it would do
make alerts-drill      # does it
```

The drill injects the same 572 bad messages as the chaos drill, runs the real alert once with
`EXECUTE ALERT`, and checks that exactly one `quarantine_recent` incident is open with the right
severity and count, that nothing else fired (the rejects are accounted for), that the alert run
succeeded, and that an email was attempted (and delivered, if the address is set up). It then
removes the messages, runs the alert again, and checks that the incident resolved and every table
count is back to normal. If the alert was suspended, the drill resumes it for the test and suspends
it again. It refuses to start if a real incident is already open.

Every line should be `PASS`. A `WARN` on "the email was delivered" means no address is set up yet.

`make alerts-verify` checks the objects, shows open incidents, and fails if the alert itself has
been failing (an alert that fails silently is worse than none).

## Demo mode

`make demo-up` resumes the tasks, dynamic tables and the alert; `make demo-down` suspends them all.
A demo is: `make demo-up`, `make silver-refresh`, `make alerts-drill` (watch the email arrive and
clear), `make demo-down`.

## Cost

The alert is serverless, so it bills only the seconds it runs, every 10 minutes while resumed. Its
condition reads `OPS.silver_reconciliation`, which scans bronze, so keep it suspended when you are
not demoing (`make demo-down`). On a large live feed, lengthen the schedule or move the
reconciliation to a table that a task keeps up to date.

## What is tested where

| Question | Where it is proved |
| --- | --- |
| Which conditions fire; one incident per condition; resolve; thresholds as data; redeploy keeps changes | `tests/test_monitoring.py`, on DuckDB, on every `make check` |
| The drill's own logic, including a failing alert and an unconfigured email | `tests/test_monitoring.py`, against a stand-in |
| The `ALERT` object, the `INFORMATION_SCHEMA` history functions, the email | `make alerts-verify` and `make alerts-drill`, on Snowflake only |

## Troubleshooting

| Symptom | Cause and fix |
| --- | --- |
| `make deploy` fails creating `HEALTH_ALERT` with insufficient privileges | The admin role needs `EXECUTE MANAGED ALERT` (serverless alerts). `GRANT EXECUTE MANAGED ALERT ON ACCOUNT TO ROLE <role>`, or add `WAREHOUSE = TRANSFORM_WH` to the alert |
| `alerts-setup` fails | The address is not a verified user address in this account |
| `alerts-verify`: "alert runs that failed" | Read the error shown; also `SELECT * FROM TABLE(INFORMATION_SCHEMA.ALERT_HISTORY())` |
| Drill: "before: no incidents are open" fails | A real incident is open. Look at `SELECT * FROM OPS.alert_log WHERE status = 'OPEN'` and fix its cause first |
| Email status `failed: ...` in `OPS.alert_log` | The reason is in the text, usually an unverified address or a missing integration grant. The incident itself is still recorded |

## Phase 6 first run (trial account)

Gate passed. `make alerts-drill`: one `quarantine_recent` incident opened for the 572 injected messages (severity warning, count 572), nothing else fired, the alert run was `TRIGGERED`, the email was delivered, and after cleanup the incident resolved and every table count was restored. `make alerts-verify`: all nine checks passed (five rules defined, procedures present, alert started, no failed alert runs, email configured, no open incidents).

One fix came from the real account: the first drill read `ALERT_HISTORY` once, straight after `EXECUTE ALERT`, and found nothing, because Snowflake records the run a little later. The incident, email and resolution had all worked. The drill now polls the history.


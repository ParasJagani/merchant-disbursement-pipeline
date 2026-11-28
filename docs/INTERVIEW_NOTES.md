# Design notes for interviews

Short answers, each tied to something in the repository. Details are in `DECISIONS.md`.

## The one-sentence version

A Kafka to Snowflake pipeline for merchant disbursements where every layer is proven against an
answer key that the pipeline never sees, and every failure mode has a drill that triggers it.

## Decisions and why

**An answer key first.** The generator computes the expected daily disbursement from the ledger
entries, not from the delivered stream. Duplicates, late messages and poison messages never change
the business truth, so any difference between gold and the key is a bug in the pipeline. This turned
"does it look right?" into a diff.

**Money is integer minor units, strictly.** A JSON string `"12.50"` is rejected, not coerced. Amounts
are never added across currencies, and the PDF formats money with integer arithmetic. Floats appear
only on chart axes.

**Silver in versioned SQL, gold in dbt.** Silver is Snowflake-native (streams, tasks, dynamic
tables) and its behaviour on refresh matters, so it is explicit SQL, generated from one spec so
seven tables cannot drift apart. Gold is plain portable SQL where dbt's tests and lineage earn their
keep. The same gold models run on DuckDB locally and on Snowflake.

**Test the logic on DuckDB, the engine on Snowflake.** A harness runs the same generated SQL locally,
so logic bugs cost nothing to find. Snowflake-only behaviour is proven by drills on the real account.
Each time this was tried, the real engine showed something the local run could not (timestamp parsing
is more lenient on Snowflake, so silver checks the exact shape with a regex).

**"Lost silently" is an accounting identity.** Every bronze row is rejected, a duplicate, or a unique
event, and the unaccounted count must be zero. The chaos drill injects 572 broken messages and checks
that each ends in quarantine with the expected reason.

**Incidents, not messages.** One open incident per condition, opened once and resolved when it clears.
The log is committed before any email is sent, and a failed email is logged, never raised. Rules are
data in a table, so thresholds change without a deploy.

**Reading the PDF back.** The statement is verified as a reader would see it. This caught real
mistakes: closed merchants were missing, and a pypdf upgrade changed how text is split into lines,
so the reader now depends on neither.

**Least privilege, proven.** Three roles (loader, transformer, reporter). `make roles-verify` connects
as each and checks both what it can do and what it is refused, accepting a refusal only if Snowflake
reports a privilege error.

## What went wrong, and what it taught

- A drill failed because the alert history lags the run. The fix was in the check (poll, with a
  timeout), not the alert.
- A dead-letter check failed after an interrupted run left uneven copies. Loss is now a failure,
  uneven copies a warning.
- A property test found that a delete arriving before its own earlier upsert was mishandled.
- Tests that used fakes for the connection could not see real-engine differences, which is why the
  integration run exists.

## What I would change with more time

- A real production account would use separate users per role and a narrower deploy role than the
  account admin; the trial runs everything through one service user that switches role.
- The live feed path (`make live`, staleness alert on) is built but the demo data is loaded once.
- Gold is rebuilt on demand; an incremental model would be the next step at real volume.

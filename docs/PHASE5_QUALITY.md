# Phase 5: quality and errors

The rule for this phase: **nothing is lost silently.** Every message that is delivered ends up in
exactly one place, and every business event the generator produced is in silver exactly once.

## Where each message can end up

| Place | What it means | Where you see it |
| --- | --- | --- |
| Dead letter | The file writer refused it (not JSON, wrong shape, unknown version, ...) and kept the original with a reason | `data/dlq/<topic>.ndjson` (or the `dlq.<topic>` Kafka topic) |
| Quarantine | It reached bronze but silver rejected it, with a reason and the original message | `QUARANTINE.rejected_*`, summarised in `QUARANTINE.reject_summary` |
| Duplicate | A valid message whose `event_id` was already seen (or a reference version already merged) | counted in `OPS.silver_reconciliation` |
| Kept | A unique valid event | `SILVER.silver_*` (latest state) and the typed layer |

The check is one view, `OPS.silver_reconciliation`. For every topic, `bronze_rows = rejected_rows + duplicate_rows + unique_valid_events`, so `unaccounted_rows` is 0, and every valid entity is in silver, so `missing_entities` is 0. A stream or refresh that is behind shows up as unaccounted rows instead of looking like a quiet success.

## What gets rejected, and why

Silver is at least as strict as the file writer, so a message that slips past the writer is still caught. The reason codes are stable:

| Reason | Caught by | Meaning |
| --- | --- | --- |
| `missing_event_id`, `missing_event_type`, `missing_entity_id` | writer and silver | Absent, null or not a string. A message that is not a JSON object also lands here in silver |
| `bad_type_event_time`, `bad_type_emitted_at` | writer (type) and silver (also format) | Missing, not text, or not `YYYY-MM-DDTHH:MM:SS.mmmZ` |
| `bad_type_schema_version`, `bad_type_sequence` | writer and silver | Not a JSON integer (`"1"`, `1.5` and `true` are all rejected) |
| `unknown_schema_version`, `unknown_event_type` | writer and silver | Not a version or event type this topic knows |
| `missing_payload_<field>` | writer and silver | A required field is absent, null or not text. For the key field this also covers a missing payload |
| `bad_type_payload_<field>` | writer and silver | A money field is not a JSON integer. `"12.50"` is rejected, never rounded |
| `unknown_currency` | writer and silver | Not one of USD, EUR, GBP, CAD, AUD (case-sensitive) |
| `bad_value_<field>` | silver only | A value outside its allowed list (payment method, status, fee type, reason code, ...) |
| `non_positive_amount`, `negative_amount`, `zero_amount` | silver only | Sales, refunds and disputes must be positive; fees may be 0 but not negative; adjustments may not be 0 |
| `bad_op` | writer and silver | A reference message that is neither an upsert nor a delete |
| `invalid_json` | writer only | Cannot be stored in a VARIANT column, so it never reaches bronze |

Two choices worth knowing. An optional field with the wrong type is treated as absent, not rejected. And unusual but valid messages are accepted on purpose: an unknown extra field, schema version 2, a fee of 0, a negative adjustment, an amount above 32 bits.

The full list is `silver/faults.py`: about 430 messages, each with the exact reason it must get.

## How it is tested (no Snowflake needed)

```bash
make check
```

- **The matrix** (`tests/test_quality_matrix.py`) runs every message in the catalog through the writer's validator and through silver on DuckDB. Each gets exactly its expected reason, silver is never more lenient than the writer, and the valid-but-unusual ones are not rejected.
- **The chaos tests** (`tests/test_quality_chaos.py`) deliver a world with 25% duplicates, 30% out-of-order events and 3% malformed messages. They prove: every delivered message is dead-lettered, quarantined, a duplicate or kept; every business event is in silver exactly once; the ledger still equals the oracle; and the result is identical whatever the order, the batching, or how many times everything is delivered.
- **The drill logic** (`tests/test_quality_ops.py`) runs the Snowflake drill against a DuckDB stand-in, including a run that must refuse to inject because the pipeline is not clean, and one that must still clean up after a failed check.

## On Snowflake

The Phase 5 changes tighten silver, so rebuild it from bronze first. Bronze is never touched.

```bash
python -m ops.reset_silver --yes     # drops silver objects (dynamic tables are never rebuilt by a plain deploy)
make deploy                          # recreates them, now with the stricter rules and the quality views
make demo-up                         # resumes tasks and dynamic tables
make silver-refresh                  # runs the merges and refreshes the dynamic tables now
make silver-verify                   # Phase 3 gate again: counts and the ledger still match
make gold                            # dbt build: gold still equals the oracle
make quality-verify                  # the Phase 5 gate (below)
make chaos-plan                      # shows what the drill would inject
make chaos-drill                     # injects ~430 bad messages, checks, cleans up
make demo-down                       # always, last
```

`make quality-verify` should show `PASS` on every line: no unaccounted rows, no missing entities, every staged file loaded, no failed loads, and (if `data/dlq` is on your machine) dead letters equal to the messages the writer should have rejected. Failed task runs and dynamic table refreshes in the last day are shown as `WARN`, not `FAIL`, because earlier setup mistakes would otherwise fail the gate forever.

`make chaos-drill` should end with every line `PASS`. It stops without injecting anything if the pipeline is not clean to begin with.

## If something fails

| Symptom | Likely cause |
| --- | --- |
| `make deploy` fails creating a view in `50_quality.sql` | Paste the message; the fix is a small change to the generator |
| `silver-refresh` says a dynamic table is suspended | Run `make demo-up` first |
| `quality-verify`: staged files never loaded | A file was staged but the load task has not run, or `ON_ERROR = SKIP_FILE` skipped it. The detail names the files; check `COPY_HISTORY` |
| `quality-verify`: dead letters do not equal | `data/dlq` is from a different dataset than `data/golden`, or a stage ran with different data |
| `chaos-drill`: "before" checks fail | Silver is not populated or not refreshed; run `make silver-refresh` |
| `chaos-drill` interrupted | The drill cleans up in a `finally` block. If your terminal died mid-run, delete rows with source file `chaos/<id>.ndjson` from the bronze tables and the two reference rejected tables, then refresh |

## Replay and recovery

- **Replaying data is safe.** A file staged again under the same name loads zero rows (`COPY INTO` remembers). Under a new name it loads, and silver counts the extra copies as duplicates; the totals do not change.
- **Changing a rule** is a rebuild from bronze: `python -m ops.reset_silver --yes`, `make deploy`, `make demo-up`, `make silver-refresh`. Because bronze keeps every raw message, nothing needs to be re-sent.
- **Fixing a dead letter**: dead letters keep the original text and the reason, so after a fix they can be restaged.

# Demo: ten minutes

Two versions. The offline one costs nothing and needs no account. The Snowflake one shows the real
engine and costs a few cents of credit; it always ends with `make demo-down`.

## The story in one minute

Payment events arrive as messages: sales, refunds, disputes, fees and adjustments, in seven topics.
Some are duplicated, some arrive late, some are broken. The pipeline must still tell each merchant,
to the cent, how much was disbursed per account and currency each day. To prove it, the generator
also writes an **answer key** that the pipeline never reads, and every stage is compared with it.

## Offline (no Snowflake)

```bash
make golden && make dbt-local     # data, silver on DuckDB, gold with dbt: 21 of 21 tests pass
make statements-verify            # 30 PDFs read back as text and compared with gold and the answer key
make dashboard                    # open the browser; Ctrl+C when done
make dbt-docs                     # the lineage graph (blue icon, bottom right); Ctrl+C when done
```

What to say while it runs:

1. **dbt build**: "Gold is three tables. The ledger is one row per money movement, the daily table
   sums it, the monthly table sums that. A test compares the daily table with the answer key in both
   directions."
2. **statements-verify**: "I do not trust the code that wrote the PDF. This reads the printed text back
   and compares every day and every total with gold."
3. **Dashboard, disbursements page**: pick a merchant and month, show the per-currency total, the
   by-account table, and the download button. Currencies are never added together.
4. **Dashboard, health page**: "Every signal, every incident, the quarantine and the reconciliation:
   each bronze row is rejected, a duplicate or a unique event, and unaccounted must be zero."

## On Snowflake

```bash
make demo-up                  # resume tasks, dynamic tables and the alert
make silver-refresh           # catch up now instead of waiting for the schedules
make silver-verify            # silver equals an independent run; all dynamic tables incremental
make gold                     # dbt build as the TRANSFORMER role
make roles-verify             # each role can do its job and is refused everything else
make statements-verify-snowflake
make dashboard-snowflake      # pick Snowflake in the sidebar; Ctrl+C when done
make demo-down                # always
```

Optional, the part people remember:

```bash
make chaos-plan               # what would be injected (changes nothing)
make chaos-drill              # inject 572 bad messages; all 572 end in quarantine, ledger unchanged
make alerts-drill             # the quarantine opens one incident, an email goes out, cleanup resolves it
make demo-down
```

## Questions to expect

- **Why is silver SQL and not dbt?** Dynamic tables and streams are Snowflake objects with their own
  refresh behaviour; a dbt run would hide that. dbt owns gold, where its tests and lineage help.
- **What stops a bad message corrupting money?** Silver is stricter than the writer, rejects rows into
  quarantine with a reason, and the reconciliation identity must hold: nothing is lost silently.
- **How do you know the PDF is right?** `statements.verify` reads it back and compares with gold and
  with the generator's answer key, which gold never touched.
- **What does it cost to keep running?** Tasks and dynamic tables use credits while resumed, so the demo
  scripts suspend them. Gold and reporting run only on demand.

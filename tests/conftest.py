"""Shared fixtures. ``reporting_world`` is a local DuckDB file shaped like the real pipeline's:
silver and monitoring tables built by the real SQL, gold tables taken from the answer key."""

from collections import defaultdict
from datetime import datetime, timezone

import pytest

GOLD_DAILY = (
    "CREATE TABLE gold.gold_daily_disbursement (merchant_id VARCHAR, account_id VARCHAR, "
    "currency VARCHAR, posting_date DATE, sales_amount BIGINT, refund_amount BIGINT, "
    "dispute_amount BIGINT, platform_fee_amount BIGINT, pass_through_fee_amount BIGINT, "
    "adjustment_amount BIGINT, disbursed_total BIGINT)"
)
# The same roll-up as dbt/models/gold/gold_monthly_disbursement.sql.
GOLD_MONTHLY = (
    "CREATE TABLE gold.gold_monthly_disbursement AS SELECT merchant_id, account_id, currency, "
    "date_trunc('month', posting_date)::DATE AS posting_month, count(*) AS active_days, "
    "sum(sales_amount)::BIGINT AS sales_amount, sum(refund_amount)::BIGINT AS refund_amount, "
    "sum(dispute_amount)::BIGINT AS dispute_amount, "
    "sum(platform_fee_amount)::BIGINT AS platform_fee_amount, "
    "sum(pass_through_fee_amount)::BIGINT AS pass_through_fee_amount, "
    "sum(adjustment_amount)::BIGINT AS adjustment_amount, "
    "sum(disbursed_total)::BIGINT AS disbursed_total "
    "FROM gold.gold_daily_disbursement GROUP BY ALL"
)


@pytest.fixture(scope="session")
def reporting_world(tmp_path_factory):
    """(DuckDB file, oracle rows). Has closed merchants and one open incident."""
    from generator.chaos import deliver
    from generator.config import GeneratorConfig
    from generator.oracle import COLUMNS
    from generator.reference import replay_to_oracle
    from generator.schema import validate_message
    from generator.simulate import simulate
    from ops.chaos_drill import injectable
    from silver.faults import TABLES
    from silver.local import LocalSilver
    from silver.spec import EVENT_TOPICS, REFERENCE_TABLES

    bronze = {t.topic: t.bronze for t in (*REFERENCE_TABLES, *EVENT_TOPICS)}
    cfg = GeneratorConfig(days=40, tx_per_day=120)  # spans January and February 2026
    messages, _ = deliver(cfg, simulate(cfg).events)
    messages = sorted(messages, key=lambda m: m.order)
    path = tmp_path_factory.mktemp("reporting") / "local.duckdb"
    silver = LocalSilver(str(path))
    silver.deploy()
    rows = defaultdict(list)
    for m in messages:
        if validate_message(m.topic, m.value)[0]:
            rows[bronze[m.topic]].append(
                (m.value, "part.ndjson.gz", len(rows[bronze[m.topic]]) + 1)
            )
    for table, batch in rows.items():
        silver.load_bronze(table, batch)
    silver.run_reference()
    oracle = replay_to_oracle(messages)

    con = silver.con
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute(GOLD_DAILY)
    for r in oracle:
        con.execute("INSERT INTO gold.gold_daily_disbursement VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    [r[c] for c in COLUMNS])  # fmt: skip
    con.execute(GOLD_MONTHLY)

    # A few bad messages, loaded "now", and the incident they open, for the health page.
    silver._clock = datetime.now(timezone.utc).replace(tzinfo=None)
    for n, fault in enumerate([f for f in injectable() if f.topic in bronze][:4], start=1):
        silver.load_bronze(TABLES[fault.topic].bronze, [(fault.raw, "chaos/t.ndjson", n)])
    silver.run_reference()
    silver.call("OPS.RAISE_ALERTS")
    silver.close()
    return path, oracle

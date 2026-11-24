"""Silver logic, run on DuckDB against the generator's own answer key.

The strongest check is the last-mile one: a small ledger derived ONLY from the silver tables must
add up to the oracle, to the cent, even though the delivered messages contain duplicates,
out-of-order events and malformed messages.
"""

import json
from collections import Counter, defaultdict

import duckdb
import pytest

from generator.chaos import deliver
from generator.config import GeneratorConfig
from generator.reference import replay_to_oracle
from generator.schema import validate_message
from generator.simulate import simulate
from silver import render
from silver.local import LocalSilver
from silver.spec import EVENT_TOPICS, REFERENCE_TABLES

CFG = GeneratorConfig(days=20, tx_per_day=200)
BRONZE = {t.topic: t.bronze for t in (*REFERENCE_TABLES, *EVENT_TOPICS)}


@pytest.fixture(scope="module")
def world():
    result = simulate(CFG)
    messages, stats = deliver(CFG, result.events)
    return result, sorted(messages, key=lambda m: m.order), stats


def _load(messages, route_invalid: bool) -> tuple[LocalSilver, dict]:
    """Bronze as the file writer would leave it. With route_invalid=False, malformed-but-parseable
    messages reach bronze too (a writer bug or a bypass), so silver has to catch them itself."""
    silver = LocalSilver()
    silver.deploy()
    rows: dict[str, list] = defaultdict(list)
    where = {}
    for m in messages:
        ok, reason = validate_message(m.topic, m.value)
        if not ok and route_invalid:
            continue
        try:
            json.loads(m.value)
        except ValueError:
            continue  # COPY cannot load text that is not JSON at all
        table = BRONZE[m.topic]
        rows[table].append((m.value, f"{m.topic}/part.ndjson.gz", len(rows[table]) + 1))
        where[(table, len(rows[table]))] = reason
    for table, batch in rows.items():
        silver.load_bronze(table, batch)
    silver.run_reference()
    return silver, where


@pytest.fixture(scope="module")
def clean(world):
    return _load(world[1], route_invalid=True)[0]


@pytest.fixture(scope="module")
def dirty(world):
    return _load(world[1], route_invalid=False)


def _ledger(silver: LocalSilver) -> list[dict]:
    return silver.ledger()


# --- the generated files ---------------------------------------------------------------------


def test_generated_sql_is_up_to_date():
    assert render.main(["--check"]) == 0


def test_every_object_is_qualified_with_the_database():
    for name in ("30_silver_reference.sql", "40_silver_events.sql"):
        text = (render.SQL_DIR / name).read_text()
        assert "{db}" in text
        assert "FROM BRONZE." not in text and "FROM SILVER." not in text


def test_tasks_are_created_suspended():
    # CREATE TASK leaves a task suspended; the file must never resume one.
    text = (render.SQL_DIR / "30_silver_reference.sql").read_text()
    assert "RESUME" not in text.upper()
    assert text.count("CREATE OR REPLACE TASK") == 2


# --- the event track -------------------------------------------------------------------------


def test_silver_ledger_matches_the_oracle(world, clean):
    _, messages, _ = world
    assert _ledger(clean) == replay_to_oracle(messages)


def test_silver_ledger_matches_the_oracle_when_malformed_messages_reach_bronze(world, dirty):
    _, messages, _ = world
    assert _ledger(dirty[0]) == replay_to_oracle(messages)


def test_latest_state_is_the_highest_sequence_per_entity(world, clean):
    result, _, _ = world
    latest = {}
    for e in result.events:
        if e.topic == "payments.transactions":
            cur = latest.get(e.entity_id)
            if cur is None or e.sequence > cur.sequence:
                latest[e.entity_id] = e
    got = dict(clean.query("SELECT transaction_id, status FROM SILVER.silver_transactions"))
    assert got == {k: v.event_type for k, v in latest.items()}


def test_zero_value_fees_are_kept(world, clean):
    result, _, _ = world
    fees = [e for e in result.events if e.topic == "billing.fees"]
    assert any(e.payload["amount"] == 0 for e in fees)  # the data really has them
    assert clean.query("SELECT count(*) FROM SILVER.silver_fees")[0][0] == len(fees)


def test_every_entity_appears_once(clean):
    for t in EVENT_TOPICS:
        dupes = clean.query(
            f"SELECT {t.key}, count(*) FROM SILVER.silver_{t.name} GROUP BY 1 HAVING count(*) > 1"
        )
        assert dupes == [], t.name


def test_schema_drift_column_is_null_before_day_15_and_set_after(clean):
    before, after = clean.query(
        "SELECT count(channel) FILTER (WHERE schema_version = 1), "
        "count(channel) FILTER (WHERE schema_version = 2) FROM SILVER.silver_transactions"
    )[0]
    assert before == 0 and after > 0


def test_malformed_messages_are_quarantined_with_the_same_reason_the_writer_gives(world, dirty):
    silver, where = dirty
    equivalent = {"not_an_object": "missing_event_id"}  # [1,2,3] has no event_id either
    seen = 0
    for t in EVENT_TOPICS:
        rows = silver.query(f"SELECT source_row, reject_reason FROM QUARANTINE.rejected_{t.name}")
        for source_row, reason in rows:
            expected = where[(t.bronze, source_row)]
            assert reason == equivalent.get(expected, expected)
            seen += 1
    expected_bad = sum(1 for reason in where.values() if reason)
    assert seen == expected_bad > 0


def test_nothing_rejected_leaks_into_silver_and_nothing_valid_is_lost(world, dirty):
    silver, where = dirty
    _, messages, _ = world
    valid_ids = set()
    for m in messages:
        ok, _ = validate_message(m.topic, m.value)
        if ok:
            valid_ids.add((m.topic, json.loads(m.value)["payload"][_key(m.topic)]))
    got = set()
    for t in EVENT_TOPICS:
        got |= {
            (t.topic, r[0]) for r in silver.query(f"SELECT {t.key} FROM SILVER.silver_{t.name}")
        }
    assert got == {x for x in valid_ids if x[0] in {t.topic for t in EVENT_TOPICS}}


def _key(topic: str) -> str:
    return next(t.key for t in (*EVENT_TOPICS, *REFERENCE_TABLES) if t.topic == topic)


def test_a_string_amount_is_rejected_not_rounded():
    silver = LocalSilver()
    silver.deploy()
    event = {
        "event_id": "evt_1",
        "event_type": "authorized",
        "entity_id": "txn_1",
        "event_time": "2026-01-01T00:00:00.000Z",
        "emitted_at": "2026-01-01T00:00:01.000Z",
        "schema_version": 1,
        "sequence": 1,
        "payload": {
            "transaction_id": "txn_1",
            "account_id": "a",
            "merchant_id": "m",
            "amount": "12.50",
            "currency": "EUR",
            "payment_method": "card",
            "status": "authorized",
        },
    }
    silver.load_bronze("payments_transactions", [(json.dumps(event), "f", 1)])
    assert silver.query("SELECT count(*) FROM SILVER.silver_transactions")[0][0] == 0
    assert silver.query("SELECT reject_reason FROM QUARANTINE.rejected_transactions") == [
        ("bad_type_payload_amount",)
    ]


def test_a_malformed_copy_of_a_real_event_does_not_displace_the_real_one():
    silver = LocalSilver()
    silver.deploy()
    good = {
        "event_id": "evt_1",
        "event_type": "captured",
        "entity_id": "txn_1",
        "event_time": "2026-01-01T00:00:00.000Z",
        "emitted_at": "2026-01-01T00:00:01.000Z",
        "schema_version": 1,
        "sequence": 2,
        "payload": {
            "transaction_id": "txn_1",
            "account_id": "a",
            "merchant_id": "m",
            "amount": 1250,
            "currency": "EUR",
            "payment_method": "card",
            "status": "captured",
        },
    }
    bad = {**good, "schema_version": 99}
    # The bad copy is loaded first, so it would win a naive "first row per event_id" rule.
    silver.load_bronze(
        "payments_transactions", [(json.dumps(bad), "f", 1), (json.dumps(good), "f", 2)]
    )
    assert silver.query("SELECT amount, status FROM SILVER.silver_transactions") == [
        (1250, "captured")
    ]


def test_loading_the_same_files_again_changes_nothing(world):
    _, messages, _ = world
    once, _ = _load(messages, route_invalid=True)
    twice, _ = _load(messages, route_invalid=True)
    rows = defaultdict(list)
    for m in messages:
        if validate_message(m.topic, m.value)[0]:
            table = BRONZE[m.topic]
            rows[table].append((m.value, "again/part.ndjson.gz", len(rows[table]) + 1))
    for table, batch in rows.items():
        twice.load_bronze(table, batch)
    twice.run_reference()
    assert _ledger(twice) == _ledger(once)
    for t in REFERENCE_TABLES:
        assert twice.query(f"SELECT * FROM SILVER.silver_{t.name} ORDER BY 1") == once.query(
            f"SELECT * FROM SILVER.silver_{t.name} ORDER BY 1"
        )


# --- the reference track ---------------------------------------------------------------------


def test_reference_current_rows_match_the_latest_clean_event(world, clean):
    result, _, _ = world
    for t in REFERENCE_TABLES:
        latest = {}
        for e in result.events:
            if e.topic == t.topic:
                cur = latest.get(e.entity_id)
                if cur is None or e.sequence > cur.sequence:
                    latest[e.entity_id] = e
        got = {
            row[0]: (row[1], row[2])
            for row in clean.query(
                f"SELECT {t.key}, is_deleted, last_sequence FROM SILVER.silver_{t.name}"
            )
        }
        assert got == {k: (v.op == "delete", v.sequence) for k, v in latest.items()}


def test_history_keeps_one_row_per_version_and_exactly_one_current(world, clean):
    result, _, _ = world
    for t in REFERENCE_TABLES:
        versions = Counter(e.entity_id for e in result.events if e.topic == t.topic)
        hist = clean.query(
            f"SELECT {t.key}, count(*), count(*) FILTER (WHERE is_current) "
            f"FROM SILVER.silver_{t.name}_history GROUP BY 1"
        )
        assert {k: n for k, n, _ in hist} == dict(versions)
        assert all(cur == 1 for _, _, cur in hist)


def test_a_deleted_account_keeps_its_last_known_attributes(clean):
    rows = clean.query("SELECT currency, fee_plan FROM SILVER.silver_accounts WHERE is_deleted")
    assert rows and all(currency is not None and plan is not None for currency, plan in rows)


def test_a_late_old_version_cannot_overwrite_a_newer_current_row():
    silver = LocalSilver()
    silver.deploy()

    def merchant(seq, name, event_time):
        return json.dumps(
            {
                "event_id": f"evt_{seq}",
                "event_type": "merchant_upserted",
                "entity_id": "m1",
                "event_time": event_time,
                "emitted_at": event_time,
                "schema_version": 1,
                "sequence": seq,
                "op": "upsert",
                "payload": {
                    "merchant_id": "m1",
                    "name": name,
                    "country": "DE",
                    "risk_status": "low",
                },
            }
        )

    silver.load_bronze(
        "ref_merchants", [(merchant(2, "New name", "2026-01-02T00:00:00.000Z"), "f", 1)]
    )
    assert silver.run_reference()["OPS.MERGE_MERCHANTS"] == "ok"
    assert silver.run_reference()["OPS.MERGE_MERCHANTS"] == "skipped"  # an idle stream does nothing
    silver.load_bronze(
        "ref_merchants", [(merchant(1, "Old name", "2026-01-01T00:00:00.000Z"), "g", 1)]
    )
    silver.run_reference()
    assert silver.query("SELECT name, last_sequence FROM SILVER.silver_merchants") == [
        ("New name", 2)
    ]
    history = silver.query(
        "SELECT sequence, name, is_current FROM SILVER.silver_merchants_history ORDER BY 1"
    )
    assert history == [(1, "Old name", False), (2, "New name", True)]


def test_a_failed_merge_does_not_consume_the_stream():
    silver = LocalSilver()
    silver.deploy()
    event = json.dumps(
        {
            "event_id": "evt_1",
            "event_type": "merchant_upserted",
            "entity_id": "m1",
            "event_time": "2026-01-01T00:00:00.000Z",
            "emitted_at": "2026-01-01T00:00:00.000Z",
            "schema_version": 1,
            "sequence": 1,
            "op": "upsert",
            "payload": {"merchant_id": "m1", "name": "A", "country": "DE", "risk_status": "low"},
        }
    )
    silver.load_bronze("ref_merchants", [(event, "f", 1)])
    silver.con.execute("ALTER TABLE SILVER.silver_merchants RENAME TO parked")
    with pytest.raises(duckdb.Error):
        silver.run_reference()
    silver.con.execute("ALTER TABLE SILVER.parked RENAME TO silver_merchants")
    assert silver.query("SELECT count(*) FROM SILVER.silver_merchants_versions")[0][0] == 0
    assert silver.run_reference()["OPS.MERGE_MERCHANTS"] == "ok"
    assert silver.query("SELECT name FROM SILVER.silver_merchants") == [("A",)]

"""The Snowflake quality tools, run against the DuckDB stand-in and against scripted fakes."""

import json
import re
from collections import defaultdict

import pytest

from generator.chaos import deliver
from generator.config import GeneratorConfig
from generator.reference import replay_to_oracle
from generator.schema import validate_message
from generator.simulate import simulate
from ops import chaos_drill, verify_quality
from silver.local import LocalSilver, translate
from silver.spec import EVENT_TOPICS, REFERENCE_TABLES

CFG = GeneratorConfig(days=10, tx_per_day=120)
BRONZE = {t.topic: t.bronze for t in (*REFERENCE_TABLES, *EVENT_TOPICS)}


class StandIn:
    """Answers the drill's Snowflake SQL with a LocalSilver (DuckDB) underneath."""

    def __init__(self, silver):
        self.silver, self._rows, self.log = silver, [], []

    def cursor(self):
        return self

    def close(self):
        pass

    def fetchall(self):
        return self._rows

    def execute(self, sql, params=None):
        self.log.append(sql)
        text = re.sub(r"\bDB\.", "", sql)
        if text.startswith("ALTER DYNAMIC TABLE"):
            return  # dynamic tables are views here and always current
        if text.startswith("CALL "):
            self.silver.run_reference()
            return
        if text.startswith("INSERT INTO BRONZE."):
            table = re.match(r"INSERT INTO BRONZE\.(\w+)", text)[1]
            rows = [tuple(params[i : i + 3]) for i in range(0, len(params), 3)]
            self.silver.load_bronze(table, rows)
            return
        self._rows = self.silver.query(translate(text))


@pytest.fixture()
def pipeline():
    result = simulate(CFG)
    messages, _ = deliver(CFG, result.events)
    messages = sorted(messages, key=lambda m: m.order)
    silver = LocalSilver()
    silver.deploy()
    rows = defaultdict(list)
    for m in messages:
        if validate_message(m.topic, m.value)[0]:
            rows[BRONZE[m.topic]].append(
                (m.value, "part.ndjson.gz", len(rows[BRONZE[m.topic]]) + 1)
            )
    for table, batch in rows.items():
        silver.load_bronze(table, batch)
    silver.run_reference()
    return silver, replay_to_oracle(messages)


def test_the_chaos_drill_injects_checks_and_cleans_up(pipeline):
    silver, oracle = pipeline
    conn = StandIn(silver)
    results = chaos_drill.run(conn, "DB", oracle, drill_id="t1")
    assert results and all(r["ok"] for r in results), [r for r in results if not r["ok"]]
    names = [r["name"] for r in results]
    assert (
        "every injected message quarantined" in names
        and "cleanup restored every table count" in names
    )
    assert any(s.startswith("INSERT INTO DB.BRONZE.") for s in conn.log)
    assert any(s.startswith("DELETE FROM DB.BRONZE.") for s in conn.log)


def test_cleanup_removes_a_wrongly_accepted_reference_row_completely(pipeline):
    silver, oracle = pipeline
    before = chaos_drill.snapshot(StandIn(silver).cursor(), "DB")
    good = json.dumps(
        {
            "event_id": "evt_chaos_1",
            "event_type": "merchant_upserted",
            "schema_version": 1,
            "sequence": 1,
            "event_time": "2026-01-05T10:00:00.000Z",
            "emitted_at": "2026-01-05T10:00:00.000Z",
            "merchant_id": "mch_t",
            "name": "Template Co",
            "country": "DE",
            "vertical": "retail",
            "risk_status": "low",
        }
    )
    silver.load_bronze("ref_merchants", [(good, "chaos/t9.ndjson", 1)])
    silver.run_reference()
    assert chaos_drill.snapshot(StandIn(silver).cursor(), "DB") != before  # it was accepted
    conn = StandIn(silver)
    for sql in chaos_drill.cleanup_statements("DB", "t9"):
        conn.execute(sql)
    assert chaos_drill.snapshot(conn.cursor(), "DB") == before


def test_the_drill_injects_only_messages_that_silver_rejects():
    faults = chaos_drill.injectable()
    assert len(faults) > 300
    assert all(f.silver_reason for f in faults) and all(f.loadable for f in faults)


def test_the_drill_flags_a_wrong_reason_and_still_cleans_up(pipeline, monkeypatch):
    silver, oracle = pipeline
    real = chaos_drill.injectable()
    first = real[0]
    bent = type(first)(first.topic, first.name, first.raw, "some_other_reason", first.layer)
    monkeypatch.setattr(chaos_drill, "injectable", lambda: [bent, *real[1:]])
    conn = StandIn(silver)
    results = {r["name"]: r for r in chaos_drill.run(conn, "DB", oracle, drill_id="t2")}
    assert not results["each one has the expected reason"]["ok"]
    assert results["cleanup restored every table count"]["ok"]


def test_the_drill_refuses_to_inject_when_the_pipeline_is_not_clean(pipeline):
    silver, oracle = pipeline
    silver.con.execute("DELETE FROM BRONZE.billing_fees WHERE rowid < 50")  # silently lose rows
    conn = StandIn(silver)
    results = chaos_drill.run(conn, "DB", oracle, drill_id="t3")
    assert [r["name"] for r in results if not r["ok"]]
    assert not any(s.startswith("INSERT INTO DB.BRONZE.") for s in conn.log)


def test_the_reconciliation_catches_rows_that_vanish_between_bronze_and_silver(pipeline):
    silver, _ = pipeline
    # Simulate a lost batch: the reference stream consumed rows that never reached the tables.
    silver.con.execute("DELETE FROM SILVER.silver_merchants_versions WHERE sequence = 1")
    row = silver.query(
        "SELECT unaccounted_rows FROM OPS.silver_reconciliation WHERE topic = 'ref.merchants'"
    )
    assert row[0][0] > 0


# --- verify_quality with scripted answers -------------------------------------------------------


class Scripted:
    def __init__(self, answers):
        self.answers = answers  # {sql fragment: (columns, rows)}

    def cursor(self):
        outer = self

        class Cur:
            description = None

            def execute(self, sql):
                for fragment, (cols, rows) in outer.answers.items():
                    if fragment in sql:
                        self.description, self._rows = [(c,) for c in cols], rows
                        return
                raise AssertionError(f"unexpected SQL: {sql[:80]}")

            def fetchall(self):
                return self._rows

            def close(self):
                pass

        return Cur()


def test_normalised_file_names_match_across_stage_and_history_spellings():
    assert verify_quality.norm("landing/ref_merchants/A.ndjson.gz") == "ref_merchants/a.ndjson.gz"
    assert verify_quality.norm("@DB.BRONZE.LANDING/ref_merchants/a.ndjson.gz") == (
        "ref_merchants/a.ndjson.gz"
    )


def test_a_staged_file_that_was_never_loaded_is_reported():
    conn = Scripted(
        {
            "LIST @": (
                ["name"],
                [("landing/ref_merchants/a.ndjson.gz",), ("landing/ref_merchants/b.ndjson.gz",)],
            ),
            "COPY_HISTORY": (
                ["file_name", "status", "first_error_message"],
                [("landing/ref_merchants/a.ndjson.gz", "Loaded", None)],
            ),
            "SELECT DISTINCT source_file": (["f"], []),
        }
    )
    results = {r["name"]: r for r in verify_quality.check_loads(conn, "DB")}
    assert not results["staged files never loaded"]["ok"]
    assert "b.ndjson.gz" in results["staged files never loaded"]["detail"]


def test_a_failed_load_is_reported_unless_a_later_load_succeeded():
    base = {
        "LIST @": (["name"], [("landing/ref_merchants/a.ndjson.gz",)]),
        "SELECT DISTINCT source_file": (["f"], []),
    }
    failed = Scripted(
        {
            **base,
            "COPY_HISTORY": (
                ["file_name", "status", "first_error_message"],
                [("landing/ref_merchants/a.ndjson.gz", "Load failed", "bad json")],
            ),
        }  # fmt: skip
    )
    got = {r["name"]: r for r in verify_quality.check_loads(failed, "DB")}
    assert not got["load failures with no later success"]["ok"]
    recovered = Scripted(
        {
            **base,
            "COPY_HISTORY": (
                ["file_name", "status", "first_error_message"],
                [
                    ("landing/ref_merchants/a.ndjson.gz", "Load failed", "x"),
                    ("landing/ref_merchants/a.ndjson.gz", "Loaded", None),
                ],
            ),
        }  # fmt: skip
    )
    assert all(r["ok"] for r in verify_quality.check_loads(recovered, "DB"))


def test_reconciliation_failure_is_a_failure():
    row = ("payments.fees", 10, 1, 2, 7, 7, 7, 3, 0)
    cols = [
        "topic", "bronze_rows", "rejected_rows", "duplicate_rows", "unique_valid_events",
        "entities_expected", "entities_actual", "unaccounted_rows", "missing_entities",
    ]  # fmt: skip
    results = verify_quality.check_reconciliation(
        Scripted({"silver_reconciliation": (cols, [row])}), "DB"
    )
    assert [r["name"] for r in results if not r["ok"]] == ["payments.fees: unaccounted_rows"]


def _golden_with_poison(tmp_path):
    """A tiny golden folder: per topic, one valid line and three refused ones (two identical)."""
    from generator.config import TOPICS
    from silver.faults import TEMPLATES

    golden = tmp_path / "golden"
    for topic in TOPICS:
        (golden / topic).mkdir(parents=True)
        good = json.dumps(TEMPLATES[topic])
        lines = [good, "[1,2,3]", "[1,2,3]", good[:30]]
        (golden / topic / "2026-01-01.ndjson").write_text("\n".join(lines) + "\n")
    return golden


def _write_dead_letters(dlq, topics, times=1, drop=None):
    dlq.mkdir(exist_ok=True)
    for topic in topics:
        rows = [
            "[1,2,3]",
            "[1,2,3]",
            json.dumps(__import__("silver.faults").faults.TEMPLATES[topic])[:30],
        ]
        if topic == drop:
            rows = rows[:2]
        lines = [json.dumps({"reason": "x", "key": None, "value": v}) for v in rows * times]
        (dlq / f"{topic}.ndjson").write_text("\n".join(lines) + "\n")


def test_dead_letters_must_match_the_refused_deliveries_even_when_staged_twice(tmp_path):
    from generator.config import TOPICS

    golden = _golden_with_poison(tmp_path)
    for times in (1, 2, 3):  # `make stage` appends again on every run
        _write_dead_letters(tmp_path / "dlq", TOPICS, times=times)
        results = verify_quality.check_dead_letters(golden, tmp_path / "dlq")
        assert all(r["ok"] for r in results), results
        assert all(r["expected"] == 3 and r["actual"] == 3 for r in results)


def test_a_stage_run_that_stopped_part_way_is_a_warning_not_a_failure(tmp_path):
    from generator.config import TOPICS

    golden = _golden_with_poison(tmp_path)
    _write_dead_letters(tmp_path / "dlq", TOPICS, times=2)
    path = tmp_path / "dlq" / f"{TOPICS[0]}.ndjson"
    first = path.read_text().splitlines()[0]
    path.write_text(path.read_text() + first + "\n")  # one extra copy from an interrupted run
    results = verify_quality.check_dead_letters(golden, tmp_path / "dlq")
    hard = [r for r in results if not r["ok"] and r["severity"] == "fail"]
    soft = [r for r in results if not r["ok"] and r["severity"] == "warn"]
    assert not hard
    assert [r["name"].split(":")[0] for r in soft] == [TOPICS[0]]


def test_an_unexpected_dead_letter_is_a_failure(tmp_path):
    from generator.config import TOPICS

    golden = _golden_with_poison(tmp_path)
    _write_dead_letters(tmp_path / "dlq", TOPICS, times=1)
    path = tmp_path / "dlq" / f"{TOPICS[2]}.ndjson"
    path.write_text(path.read_text() + json.dumps({"reason": "x", "value": "{}"}) + "\n")
    bad = [r for r in verify_quality.check_dead_letters(golden, tmp_path / "dlq") if not r["ok"]]
    assert [r["name"].split(":")[0] for r in bad] == [TOPICS[2]]


def test_a_missing_dead_letter_is_reported(tmp_path):
    from generator.config import TOPICS

    golden = _golden_with_poison(tmp_path)
    _write_dead_letters(tmp_path / "dlq", TOPICS, times=2, drop=TOPICS[3])
    bad = [r for r in verify_quality.check_dead_letters(golden, tmp_path / "dlq") if not r["ok"]]
    assert [r["name"].split(":")[0] for r in bad] == [TOPICS[3]]


def test_a_folder_that_is_not_there_is_a_warning_not_a_failure(tmp_path):
    missing = verify_quality.check_dead_letters(tmp_path, tmp_path / "nope")
    assert missing[0]["severity"] == "warn"


def test_failure_queries_that_error_become_warnings_not_crashes():
    class Broken:
        def cursor(self):
            raise RuntimeError("no such function")

    results = verify_quality.check_failures(Broken(), "DB")
    assert results and all(r["severity"] == "warn" and not r["ok"] for r in results)

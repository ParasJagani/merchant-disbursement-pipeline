import gzip
import json
from collections import Counter
from pathlib import Path

import pytest

from file_writer.dlq import LocalDeadLetters
from file_writer.naming import (
    StagedFile,
    backfill_file_name,
    gzip_lines,
    kafka_file_name,
    topic_path,
)
from file_writer.stagers import LocalStager, SnowflakeStager
from file_writer.writer import FileWriter, Record, stage_backfill
from generator.__main__ import main as generate
from generator.chaos import deliver
from generator.config import TOPICS, GeneratorConfig
from generator.events import Message, parse_iso_ms
from generator.reference import replay_to_oracle
from generator.schema import validate_message
from generator.simulate import simulate

CFG = GeneratorConfig(days=8, tx_per_day=150)


@pytest.fixture(scope="module")
def messages():
    return deliver(CFG, simulate(CFG).events)[0]


def records_for(messages, topic="payments.transactions"):
    return [
        Record(m.topic, 0, i, m.key, m.value, m.emitted_at_ms)
        for i, m in enumerate(x for x in messages if x.topic == topic)
    ]


class SpyStager(LocalStager):
    def __init__(self, root, log):
        super().__init__(root)
        self.log = log

    def stage(self, file):
        self.log.append(("stage", file.name))
        return super().stage(file)


# --- naming -------------------------------------------------------------------------------


def test_names_are_deterministic_and_stage_friendly():
    a = kafka_file_name("payments.transactions", 2, 10, 99, 1_767_225_600_000)
    assert a == kafka_file_name("payments.transactions", 2, 10, 99, 1_767_225_600_000)
    assert a.startswith("payments_transactions/dt=2026-01-01/")
    assert "." not in a.split("/")[0]
    assert a.endswith("p02__000000000010-000000000099.ndjson.gz")
    assert backfill_file_name("ref.accounts", "2026-01-05", 3) == (
        "ref_accounts/dt=2026-01-05/ref_accounts__2026-01-05__part-0003.ndjson.gz"
    )
    assert topic_path("ledger.adjustments") == "ledger_adjustments"


def test_gzip_is_byte_for_byte_reproducible():
    lines = ['{"a":1}', '{"a":2}']
    assert gzip_lines(lines) == gzip_lines(lines)
    assert gzip.decompress(gzip_lines(lines)).decode().splitlines() == lines


# --- the writer ---------------------------------------------------------------------------


def test_invalid_messages_go_to_the_dead_letter_store_not_to_files(tmp_path, messages):
    stager = LocalStager(tmp_path / "stage")
    dlq = LocalDeadLetters(tmp_path / "dlq")
    writer = FileWriter(stager, dlq, max_messages=500)
    records = records_for(messages)
    for r in records:
        writer.handle(r)
    writer.close()

    expected_bad = sum(1 for r in records if not validate_message(r.topic, r.value)[0])
    assert writer.stats.rejected == expected_bad
    assert writer.stats.valid + writer.stats.rejected == len(records)
    staged_lines = [
        line
        for f in (tmp_path / "stage").rglob("*.ndjson.gz")
        for line in gzip.decompress(f.read_bytes()).decode().splitlines()
    ]
    assert len(staged_lines) == writer.stats.valid
    assert all(validate_message("payments.transactions", ln)[0] for ln in staged_lines)
    if expected_bad:
        rows = [
            json.loads(x)
            for x in (tmp_path / "dlq" / "payments.transactions.ndjson").read_text().splitlines()
        ]
        assert len(rows) == expected_bad and all(r["reason"] for r in rows)


def test_offsets_are_committed_only_after_the_file_is_staged(tmp_path, messages):
    log = []
    writer = FileWriter(
        SpyStager(tmp_path / "s", log),
        LocalDeadLetters(tmp_path / "d"),
        commit=lambda offsets: log.append(("commit", dict(offsets))),
        max_messages=100,
    )
    for r in records_for(messages)[:250]:
        writer.handle(r)
    writer.close()
    assert log[0][0] == "stage"
    for i, entry in enumerate(log):
        if entry[0] == "commit":
            assert log[i - 1][0] in {"stage", "commit"}
    committed = [e[1][("payments.transactions", 0)] for e in log if e[0] == "commit"]
    assert committed == sorted(committed)
    assert committed[-1] == 250  # next offset to read after the last record


def test_replaying_the_same_records_creates_the_same_files_and_stages_nothing_new(
    tmp_path, messages
):
    records = records_for(messages)[:1200]
    first = FileWriter(
        LocalStager(tmp_path / "s"), LocalDeadLetters(tmp_path / "d1"), max_messages=300
    )
    for r in records:
        first.handle(r)
    first.close()
    names = sorted(p.name for p in (tmp_path / "s").rglob("*.gz"))
    second = FileWriter(
        LocalStager(tmp_path / "s"), LocalDeadLetters(tmp_path / "d2"), max_messages=300
    )
    for r in records:
        second.handle(r)
    second.close()
    assert second.stats.files_new == 0 and second.stats.files_skipped == first.stats.files_new
    assert sorted(p.name for p in (tmp_path / "s").rglob("*.gz")) == names


def test_open_batches_are_flushed_after_max_age(tmp_path, messages):
    now = [0.0]
    writer = FileWriter(
        LocalStager(tmp_path / "s"),
        LocalDeadLetters(tmp_path / "d"),
        max_messages=10_000,
        max_age_seconds=30,
        clock=lambda: now[0],
    )
    for r in records_for(messages)[:20]:
        writer.handle(r)
    writer.tick()
    assert not list((tmp_path / "s").rglob("*.gz"))
    now[0] = 31.0
    writer.tick()
    assert len(list((tmp_path / "s").rglob("*.gz"))) == 1


def test_a_poison_message_still_advances_the_committed_offset(tmp_path):
    committed = {}
    writer = FileWriter(
        LocalStager(tmp_path / "s"),
        LocalDeadLetters(tmp_path / "d"),
        commit=lambda o: committed.update(o),
    )
    writer.handle(Record("payments.refunds", 0, 7, None, "{not json", 0))
    writer.tick()
    assert committed == {("payments.refunds", 0): 8}


# --- backfill staging ---------------------------------------------------------------------


@pytest.fixture(scope="module")
def backfill(tmp_path_factory):
    out = tmp_path_factory.mktemp("golden")
    generate(["backfill", "--days", "8", "--tx-per-day", "150", "--out", str(out)])
    return out, json.loads((out / "manifest.json").read_text())


def test_stage_backfill_matches_the_manifest_and_is_idempotent(tmp_path, backfill):
    src, manifest = backfill
    stage = LocalStager(tmp_path / "stage")
    stats = stage_backfill(src, stage, LocalDeadLetters(tmp_path / "dlq"), lines_per_file=2000)
    assert stats.consumed == sum(manifest["delivered_messages_by_topic"].values())
    assert stats.valid == sum(manifest["valid_messages_by_topic"].values())
    assert stats.rejected == manifest["delivery_effects"]["poison"]
    assert stats.files_new > 0 and stats.files_skipped == 0

    again = stage_backfill(src, stage, LocalDeadLetters(tmp_path / "dlq2"), lines_per_file=2000)
    assert again.files_new == 0 and again.files_skipped == stats.files_new


def test_staged_files_lose_nothing_the_oracle_needs(tmp_path, backfill):
    src, manifest = backfill
    stage_dir = tmp_path / "stage"
    stage_backfill(src, LocalStager(stage_dir), LocalDeadLetters(tmp_path / "dlq"))
    rebuilt, per_topic = [], Counter()
    for path in sorted(stage_dir.rglob("*.ndjson.gz")):
        topic = next(t for t in TOPICS if topic_path(t) == path.relative_to(stage_dir).parts[0])
        for line in gzip.decompress(path.read_bytes()).decode().splitlines():
            per_topic[topic] += 1
            rebuilt.append(Message(topic, None, line, parse_iso_ms(json.loads(line)["emitted_at"])))
    assert dict(per_topic) == {t: n for t, n in manifest["valid_messages_by_topic"].items() if n}
    ordered = [
        Message(m.topic, m.key, m.value, m.emitted_at_ms, order=i)
        for i, m in enumerate(sorted(rebuilt, key=lambda m: m.emitted_at_ms))
    ]
    expected = Path(src / "oracle" / "daily_disbursement.csv").read_text().splitlines()[1:]
    got = replay_to_oracle(ordered)
    assert len(got) == len(expected)
    assert got == build_oracle_from_csv(expected)


def build_oracle_from_csv(lines):
    import csv

    from generator.oracle import COLUMNS

    rows = []
    for rec in csv.DictReader([",".join(COLUMNS), *lines]):
        rows.append({k: (v if k in COLUMNS[:4] else int(v)) for k, v in rec.items()})
    return rows


def test_dead_letter_files_hold_exactly_the_rejected_lines(tmp_path, backfill):
    src, manifest = backfill
    dlq_dir = tmp_path / "dlq"
    stage_backfill(src, LocalStager(tmp_path / "stage"), LocalDeadLetters(dlq_dir))
    lines = sum(len(p.read_text().splitlines()) for p in dlq_dir.glob("*.ndjson"))
    assert lines == manifest["delivery_effects"]["poison"]


# --- Snowflake stager (fake connection) --------------------------------------------------------


class FakeCursor:
    def __init__(self, status):
        self.executed, self.status = [], status

    def execute(self, sql):
        self.executed.append(sql)

    def fetchone(self):
        return ("f.gz", "f.gz", 1, 1, "NONE", "NONE", self.status, "")

    def close(self):
        pass


class FakeConnection:
    def __init__(self, status="UPLOADED"):
        self.cursor_obj = FakeCursor(status)

    def cursor(self):
        return self.cursor_obj


def test_snowflake_stager_puts_without_overwrite_and_reports_skips():
    file = StagedFile("payments_refunds/dt=2026-01-02/x.ndjson.gz", b"abc", 1, "payments.refunds")
    conn = FakeConnection("UPLOADED")
    assert SnowflakeStager(conn).stage(file) is True
    sql = conn.cursor_obj.executed[0]
    assert (
        sql.startswith("PUT 'file://") and "@BRONZE.LANDING/payments_refunds/dt=2026-01-02" in sql
    )
    assert "AUTO_COMPRESS=FALSE" in sql and "OVERWRITE=FALSE" in sql
    assert SnowflakeStager(FakeConnection("SKIPPED")).stage(file) is False


def test_snowflake_stage_is_database_qualified(monkeypatch):
    """A PUT needs a database; the stager must not rely on the session having one."""
    from argparse import Namespace

    from file_writer import __main__ as cli

    monkeypatch.setenv("SNOWFLAKE_DATABASE", "SOME_DB")
    monkeypatch.setattr(cli, "connect_from_env", lambda: FakeConnection("UPLOADED"))
    stager = cli._stager(Namespace(stager="snowflake"))
    assert stager.stage_name == "@SOME_DB.BRONZE.LANDING"

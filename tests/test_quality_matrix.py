"""Every kind of bad message, through the file writer's validator and through silver.

The contract (see silver/faults.py): silver is at least as strict as the writer, every rejection
has a stable reason, and unusual-but-valid messages are NOT rejected.
"""

import json
from collections import Counter

import pytest

from generator.schema import validate_message
from silver.faults import TEMPLATES, catalog
from silver.local import LocalSilver
from silver.spec import EVENT_TOPICS, REFERENCE_TABLES

TABLES = {t.topic: t for t in (*REFERENCE_TABLES, *EVENT_TOPICS)}
FAULTS = catalog()


@pytest.fixture(scope="module")
def verdicts():
    """Load every loadable fault into bronze, one row each, and read back what silver decided."""
    silver = LocalSilver()
    silver.deploy()
    rows: dict[str, list] = {}
    where: dict[tuple[str, int], object] = {}
    for fault in FAULTS:
        if not fault.loadable:
            continue
        table = TABLES[fault.topic].bronze
        batch = rows.setdefault(table, [])
        batch.append((fault.raw, "faults.ndjson.gz", len(batch) + 1))
        where[(table, len(batch))] = fault
    for table, batch in rows.items():
        silver.load_bronze(table, batch)
    silver.run_reference()
    decided = {}
    for t in TABLES.values():
        rejected = dict(
            silver.query(f"SELECT source_row, reject_reason FROM QUARANTINE.rejected_{t.name}")
        )
        for (table, row), fault in where.items():
            if table == t.bronze:
                decided[id(fault)] = rejected.get(row)
    return decided


def test_the_catalog_is_large_and_covers_every_topic_and_layer():
    assert len(FAULTS) > 300
    assert {f.topic for f in FAULTS} == set(TABLES)
    assert {f.layer for f in FAULTS} == {"both", "silver_only", "accepted", "writer_only"}


def test_the_templates_are_valid_for_the_writer():
    for topic, doc in TEMPLATES.items():
        assert validate_message(topic, json.dumps(doc)) == (True, None), topic


@pytest.mark.parametrize("fault", [f for f in FAULTS if f.layer == "writer_only"], ids=str)
def test_unparseable_text_is_stopped_by_the_writer(fault):
    assert validate_message(fault.topic, fault.raw) == (False, "invalid_json")


def test_every_message_gets_exactly_the_reason_the_catalog_expects(verdicts):
    wrong = [
        (f.topic, f.name, f.silver_reason, verdicts[id(f)])
        for f in FAULTS
        if f.loadable and verdicts[id(f)] != f.silver_reason
    ]
    assert wrong == []


def test_the_declared_layer_matches_what_actually_happens(verdicts):
    """The declared layer (see silver/faults.py) is what really happens."""
    mismatches = []
    for f in FAULTS:
        if not f.loadable:
            continue
        writer_rejects = not validate_message(f.topic, f.raw)[0]
        silver_rejects = verdicts[id(f)] is not None
        actual = {
            (True, True): "both",
            (False, True): "silver_only",
            (False, False): "accepted",
            (True, False): "writer_only",
        }[(writer_rejects, silver_rejects)]
        if actual != f.layer:
            mismatches.append((f.topic, f.name, f.layer, actual))
    assert mismatches == []


def test_silver_is_never_more_lenient_than_the_writer(verdicts):
    lenient = [
        (f.topic, f.name)
        for f in FAULTS
        if f.loadable and not validate_message(f.topic, f.raw)[0] and verdicts[id(f)] is None
    ]
    assert lenient == []


def test_the_catalog_exercises_a_wide_range_of_reasons(verdicts):
    reasons = Counter(r for r in verdicts.values() if r)
    assert len(reasons) >= 30

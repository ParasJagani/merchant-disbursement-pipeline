"""Zero rows lost silently, under heavy chaos.

Every delivered message must end up in exactly one place: dead-lettered by the writer, rejected
into quarantine, counted as a duplicate, or kept as a unique event. Every business event the
generator produced must be in silver. And none of that may depend on the order, the batching or
the repetition of delivery.
"""

import json
import random
from collections import Counter, defaultdict
from dataclasses import replace

import pytest

from generator.chaos import deliver
from generator.config import GeneratorConfig
from generator.reference import replay_to_oracle
from generator.schema import validate_message
from generator.simulate import simulate
from silver.local import LocalSilver
from silver.spec import EVENT_TOPICS, REFERENCE_TABLES

ALL = (*REFERENCE_TABLES, *EVENT_TOPICS)
BRONZE = {t.topic: t.bronze for t in ALL}
HEAVY = replace(
    GeneratorConfig(days=12, tx_per_day=150),
    duplicate_rate=0.25,
    out_of_order_rate=0.30,
    poison_rate=0.03,
)


@pytest.fixture(scope="module")
def world():
    result = simulate(HEAVY)
    messages, stats = deliver(HEAVY, result.events)
    return result, sorted(messages, key=lambda m: m.order), stats


def build(messages, *, route_invalid=True, shuffle_seed=None, batches=1, copies=1):
    """Load messages into bronze (optionally shuffled, split into batches, or delivered twice)."""
    silver = LocalSilver()
    silver.deploy()
    rows = defaultdict(list)
    for m in messages:
        if route_invalid and not validate_message(m.topic, m.value)[0]:
            continue
        try:
            json.loads(m.value)
        except ValueError:
            continue
        rows[BRONZE[m.topic]].append(m.value)
    for table, values in rows.items():
        values = values * copies
        if shuffle_seed is not None:
            random.Random(shuffle_seed).shuffle(values)
        size = -(-len(values) // batches)
        for n in range(batches):
            chunk = values[n * size : (n + 1) * size]
            silver.load_bronze(
                table, [(v, f"batch{n}.ndjson.gz", i + 1) for i, v in enumerate(chunk)]
            )
            silver.run_reference()
    return silver


def snapshot(silver):
    tables = [f"SILVER.silver_{t.name}" for t in ALL]
    tables += [f"SILVER.silver_{t.name}_history" for t in REFERENCE_TABLES]
    return {t: silver.query(f"SELECT * FROM {t} ORDER BY ALL") for t in tables}


def recon(silver):
    cols = (
        "topic, bronze_rows, rejected_rows, duplicate_rows, unique_valid_events, "
        "unaccounted_rows, missing_entities"
    )
    return {r[0]: r for r in silver.query(f"SELECT {cols} FROM OPS.silver_reconciliation")}


def test_the_world_really_is_chaotic(world):
    _, _, stats = world
    assert stats["duplicates"] > 300 and stats["out_of_order"] > 400 and stats["poison"] > 40


def test_every_delivered_message_is_accounted_for(world):
    result, messages, _ = world
    delivered = Counter(m.topic for m in messages)
    dead = Counter(m.topic for m in messages if not validate_message(m.topic, m.value)[0])
    silver = build(messages)
    for topic, row in recon(silver).items():
        _, bronze, rejected, dups, unique, unaccounted, missing = row
        assert delivered[topic] == dead[topic] + bronze, topic
        assert bronze == rejected + dups + unique, topic
        assert unaccounted == 0 and missing == 0, topic


def test_every_business_event_the_generator_made_is_in_silver_exactly_once(world):
    result, messages, _ = world
    clean = Counter(e.topic for e in result.events)
    for topic, row in recon(build(messages)).items():
        assert row[4] == clean[topic], topic  # unique_valid_events


def test_malformed_messages_that_get_past_the_writer_are_quarantined_not_lost(world):
    _, messages, _ = world
    silver = build(messages, route_invalid=False)
    parsable_bad = 0
    for m in messages:
        if not validate_message(m.topic, m.value)[0]:
            try:
                json.loads(m.value)
                parsable_bad += 1
            except ValueError:
                pass
    quarantined = sum(r[2] for r in recon(silver).values())
    assert quarantined == parsable_bad > 0
    assert all(r[5] == 0 for r in recon(silver).values())  # still nothing unaccounted
    assert silver.ledger() == replay_to_oracle(messages)


def test_the_ledger_equals_the_oracle_under_heavy_chaos(world):
    _, messages, _ = world
    assert build(messages).ledger() == replay_to_oracle(messages)


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_order_and_batching_of_delivery_do_not_change_the_result(world, seed):
    _, messages, _ = world
    baseline = snapshot(build(messages))
    shuffled = build(messages, shuffle_seed=seed, batches=3)
    assert snapshot(shuffled) == baseline


def test_replaying_everything_changes_nothing_but_the_duplicate_count(world):
    _, messages, _ = world
    once, twice = build(messages), build(messages, copies=2)
    assert snapshot(twice) == snapshot(once)
    r1, r2 = recon(once), recon(twice)
    for topic in r1:
        assert r2[topic][1] == 2 * r1[topic][1]  # bronze doubled
        assert r2[topic][4] == r1[topic][4]  # unique events unchanged
        assert r2[topic][5] == 0  # nothing unaccounted
    assert twice.ledger() == once.ledger()

import json
from collections import Counter, defaultdict
from dataclasses import replace

import pytest

from generator.__main__ import main
from generator.chaos import POISON_KINDS, deliver
from generator.config import DAY_MS, REFERENCE_TOPICS, TOPICS, GeneratorConfig
from generator.events import dumps, parse_iso_ms
from generator.oracle import build_oracle
from generator.reference import replay_to_oracle
from generator.schema import validate_message
from generator.simulate import simulate
from generator.sinks import replay_to_producer

BIG = GeneratorConfig(days=45, tx_per_day=400)


@pytest.fixture(scope="module")
def run():
    result = simulate(BIG)
    messages, stats = deliver(BIG, result.events)
    return result, messages, stats


# --- shape and determinism ---------------------------------------------------------------


def test_all_seven_topics_are_produced(run):
    result, _, _ = run
    counts = Counter(e.topic for e in result.events)
    assert set(counts) == set(TOPICS)
    assert all(counts[t] > 0 for t in TOPICS)


def test_same_seed_gives_identical_output():
    cfg = GeneratorConfig(days=6, tx_per_day=120)
    a = deliver(cfg, simulate(cfg).events)[0]
    b = deliver(cfg, simulate(cfg).events)[0]
    assert [m.value for m in a] == [m.value for m in b]


def test_different_seed_gives_different_output():
    a = GeneratorConfig(days=6, tx_per_day=120, seed=1)
    b = replace(a, seed=2)
    assert [m.value for m in deliver(a, simulate(a).events)[0]] != [
        m.value for m in deliver(b, simulate(b).events)[0]
    ]


def test_pinned_event_counts_for_small_run():
    # Counts depend only on the seeded random stream, so they are stable across platforms.
    cfg = GeneratorConfig(days=5, tx_per_day=100, seed=7)
    counts = Counter(e.topic for e in simulate(cfg).events)
    assert counts == EXPECTED_SMALL_COUNTS


EXPECTED_SMALL_COUNTS = Counter(
    {
        "payments.transactions": 1046,
        "billing.fees": 962,
        "ref.accounts": 72,
        "ref.merchants": 37,
        "ledger.adjustments": 11,
        "payments.refunds": 2,
    }
)


def test_world_is_fictional_and_well_formed(run):
    merchants = run[0].merchants
    assert len({m.name for m in merchants}) == len(merchants) == 30
    assert all(1 <= len(m.accounts) <= 4 for m in merchants)
    assert 50 <= sum(len(m.accounts) for m in merchants) <= 90


def test_hot_merchants_dominate_volume(run):
    per_merchant = Counter(
        e.payload["merchant_id"]
        for e in run[0].events
        if e.topic == "payments.transactions" and e.event_type == "authorized"
    )
    top4 = sum(n for _, n in per_merchant.most_common(4))
    assert 0.40 <= top4 / sum(per_merchant.values()) <= 0.65


# --- envelope and schema -----------------------------------------------------------------


def test_every_clean_event_passes_validation(run):
    for e in run[0].events:
        ok, reason = validate_message(e.topic, dumps(e.envelope(e.event_time_ms + 500)))
        assert ok, (e.topic, e.event_type, reason)


def test_reference_topics_carry_op_and_others_do_not(run):
    for e in run[0].events:
        if e.topic in REFERENCE_TOPICS:
            assert e.op in {"upsert", "delete"}
        else:
            assert e.op is None


def test_sequences_are_contiguous_and_follow_event_time(run):
    by_entity = defaultdict(list)
    for e in run[0].events:
        by_entity[(e.topic, e.entity_id)].append(e)
    for events in by_entity.values():
        ordered = sorted(events, key=lambda e: e.sequence)
        assert [e.sequence for e in ordered] == list(range(1, len(ordered) + 1))
        times = [e.event_time_ms for e in ordered]
        assert times == sorted(times)


def test_money_is_always_integer_minor_units(run):
    for e in run[0].events:
        amount = e.payload.get("amount")
        if amount is not None:
            assert type(amount) is int


def test_schema_drift_adds_one_optional_field_from_the_drift_day(run):
    drift_ms = BIG.start_ms + BIG.drift_day * DAY_MS
    for e in run[0].events:
        if e.topic != "payments.transactions":
            assert "channel" not in e.payload
        elif e.event_time_ms >= drift_ms:
            assert e.schema_version == 2 and "channel" in e.payload
        else:
            assert e.schema_version == 1 and "channel" not in e.payload


# --- business rules ----------------------------------------------------------------------


def _by_topic(run):
    out = defaultdict(list)
    for e in run[0].events:
        out[e.topic].append(e)
    return out


def test_refunds_never_exceed_the_sale(run):
    topics = _by_topic(run)
    sales = {
        e.entity_id: e.payload["amount"]
        for e in topics["payments.transactions"]
        if e.event_type == "captured"
    }
    refunded = Counter()
    for e in topics["payments.refunds"]:
        assert e.payload["transaction_id"] in sales
        refunded[e.payload["transaction_id"]] += e.payload["amount"]
    assert refunded
    assert all(refunded[t] <= sales[t] for t in refunded)


def test_fees_never_exceed_the_sale_and_both_types_exist(run):
    topics = _by_topic(run)
    sales = {
        e.entity_id: e.payload["amount"]
        for e in topics["payments.transactions"]
        if e.event_type == "captured"
    }
    fees = Counter()
    types = set()
    for e in topics["billing.fees"]:
        fees[e.payload["transaction_id"]] += e.payload["amount"]
        types.add(e.payload["fee_type"])
    assert types == {"platform", "pass_through"}
    assert all(fees[t] <= sales[t] for t in fees)


def test_disputes_hit_captured_sales_five_to_forty_five_days_later(run):
    topics = _by_topic(run)
    captured = {
        e.entity_id: e.event_time_ms
        for e in topics["payments.transactions"]
        if e.event_type == "captured"
    }
    opened = [e for e in topics["payments.disputes"] if e.event_type == "dispute_opened"]
    assert opened
    for e in opened:
        delay_days = (e.event_time_ms - captured[e.payload["transaction_id"]]) / DAY_MS
        assert 5 <= delay_days <= 45


def test_offboarded_merchants_have_deletes_and_no_new_sales_afterwards(run):
    result = run[0]
    deleted = {m.merchant_id: m.deleted_ms for m in result.merchants if m.deleted_ms}
    assert len(deleted) == BIG.offboard_count
    deletes = {
        e.entity_id for e in result.events if e.topic == "ref.merchants" and e.op == "delete"
    }
    assert deletes == set(deleted)
    for e in result.events:
        if e.topic == "payments.transactions" and e.event_type == "authorized":
            m = e.payload["merchant_id"]
            if m in deleted:
                assert e.event_time_ms < deleted[m]


def test_fee_plan_changes_are_applied_to_later_fees(run):
    result = run[0]
    changed = [a for m in result.merchants for a in m.accounts if len(a.plan_history) > 1]
    assert changed
    seen_plans = {
        e.payload["fee_subtype"]
        for e in result.events
        if e.topic == "billing.fees" and e.payload["fee_type"] == "platform"
    }
    assert {"standard", "growth", "enterprise"} <= seen_plans


# --- delivery effects --------------------------------------------------------------------


def test_delivery_rates_match_configuration(run):
    result, _, stats = run
    n = len(result.events)
    assert 0.010 <= stats["duplicates"] / n <= 0.022
    assert 0.014 <= stats["out_of_order"] / n <= 0.026
    assert stats["poison"] == round(BIG.poison_rate * n)


def test_out_of_order_events_really_arrive_out_of_order(run):
    _, messages, _ = run
    last_seq = {}
    inversions = 0
    for m in messages:
        try:
            doc = json.loads(m.value)
        except ValueError:
            continue
        if not isinstance(doc, dict) or "entity_id" not in doc:
            continue
        key = (m.topic, doc["entity_id"])
        if doc["sequence"] < last_seq.get(key, 0):
            inversions += 1
        last_seq[key] = max(last_seq.get(key, 0), doc["sequence"])
    assert inversions > 0


def test_duplicates_share_an_event_id_and_poison_is_rejected(run):
    _, messages, stats = run
    invalid = Counter()
    ids = Counter()
    for m in messages:
        ok, reason = validate_message(m.topic, m.value)
        if ok:
            ids[json.loads(m.value)["event_id"]] += 1
        else:
            invalid[reason] += 1
    assert sum(invalid.values()) == stats["poison"]
    assert sum(1 for n in ids.values() if n > 1) == stats["duplicates"]
    for kind in POISON_KINDS:
        assert stats[f"poison:{kind}"] > 0


# --- the oracle --------------------------------------------------------------------------


def test_oracle_reconciles_with_its_own_columns(run):
    for row in build_oracle(run[0].ledger):
        assert row["disbursed_total"] == (
            row["sales_amount"]
            - row["refund_amount"]
            - row["dispute_amount"]
            - row["platform_fee_amount"]
            - row["pass_through_fee_amount"]
            + row["adjustment_amount"]
        )


def test_replaying_the_messy_delivered_stream_reproduces_the_oracle(run):
    result, messages, _ = run
    assert replay_to_oracle(messages) == build_oracle(result.ledger)


def test_oracle_covers_every_money_movement(run):
    result = run[0]
    oracle = build_oracle(result.ledger)
    assert sum(r["disbursed_total"] for r in oracle) == sum(e.amount_signed for e in result.ledger)
    assert any(r["dispute_amount"] != 0 for r in oracle)
    assert any(r["adjustment_amount"] != 0 for r in oracle)
    assert any(r["refund_amount"] != 0 for r in oracle)


def test_posting_date_follows_event_time_not_arrival(run):
    # A capture delivered after midnight (out of order) must still post on its capture date.
    result, messages, _ = run
    late = [e for e in result.ledger if e.entry_type == "SALE"]
    assert late and all(len(e.posting_date) == 10 for e in late)
    arrived = {
        json.loads(m.value)["event_id"]: m
        for m in messages
        if validate_message(m.topic, m.value)[0]
    }
    shifted = 0
    for e in result.events:
        if e.event_type == "captured":
            m = arrived[e.event_id]
            if m.emitted_at_ms // DAY_MS != e.event_time_ms // DAY_MS:
                shifted += 1
                assert parse_iso_ms(json.loads(m.value)["event_time"]) == e.event_time_ms
    assert shifted > 0


# --- sinks and command line --------------------------------------------------------------


def test_backfill_cli_writes_files_oracle_and_manifest_deterministically(tmp_path):
    args = ["backfill", "--days", "6", "--tx-per-day", "80"]
    assert main([*args, "--out", str(tmp_path / "a")]) == 0
    assert main([*args, "--out", str(tmp_path / "b")]) == 0
    a = json.loads((tmp_path / "a" / "manifest.json").read_text())
    b = json.loads((tmp_path / "b" / "manifest.json").read_text())
    assert a == b
    assert (tmp_path / "a" / "oracle" / "daily_disbursement.csv").is_file()
    assert (tmp_path / "a" / "payments.transactions").is_dir()
    assert set(a["clean_events_by_topic"]) == set(TOPICS)
    main([*args, "--seed", "99", "--out", str(tmp_path / "c")])
    c = json.loads((tmp_path / "c" / "manifest.json").read_text())
    assert c["dataset_sha256"] != a["dataset_sha256"]


class FakeProducer:
    def __init__(self):
        self.sent, self.flushed = [], False

    def produce(self, topic, key=None, value=None):
        self.sent.append((topic, key, value))

    def poll(self, timeout):
        pass

    def flush(self):
        self.flushed = True


def test_live_replay_publishes_in_order_and_throttles():
    cfg = GeneratorConfig(days=2, tx_per_day=30)
    messages, _ = deliver(cfg, simulate(cfg).events)
    producer, sleeps = FakeProducer(), []
    sent = replay_to_producer(producer, messages, rate=50, sleep=sleeps.append)
    assert sent == len(messages) == len(producer.sent) and producer.flushed
    assert [v for _, _, v in producer.sent] == [m.value for m in messages]
    assert set(sleeps) == {0.02}

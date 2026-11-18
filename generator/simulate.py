"""Business simulation: builds clean events and the ledger entries behind them.

The ledger entries are the ground truth. They are created next to the events they belong to
but are never derived from the delivered stream, so the oracle stays independent of the pipeline.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from .config import DAY_MS, GeneratorConfig
from .events import Event, LedgerEntry, SequenceBook, make_event_id, posting_date
from .rng import Rng, cumulative
from .world import FEE_PLANS, PASS_THROUGH, Merchant, build_world

MINUTE = 60_000
HOUR = 3_600_000
WEEKDAY_FACTOR = [1.0, 1.0, 1.0, 1.05, 1.2, 1.3, 1.1]  # Monday first
HOUR_WEIGHTS = [1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 7, 5, 3, 2]
ADJUSTMENT_REASONS = [
    "goodwill_credit",
    "fee_correction",
    "manual_reversal",
    "reserve_release",
    "penalty",
]
APPROVERS = ["ops.alvarez", "ops.nakamura", "ops.okafor", "ops.lindqvist"]
RISK = ["low", "medium", "high"]


@dataclass
class SimulationResult:
    merchants: list[Merchant]
    events: list[Event]  # clean events in creation order
    ledger: list[LedgerEntry]


def _round_div(numerator: int, denominator: int) -> int:
    return (numerator * 2 + denominator) // (denominator * 2)


def platform_fee(amount: int, plan: str) -> int:
    bps, fixed = FEE_PLANS[plan]
    return min(amount, _round_div(amount * bps, 10_000) + fixed)


def pass_through_fee(amount: int, subtype: str, already_taken: int) -> int:
    bps, fixed = PASS_THROUGH[subtype]
    return min(amount - already_taken, _round_div(amount * bps, 10_000) + fixed)


class Simulation:
    def __init__(self, cfg: GeneratorConfig) -> None:
        self.cfg = cfg
        self.rng = Rng(cfg.seed)
        self.book = SequenceBook()
        self.merchants = build_world(cfg, self.rng)
        self._counter = 0
        self._pairs: list[tuple[Event, LedgerEntry | None]] = []
        self._ids = {"txn": 0, "rfd": 0, "dsp": 0, "fee": 0, "adj": 0}
        self.drift_ms = cfg.start_ms + cfg.drift_day * DAY_MS

    # -- helpers ---------------------------------------------------------------------------
    def _next_id(self, prefix: str) -> str:
        self._ids[prefix] += 1
        return f"{prefix}_{self._ids[prefix]:08d}"

    def _add(self, topic, event_type, entity_id, t_ms, payload, op=None, entry=None) -> Event:
        version = 1
        if topic == "payments.transactions" and t_ms >= self.drift_ms:
            payload = {**payload, "channel": self.rng.choice(["web", "app", "pos"])}
            version = 2
        ev = Event(
            topic=topic,
            event_type=event_type,
            entity_id=entity_id,
            event_time_ms=t_ms,
            sequence=self.book.take(entity_id),
            payload=payload,
            event_id=make_event_id(self.cfg.seed, self._counter),
            op=op,
            schema_version=version,
        )
        self._counter += 1
        self._pairs.append((ev, entry))
        return ev

    @staticmethod
    def _merchant_payload(m: Merchant) -> dict:
        return {
            "merchant_id": m.merchant_id,
            "name": m.name,
            "country": m.country,
            "vertical": m.vertical,
            "risk_status": m.risk_status,
        }

    @staticmethod
    def _account_payload(a, plan: str, status: str) -> dict:
        return {
            "account_id": a.account_id,
            "merchant_id": a.merchant_id,
            "label": a.label,
            "currency": a.currency,
            "fee_plan": plan,
            "payout_schedule": a.payout_schedule,
            "status": status,
        }

    # -- reference data ----------------------------------------------------------------------
    def _reference_events(self) -> None:
        cfg, rng = self.cfg, self.rng
        t = cfg.start_ms
        for m in self.merchants:
            self._add(
                "ref.merchants",
                "merchant_upserted",
                m.merchant_id,
                t,
                self._merchant_payload(m),
                op="upsert",
            )
            t += 1000
        for m in self.merchants:
            for a in m.accounts:
                self._add(
                    "ref.accounts",
                    "account_upserted",
                    a.account_id,
                    t,
                    self._account_payload(a, a.plan_at(cfg.start_ms), "active"),
                    op="upsert",
                )
                t += 1000

        # Offboarded merchants: chosen among the smaller ones, deleted at 02:00 on their day.
        candidates = list(range(4, len(self.merchants)))
        offboard: list[int] = []
        for _ in range(min(cfg.offboard_count, len(candidates))):
            offboard.append(candidates.pop(rng.randint(0, len(candidates) - 1)))
        offboard_days = {}
        for idx in offboard:
            day = int(cfg.days * 0.6) + rng.randint(0, 2)
            offboard_days[idx] = min(day, cfg.days - 1)

        for idx, m in enumerate(self.merchants):
            if idx in offboard_days:
                continue
            if rng.chance(0.15) and cfg.days > 2:
                day = rng.randint(1, cfg.days - 1)
                m.risk_status = RISK[(RISK.index(m.risk_status) + 1) % 3]
                self._add(
                    "ref.merchants",
                    "merchant_upserted",
                    m.merchant_id,
                    cfg.start_ms + day * DAY_MS + 9 * HOUR,
                    self._merchant_payload(m),
                    op="upsert",
                )
            for a in m.accounts:
                if rng.chance(0.10) and cfg.days > 12:
                    day = rng.randint(10, min(20, cfg.days - 1))
                    new_plan = rng.choice([p for p in FEE_PLANS if p != a.plan_at(cfg.end_ms)])
                    at = cfg.start_ms + day * DAY_MS + 10 * HOUR
                    a.plan_history.append((at, new_plan))
                    self._add(
                        "ref.accounts",
                        "account_upserted",
                        a.account_id,
                        at,
                        self._account_payload(a, new_plan, "active"),
                        op="upsert",
                    )

        for idx, day in offboard_days.items():
            m = self.merchants[idx]
            at = cfg.start_ms + day * DAY_MS + 2 * HOUR
            m.deleted_ms = at
            for k, a in enumerate(m.accounts):
                a.status = "closed"
                self._add(
                    "ref.accounts",
                    "account_deleted",
                    a.account_id,
                    at + k * 1000,
                    {"account_id": a.account_id},
                    op="delete",
                )
            self._add(
                "ref.merchants",
                "merchant_deleted",
                m.merchant_id,
                at + (len(m.accounts) + 1) * 1000,
                {"merchant_id": m.merchant_id},
                op="delete",
            )

    # -- money movements ---------------------------------------------------------------------
    def _entry(self, merchant_id, account, t_ms, kind, amount_signed, source_id) -> LedgerEntry:
        return LedgerEntry(
            merchant_id,
            account.account_id,
            account.currency,
            posting_date(t_ms),
            kind,
            amount_signed,
            source_id,
        )

    def _transaction(self, t_ms: int, m: Merchant, a) -> None:
        cfg, rng = self.cfg, self.rng
        txn_id = self._next_id("txn")
        amount = int(math.exp(a.mu + 0.8 * rng.normal()))
        amount = max(100, min(amount, 2_000_000))
        base = {
            "transaction_id": txn_id,
            "account_id": a.account_id,
            "merchant_id": m.merchant_id,
            "amount": amount,
            "currency": a.currency,
            "payment_method": rng.choice(["card", "card", "card", "wallet", "bank_transfer"]),
        }

        def txn_event(kind: str, at: int, entry=None) -> Event:
            return self._add(
                "payments.transactions", kind, txn_id, at, {**base, "status": kind}, entry=entry
            )

        r = rng.random()
        if r < cfg.decline_rate:
            txn_event("declined", t_ms)
            return
        txn_event("authorized", t_ms)
        if r < cfg.decline_rate + cfg.void_rate:
            txn_event("voided", t_ms + rng.randint(5 * MINUTE, 60 * MINUTE))
            return

        u = rng.random()
        if u < 0.85:
            delay = rng.randint(0, 30 * MINUTE)
        elif u < 0.97:
            delay = rng.randint(HOUR, 20 * HOUR)
        else:
            delay = rng.randint(DAY_MS, 3 * DAY_MS)
        tc = t_ms + delay
        txn_event("captured", tc, self._entry(m.merchant_id, a, tc, "SALE", amount, txn_id))

        plan = a.plan_at(tc)
        fee_amount = platform_fee(amount, plan)
        fee_id = self._next_id("fee")
        self._add(
            "billing.fees",
            "fee_assessed",
            fee_id,
            tc + 1000,
            {
                "fee_id": fee_id,
                "transaction_id": txn_id,
                "account_id": a.account_id,
                "merchant_id": m.merchant_id,
                "fee_type": "platform",
                "fee_subtype": plan,
                "amount": fee_amount,
                "currency": a.currency,
            },
            entry=self._entry(m.merchant_id, a, tc + 1000, "PLATFORM_FEE", -fee_amount, fee_id),
        )
        subtype = ["interchange", "scheme", "fx"][rng.pick_weighted([80, 95, 100])]
        pt_amount = pass_through_fee(amount, subtype, fee_amount)
        fee_id = self._next_id("fee")
        self._add(
            "billing.fees",
            "fee_assessed",
            fee_id,
            tc + 1000,
            {
                "fee_id": fee_id,
                "transaction_id": txn_id,
                "account_id": a.account_id,
                "merchant_id": m.merchant_id,
                "fee_type": "pass_through",
                "fee_subtype": subtype,
                "amount": pt_amount,
                "currency": a.currency,
            },
            entry=self._entry(m.merchant_id, a, tc + 1000, "PASS_THROUGH_FEE", -pt_amount, fee_id),
        )

        if rng.chance(cfg.refund_rate):
            remaining = amount
            first_full = rng.chance(0.6)
            parts = 1 if first_full else (2 if rng.chance(0.15) else 1)
            at = tc
            for _ in range(parts):
                at += rng.randint(DAY_MS, 30 * DAY_MS)
                if first_full:
                    refund = amount
                else:
                    refund = max(1, min(remaining, int(remaining * rng.uniform(0.1, 0.9))))
                remaining -= refund
                rid = self._next_id("rfd")
                self._add(
                    "payments.refunds",
                    "refund_issued",
                    rid,
                    at,
                    {
                        "refund_id": rid,
                        "transaction_id": txn_id,
                        "account_id": a.account_id,
                        "merchant_id": m.merchant_id,
                        "amount": refund,
                        "currency": a.currency,
                        "reason": rng.choice(
                            ["customer_request", "duplicate", "fraud", "service_issue"]
                        ),
                    },
                    entry=self._entry(m.merchant_id, a, at, "REFUND", -refund, rid),
                )
                if remaining <= 0:
                    break
        elif rng.chance(cfg.dispute_rate / (1.0 - cfg.refund_rate)):
            opened = tc + rng.randint(5 * DAY_MS, 45 * DAY_MS)
            did = self._next_id("dsp")
            dbase = {
                "dispute_id": did,
                "transaction_id": txn_id,
                "account_id": a.account_id,
                "merchant_id": m.merchant_id,
                "amount": amount,
                "currency": a.currency,
                "reason_code": rng.choice(
                    ["fraud", "not_received", "not_as_described", "duplicate"]
                ),
            }
            self._add(
                "payments.disputes",
                "dispute_opened",
                did,
                opened,
                {**dbase, "status": "opened"},
                entry=self._entry(m.merchant_id, a, opened, "DISPUTE_DEBIT", -amount, did),
            )
            resolved = opened + rng.randint(10 * DAY_MS, 30 * DAY_MS)
            if rng.chance(cfg.dispute_win_rate):
                self._add(
                    "payments.disputes",
                    "dispute_won",
                    did,
                    resolved,
                    {**dbase, "status": "won"},
                    entry=self._entry(m.merchant_id, a, resolved, "DISPUTE_REVERSAL", amount, did),
                )
            else:
                self._add(
                    "payments.disputes", "dispute_lost", did, resolved, {**dbase, "status": "lost"}
                )

    def _transactions(self) -> None:
        cfg, rng = self.cfg, self.rng
        hour_cum = cumulative(HOUR_WEIGHTS)
        for day in range(cfg.days):
            day_start = cfg.start_ms + day * DAY_MS
            weekday = ((day_start // DAY_MS) + 3) % 7  # 1970-01-01 was a Thursday
            active = [
                m
                for m in self.merchants
                if m.deleted_ms is None or m.deleted_ms >= day_start + DAY_MS
            ]
            weights = cumulative([m.weight for m in active])
            n = round(cfg.tx_per_day * WEEKDAY_FACTOR[weekday] * (0.9 + 0.2 * rng.random()))
            for _ in range(n):
                m = active[rng.pick_weighted(weights)]
                a = m.accounts[rng.pick_weighted(cumulative([x.weight for x in m.accounts]))]
                hour = rng.pick_weighted(hour_cum)
                t = day_start + hour * HOUR + rng.randint(0, HOUR - 1)
                self._transaction(t, m, a)

    def _adjustments(self) -> None:
        cfg, rng = self.cfg, self.rng
        months = max(1, math.ceil(cfg.days / 30))
        for m in self.merchants:
            for a in m.accounts:
                for month in range(months):
                    day = month * 30 + rng.randint(0, 29)
                    t = cfg.start_ms + day * DAY_MS + rng.randint(9 * HOUR, 17 * HOUR)
                    sign = 1 if rng.chance(0.5) else -1
                    amount = sign * rng.randint(500, 50_000)
                    reason = rng.choice(ADJUSTMENT_REASONS)
                    approver = rng.choice(APPROVERS)
                    if m.deleted_ms is not None and t >= m.deleted_ms:
                        continue
                    aid = self._next_id("adj")
                    self._add(
                        "ledger.adjustments",
                        "adjustment_posted",
                        aid,
                        t,
                        {
                            "adjustment_id": aid,
                            "account_id": a.account_id,
                            "merchant_id": m.merchant_id,
                            "amount": amount,
                            "currency": a.currency,
                            "reason_code": reason,
                            "approver": approver,
                        },
                        entry=self._entry(m.merchant_id, a, t, "ADJUSTMENT", amount, aid),
                    )

    def run(self) -> SimulationResult:
        self._reference_events()
        self._transactions()
        self._adjustments()
        end = self.cfg.end_ms
        kept = [(ev, entry) for ev, entry in self._pairs if ev.event_time_ms < end]
        return SimulationResult(
            merchants=self.merchants,
            events=[ev for ev, _ in kept],
            ledger=[entry for _, entry in kept if entry is not None],
        )


def simulate(cfg: GeneratorConfig) -> SimulationResult:
    return Simulation(cfg).run()

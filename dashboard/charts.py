"""Chart builders and the small data reshaping they need. No Streamlit in here, so it is testable.

Colours follow the data-viz rules used in this project: categorical hues in a fixed order, assigned
to the thing they stand for (an account, a ledger component) and never to its rank, so filtering
does not repaint what is left. Money is never added across currencies: one chart per currency.
"""

from __future__ import annotations

import altair as alt
import pandas as pd

from statements.pdf import HEADINGS
from statements.sources import AMOUNTS

# Categorical slots in order (light, dark): blue, orange, aqua, yellow, magenta, green, violet, red.
SLOTS = [
    ("#257de0", "#338ced"),
    ("#f47230", "#e76624"),
    ("#00b894", "#00a888"),
    ("#f2a600", "#d28e05"),
    ("#ee7fac", "#e24d8b"),
    ("#008a17", "#0aac23"),
    ("#6a4ce2", "#9887ef"),
    ("#ec4647", "#eb6263"),
]
COMPONENT_ORDER = [
    "sales_amount",
    "refund_amount",
    "dispute_amount",
    "platform_fee_amount",
    "pass_through_fee_amount",
    "adjustment_amount",
]
MUTED = ("#666666", "#9c9c9c")


def colour(slot: int, dark: bool) -> str:
    return SLOTS[slot % len(SLOTS)][1 if dark else 0]


def daily_frame(rows: list[dict]) -> pd.DataFrame:
    """Daily gold rows as a frame, with money in major units for display (cents / 100)."""
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["posting_date"] = pd.to_datetime(frame["posting_date"])
    for name in AMOUNTS:
        frame[name] = frame[name].astype("int64")
    return frame


def to_major(minor: int) -> float:
    """Cents to a number for a chart axis. Only for drawing: the statements never use floats."""
    return minor / 100


def component_totals(frame: pd.DataFrame) -> pd.DataFrame:
    """One row per ledger component with its month total, as a signed effect on the payout."""
    sign = {"refund_amount": -1, "dispute_amount": -1, "platform_fee_amount": -1,
            "pass_through_fee_amount": -1}  # fmt: skip
    rows = []
    for name in COMPONENT_ORDER:
        total = int(frame[name].sum())
        rows.append(
            {
                "component": HEADINGS[name],
                "slot": COMPONENT_ORDER.index(name),
                "amount": to_major(total),
                "effect": to_major(sign.get(name, 1) * total),
            }
        )
    return pd.DataFrame(rows)


def account_slots(account_ids: list[str]) -> dict[str, int]:
    """A stable colour slot for each account of a merchant, whatever is filtered."""
    return {a: i for i, a in enumerate(sorted(account_ids))}


def daily_chart(frame: pd.DataFrame, slots: dict[str, int], dark: bool, currency: str):
    """Daily amount disbursed, stacked by account (one account = one solid colour)."""
    data = frame.assign(disbursed=frame["disbursed_total"] / 100)
    accounts = sorted(data["account_id"].unique())
    return (
        alt.Chart(data, height=260)
        .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3, size=14)
        .encode(
            x=alt.X("posting_date:T", title=None, axis=alt.Axis(format="%d %b", labelAngle=0)),
            y=alt.Y("disbursed:Q", title=f"Disbursed ({currency})", axis=alt.Axis(format=",.0f")),
            color=alt.Color(
                "account_id:N",
                scale=alt.Scale(domain=accounts, range=[colour(slots[a], dark) for a in accounts]),
                legend=alt.Legend(title="Account", orient="top") if len(accounts) > 1 else None,
            ),
            tooltip=[
                alt.Tooltip("posting_date:T", title="Date", format="%Y-%m-%d"),
                alt.Tooltip("account_id:N", title="Account"),
                alt.Tooltip("disbursed:Q", title="Disbursed", format=",.2f"),
            ],
        )
        .configure_view(stroke=None)
    )


def components_chart(totals: pd.DataFrame, dark: bool, currency: str):
    """What was taken off the payout, and adjusted: a labelled bar per component.

    Sales are left out (they are the headline number and would dwarf everything else here).
    Deductions point left, a positive adjustment points right."""
    totals = totals[totals["component"] != "Sales"]
    order = totals["component"].tolist()
    scale = alt.Scale(domain=order, range=[colour(int(s), dark) for s in totals["slot"]])
    base = alt.Chart(totals, height=34 * len(totals)).encode(
        y=alt.Y("component:N", sort=order, title=None),
    )
    bars = base.mark_bar(cornerRadiusEnd=3, size=16).encode(
        x=alt.X("effect:Q", title=f"Effect on payout ({currency})", axis=alt.Axis(format=",.0f")),
        color=alt.Color("component:N", scale=scale, legend=None),
        tooltip=[
            alt.Tooltip("component:N", title="Component"),
            alt.Tooltip("amount:Q", title="Amount", format=",.2f"),
        ],
    )
    labels = base.mark_text(align="left", dx=4, fontSize=11).encode(
        x=alt.X("effect:Q"),
        text=alt.Text("effect:Q", format=",.0f"),
    )
    return (bars + labels).configure_view(stroke=None)


def reject_chart(rows: list[dict], dark: bool):
    """Quarantined messages by reason (one hue: this is magnitude, not identity)."""
    frame = pd.DataFrame(rows)
    if frame.empty:
        return None
    frame["messages"] = frame["messages"].astype("int64")
    by_reason = frame.groupby("reject_reason", as_index=False)["messages"].sum()
    by_reason = by_reason.sort_values("messages", ascending=False).head(15)
    order = by_reason["reject_reason"].tolist()
    return (
        alt.Chart(by_reason, height=24 * len(by_reason) + 20)
        .mark_bar(cornerRadiusEnd=3, size=14, color=colour(0, dark))
        .encode(
            y=alt.Y("reject_reason:N", sort=order, title=None),
            x=alt.X("messages:Q", title="Messages quarantined"),
            tooltip=[
                alt.Tooltip("reject_reason:N", title="Reason"),
                alt.Tooltip("messages:Q", title="Messages"),
            ],  # fmt: skip
        )
        .configure_view(stroke=None)
    )

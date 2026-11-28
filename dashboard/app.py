"""Merchant reporting dashboard.

    streamlit run dashboard/app.py                      # reads the local DuckDB file
    DASHBOARD_SOURCE=snowflake streamlit run dashboard/app.py

Two pages: merchant disbursements (with the statement PDF) and pipeline health. The data source
can be switched in the sidebar; Snowflake queries use warehouse credits, so they are cached.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # so `streamlit run` finds the repo

from dashboard import charts, data  # noqa: E402
from statements.model import StatementError, money  # noqa: E402
from statements.pdf import HEADINGS  # noqa: E402
from statements.sources import AMOUNTS, DEFAULT_LOCAL  # noqa: E402

PAGES = ["Merchant disbursements", "Pipeline health"]
SOURCES = {"Local file (DuckDB)": "duckdb", "Snowflake": "snowflake"}


def is_dark() -> bool:
    try:
        return st.context.theme.type == "dark"
    except Exception:  # an older Streamlit, or no theme information
        return False


def sidebar() -> tuple[str, str, str, str, str]:
    """(page, kind, path, database, label) for the selections made in the sidebar."""
    st.sidebar.title("Merchant reporting")
    page = st.sidebar.radio("Page", PAGES, key="page")
    default = os.environ.get("DASHBOARD_SOURCE", "duckdb").lower()
    labels = list(SOURCES)
    choice = st.sidebar.radio(
        "Data source", labels, index=0 if default == "duckdb" else 1, key="source"
    )
    kind = SOURCES[choice]
    path = os.environ.get("DBT_DUCKDB_PATH", str(DEFAULT_LOCAL))
    database = os.environ.get("SNOWFLAKE_DATABASE", "MERCHANT_DISBURSEMENT")
    if kind == "snowflake":
        st.sidebar.caption(
            f"Reads the gold tables in {database}. Queries run on the transform warehouse and "
            "use credits, so results are cached for five minutes."
        )
        label = f"Snowflake ({database})"
    else:
        st.sidebar.caption(f"Reads {path}. Build it with `make dbt-local`.")
        label = "local DuckDB file"
    if st.sidebar.button("Refresh data"):
        data.clear()
    return page, kind, path, database, label


def connect(kind: str, path: str, database: str) -> None:
    """Fail with a readable message, not a traceback, if the source cannot be opened."""
    try:
        data.get_source(kind, path, database if kind == "snowflake" else "")
    except FileNotFoundError as error:
        st.error(str(error))
        st.stop()
    except Exception as error:  # a bad key, a suspended account, no network
        st.error(f"Could not open the {kind} source: {error}")
        st.stop()


# -- page 1 -------------------------------------------------------------------------------------
def merchant_label(m: dict) -> str:
    return f"{m['merchant_id']} · {m['name']}" + (" (closed)" if m["closed"] else "")


def totals_table(frame: pd.DataFrame, labels: dict[str, str]) -> pd.DataFrame:
    rows = []
    for account_id, group in frame.groupby("account_id"):
        currency = group["currency"].iloc[0]
        row = {"Account": account_id, "Label": labels.get(account_id, ""), "Currency": currency,
               "Active days": len(group)}  # fmt: skip
        for name in AMOUNTS:
            row[HEADINGS[name]] = money(int(group[name].sum()), currency)
        rows.append(row)
    return pd.DataFrame(rows)


def page_disbursements(kind: str, path: str, database: str) -> None:
    st.title("Merchant disbursements")
    args = (kind, path, database)
    merchants = data.fetch(*args, "merchants")
    paid = {r["merchant_id"] for r in data.fetch(*args, "monthly")}
    merchants = [m for m in merchants if m["merchant_id"] in paid]
    if not merchants:
        st.info("The gold tables hold no disbursements yet. Run `make gold` (or `make dbt-local`).")
        return
    by_label = {merchant_label(m): m for m in merchants}
    c1, c2, c3 = st.columns([2, 1, 2])
    merchant = by_label[c1.selectbox("Merchant", list(by_label), key="merchant")]
    months = data.fetch(*args, "merchant_months", merchant["merchant_id"])
    month = c2.selectbox("Month", months, index=len(months) - 1, format_func=lambda d: f"{d:%Y-%m}",
                         key="month")  # fmt: skip
    accounts = data.fetch(*args, "accounts", merchant["merchant_id"])
    labels = {a["account_id"]: a["label"] for a in accounts}
    names = {"All accounts": None}
    for a in accounts:
        names[f"{a['account_id']} · {a['label']} · {a['currency']}"] = a["account_id"]
    account = names[c3.selectbox("Account", list(names), key="account")]

    frame = charts.daily_frame(data.fetch(*args, "daily", merchant["merchant_id"], month))
    if frame.empty:
        st.info("No activity for this merchant in that month.")
        return
    slots = charts.account_slots(frame["account_id"].unique().tolist())
    if account:
        frame = frame[frame["account_id"] == account]
    dark = is_dark()

    pdf = data.statement_pdf(*args, merchant["merchant_id"], month)
    st.download_button(
        "Download the statement (PDF)", data=pdf, mime="application/pdf",
        file_name=f"{merchant['merchant_id']}_{month:%Y-%m}.pdf",
        help="The statement covers every account of the merchant for the whole month.",
    )  # fmt: skip

    currencies = sorted(frame["currency"].unique())
    for column, currency in zip(st.columns(len(currencies)), currencies, strict=True):
        part = frame[frame["currency"] == currency]
        deductions = int(part[["refund_amount", "dispute_amount", "platform_fee_amount",
                               "pass_through_fee_amount"]].to_numpy().sum())  # fmt: skip
        column.metric(f"Disbursed, {currency}", money(int(part["disbursed_total"].sum()), currency))
        column.caption(
            f"Sales {money(int(part['sales_amount'].sum()), currency)} · "
            f"deductions {money(deductions, currency)}"
        )
    st.subheader("By account")
    st.dataframe(totals_table(frame, labels), hide_index=True, width="stretch")

    for currency in currencies:
        part = frame[frame["currency"] == currency]
        st.subheader(f"{currency}")
        left, right = st.columns([3, 2])
        left.altair_chart(charts.daily_chart(part, slots, dark, currency), width="stretch")
        right.altair_chart(
            charts.components_chart(charts.component_totals(part), dark, currency),
            width="stretch",
        )
    with st.expander("Daily rows"):
        shown = frame.assign(posting_date=frame["posting_date"].dt.strftime("%Y-%m-%d"))
        for name in AMOUNTS:
            shown[name] = [
                money(int(v), c) for v, c in zip(shown[name], shown["currency"], strict=True)
            ]
        shown = shown.rename(
            columns={**{k: v for k, v in HEADINGS.items()}, "posting_date": "Date"}
        )
        st.dataframe(shown.drop(columns=["merchant_id"]), hide_index=True, width="stretch")
    st.caption("Amounts come from the gold tables. A month is a calendar month in UTC.")


# -- page 2 -------------------------------------------------------------------------------------
def page_health(kind: str, path: str, database: str, label: str) -> None:
    st.title("Pipeline health")
    args = (kind, path, database)
    signals = data.fetch(*args, "health_signals")
    incidents = data.fetch(*args, "incidents", 50)
    quarantine = data.fetch(*args, "quarantine")
    recon = data.fetch(*args, "reconciliation")
    firing = [s for s in signals if s["firing"]]
    open_now = [i for i in incidents if i["status"] == "OPEN"]
    quarantined = sum(int(q["messages"]) for q in quarantine)
    unaccounted = sum(
        abs(int(r["unaccounted_rows"])) + abs(int(r["missing_entities"])) for r in recon
    )

    if firing or open_now:
        st.error(
            f"{len(firing)} signal(s) firing, {len(open_now)} open incident(s). "
            "See the tables below."
        )
    else:
        st.success("All signals are normal and no incidents are open.")
    k = st.columns(4)
    k[0].metric("Open incidents", len(open_now))
    k[1].metric("Signals firing", len(firing))
    k[2].metric("Quarantined messages", f"{quarantined:,}")
    k[3].metric("Unaccounted rows", f"{unaccounted:,}",
                help="Bronze rows silver cannot account for. Must be 0.")  # fmt: skip
    st.caption(f"Source: {label}.")

    st.subheader("Signals")
    st.dataframe(
        pd.DataFrame(
            [
                {"Signal": s["signal"], "Severity": s["severity"],
                 "State": "FIRING" if s["firing"] else ("off" if not s["enabled"] else "ok"),
                 "Observed": "n/a" if s["observed"] is None else str(s["observed"]),
                 "Alerts above": str(s["threshold"])}
                for s in signals
            ]
        ),
        hide_index=True, width="stretch",
    )  # fmt: skip
    if kind == "duckdb":
        st.caption(
            "Failed tasks and refreshes are measured only on Snowflake, so they show n/a here."
        )
    st.subheader("Incidents")
    if incidents:
        st.dataframe(pd.DataFrame(incidents), hide_index=True, width="stretch")
    else:
        st.caption("No incidents have been recorded.")
    st.subheader("Quarantine")
    chart = charts.reject_chart(quarantine, is_dark())
    if chart is None:
        st.caption("Nothing is in quarantine.")
    else:
        st.altair_chart(chart, width="stretch")
        st.dataframe(pd.DataFrame(quarantine), hide_index=True, width="stretch")
    st.subheader("Reconciliation")
    st.caption(
        "Every bronze row is rejected, a duplicate, or a unique event. Unaccounted must be 0."
    )
    st.dataframe(pd.DataFrame(recon), hide_index=True, width="stretch")


def main() -> None:
    st.set_page_config(page_title="Merchant reporting", layout="wide")
    page, kind, path, database, label = sidebar()
    connect(kind, path, database if kind == "snowflake" else "")
    db = database if kind == "snowflake" else ""
    try:
        if page == PAGES[0]:
            page_disbursements(kind, path, db)
        else:
            page_health(kind, path, db, label)
    except StatementError as error:
        st.error(str(error))


main()
